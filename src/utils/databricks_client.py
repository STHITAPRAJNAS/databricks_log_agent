import asyncio
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Run, Job
from config.config import config
import re

class DatabricksClient:
    def __init__(self):
        self.client = WorkspaceClient(
            host=config.databricks_host,
            token=config.databricks_token
        )

    async def search_jobs_by_name(self, job_name_pattern: str) -> List[Dict]:
        try:
            jobs = list(self.client.jobs.list())
            matching_jobs = []

            pattern = re.compile(job_name_pattern, re.IGNORECASE)

            for job in jobs:
                if pattern.search(job.settings.name):
                    matching_jobs.append({
                        'job_id': job.job_id,
                        'job_name': job.settings.name,
                        'created_time': job.created_time,
                        'creator_user_name': job.creator_user_name
                    })

            return matching_jobs

        except Exception as e:
            raise Exception(f"Failed to search jobs: {str(e)}")

    async def get_job_runs(self, job_id: int, start_time: Optional[datetime] = None,
                          end_time: Optional[datetime] = None, limit: int = 50) -> List[Dict]:
        try:
            if start_time is None:
                start_time = datetime.now() - timedelta(days=7)

            if end_time is None:
                end_time = datetime.now()

            start_time_ms = int(start_time.timestamp() * 1000)
            end_time_ms = int(end_time.timestamp() * 1000)

            runs = list(self.client.jobs.list_runs(
                job_id=job_id,
                start_time_from=start_time_ms,
                start_time_to=end_time_ms,
                limit=limit
            ))

            job_runs = []
            for run in runs:
                cluster_id = None
                if run.cluster_instance and run.cluster_instance.cluster_id:
                    cluster_id = run.cluster_instance.cluster_id

                job_runs.append({
                    'run_id': run.run_id,
                    'job_id': run.job_id,
                    'cluster_id': cluster_id,
                    'run_name': run.run_name,
                    'state': {
                        'life_cycle_state': run.state.life_cycle_state.value if run.state else None,
                        'result_state': run.state.result_state.value if run.state and run.state.result_state else None,
                        'state_message': run.state.state_message if run.state else None
                    },
                    'start_time': datetime.fromtimestamp(run.start_time / 1000) if run.start_time else None,
                    'end_time': datetime.fromtimestamp(run.end_time / 1000) if run.end_time else None,
                    'execution_duration': run.execution_duration,
                    'creator_user_name': run.creator_user_name
                })

            return job_runs

        except Exception as e:
            raise Exception(f"Failed to get job runs for job {job_id}: {str(e)}")

    async def get_run_details(self, run_id: int) -> Dict:
        try:
            run = self.client.jobs.get_run(run_id)

            cluster_id = None
            if run.cluster_instance and run.cluster_instance.cluster_id:
                cluster_id = run.cluster_instance.cluster_id

            tasks = []
            if run.tasks:
                for task in run.tasks:
                    task_cluster_id = None
                    if task.cluster_instance and task.cluster_instance.cluster_id:
                        task_cluster_id = task.cluster_instance.cluster_id

                    tasks.append({
                        'task_key': task.task_key,
                        'cluster_id': task_cluster_id,
                        'state': {
                            'life_cycle_state': task.state.life_cycle_state.value if task.state else None,
                            'result_state': task.state.result_state.value if task.state and task.state.result_state else None,
                        },
                        'start_time': datetime.fromtimestamp(task.start_time / 1000) if task.start_time else None,
                        'end_time': datetime.fromtimestamp(task.end_time / 1000) if task.end_time else None,
                    })

            return {
                'run_id': run.run_id,
                'job_id': run.job_id,
                'cluster_id': cluster_id,
                'run_name': run.run_name,
                'state': {
                    'life_cycle_state': run.state.life_cycle_state.value if run.state else None,
                    'result_state': run.state.result_state.value if run.state and run.state.result_state else None,
                    'state_message': run.state.state_message if run.state else None
                },
                'start_time': datetime.fromtimestamp(run.start_time / 1000) if run.start_time else None,
                'end_time': datetime.fromtimestamp(run.end_time / 1000) if run.end_time else None,
                'execution_duration': run.execution_duration,
                'creator_user_name': run.creator_user_name,
                'tasks': tasks,
                'cluster_spec': run.cluster_spec.dict() if run.cluster_spec else None
            }

        except Exception as e:
            raise Exception(f"Failed to get run details for run {run_id}: {str(e)}")

    async def search_failed_runs(self, job_name_pattern: str = None,
                               hours_back: int = 24) -> List[Dict]:
        try:
            start_time = datetime.now() - timedelta(hours=hours_back)
            end_time = datetime.now()

            jobs = []
            if job_name_pattern:
                jobs = await self.search_jobs_by_name(job_name_pattern)
            else:
                all_jobs = list(self.client.jobs.list())
                jobs = [{'job_id': job.job_id, 'job_name': job.settings.name} for job in all_jobs]

            failed_runs = []
            for job in jobs:
                runs = await self.get_job_runs(job['job_id'], start_time, end_time, limit=20)

                for run in runs:
                    if (run['state']['result_state'] in ['FAILED', 'CANCELED', 'TIMEOUT'] or
                        run['state']['life_cycle_state'] in ['INTERNAL_ERROR']):

                        failed_runs.append({
                            **run,
                            'job_name': job['job_name']
                        })

            return sorted(failed_runs, key=lambda x: x['start_time'] or datetime.min, reverse=True)

        except Exception as e:
            raise Exception(f"Failed to search failed runs: {str(e)}")

    async def get_cluster_logs_info(self, cluster_id: str) -> Dict:
        try:
            cluster = self.client.clusters.get(cluster_id)

            log_info = {
                'cluster_id': cluster_id,
                'cluster_name': cluster.cluster_name,
                'spark_version': cluster.spark_version,
                'state': cluster.state.value if cluster.state else None,
                'log_conf': None,
                's3_log_path': None
            }

            if cluster.cluster_log_conf and cluster.cluster_log_conf.s3:
                s3_conf = cluster.cluster_log_conf.s3
                log_info['log_conf'] = {
                    'destination': s3_conf.destination,
                    'region': s3_conf.region,
                    'enable_encryption': s3_conf.enable_encryption
                }
                log_info['s3_log_path'] = f"{s3_conf.destination}/{cluster_id}"

            return log_info

        except Exception as e:
            raise Exception(f"Failed to get cluster log info for {cluster_id}: {str(e)}")

    def format_time_range(self, start_time: datetime, end_time: datetime) -> str:
        return f"{start_time.strftime('%Y-%m-%d %H:%M:%S')} to {end_time.strftime('%Y-%m-%d %H:%M:%S')}"

    async def get_cluster_ids_for_job(self, job_name_pattern: str,
                                    time_range: Tuple[datetime, datetime] = None) -> List[str]:
        if time_range is None:
            start_time = datetime.now() - timedelta(hours=24)
            end_time = datetime.now()
        else:
            start_time, end_time = time_range

        jobs = await self.search_jobs_by_name(job_name_pattern)
        cluster_ids = set()

        for job in jobs:
            runs = await self.get_job_runs(job['job_id'], start_time, end_time)
            for run in runs:
                if run['cluster_id']:
                    cluster_ids.add(run['cluster_id'])

                run_details = await self.get_run_details(run['run_id'])
                for task in run_details.get('tasks', []):
                    if task.get('cluster_id'):
                        cluster_ids.add(task['cluster_id'])

        return list(cluster_ids)