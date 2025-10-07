from google.adk import Agent
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import asyncio
import json

from src.utils.databricks_client import DatabricksClient
from src.utils.s3_client import S3LogClient
from src.utils.log_analyzer import LogAnalyzer

async def search_databricks_jobs(job_name_pattern: str) -> str:
    """
    Search for Databricks jobs by name pattern.

    Args:
        job_name_pattern: Pattern to search for in job names (supports regex)

    Returns:
        JSON string with matching jobs information
    """
    try:
        client = DatabricksClient()
        jobs = await client.search_jobs_by_name(job_name_pattern)

        if not jobs:
            return f"No jobs found matching pattern: {job_name_pattern}"

        result = {
            "found_jobs": len(jobs),
            "jobs": jobs
        }

        return json.dumps(result, indent=2, default=str)

    except Exception as e:
        return f"Error searching jobs: {str(e)}"

async def get_job_cluster_ids(job_name_pattern: str,
                            start_time: Optional[str] = None,
                            end_time: Optional[str] = None) -> str:
    """
    Get cluster IDs for jobs matching the pattern within a time range.

    Args:
        job_name_pattern: Pattern to search for in job names
        start_time: Start time in format 'YYYY-MM-DD HH:MM:SS' (optional)
        end_time: End time in format 'YYYY-MM-DD HH:MM:SS' (optional)

    Returns:
        JSON string with cluster IDs and job run information
    """
    try:
        client = DatabricksClient()

        time_range = None
        if start_time and end_time:
            start_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
            end_dt = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
            time_range = (start_dt, end_dt)
        elif start_time:
            start_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
            end_dt = datetime.now()
            time_range = (start_dt, end_dt)

        cluster_ids = await client.get_cluster_ids_for_job(job_name_pattern, time_range)

        if not cluster_ids:
            return f"No cluster IDs found for job pattern: {job_name_pattern}"

        result = {
            "job_pattern": job_name_pattern,
            "time_range": f"{start_time or '24 hours ago'} to {end_time or 'now'}",
            "cluster_ids": cluster_ids,
            "total_clusters": len(cluster_ids)
        }

        return json.dumps(result, indent=2)

    except Exception as e:
        return f"Error getting cluster IDs: {str(e)}"

async def search_failed_job_runs(job_name_pattern: Optional[str] = None,
                               hours_back: int = 24) -> str:
    """
    Search for failed job runs in the specified time period.

    Args:
        job_name_pattern: Pattern to search for in job names (optional)
        hours_back: How many hours back to search (default: 24)

    Returns:
        JSON string with failed job runs information
    """
    try:
        client = DatabricksClient()
        failed_runs = await client.search_failed_runs(job_name_pattern, hours_back)

        if not failed_runs:
            return "No failed job runs found in the specified time period."

        result = {
            "search_period_hours": hours_back,
            "failed_runs_count": len(failed_runs),
            "failed_runs": failed_runs
        }

        return json.dumps(result, indent=2, default=str)

    except Exception as e:
        return f"Error searching failed runs: {str(e)}"

async def list_cluster_logs(cluster_id: str) -> str:
    """
    List available log files for a specific cluster ID.

    Args:
        cluster_id: The cluster ID to list logs for

    Returns:
        JSON string with available log files information
    """
    try:
        s3_client = S3LogClient()
        log_files = await s3_client.list_cluster_logs(cluster_id)

        if not log_files:
            return f"No log files found for cluster: {cluster_id}"

        result = {
            "cluster_id": cluster_id,
            "log_files_count": len(log_files),
            "log_files": log_files
        }

        return json.dumps(result, indent=2, default=str)

    except Exception as e:
        return f"Error listing cluster logs: {str(e)}"

async def analyze_cluster_logs(cluster_id: str,
                             search_pattern: Optional[str] = None,
                             max_files: int = 10) -> str:
    """
    Analyze logs for a specific cluster ID to identify errors and issues.

    Args:
        cluster_id: The cluster ID to analyze logs for
        search_pattern: Optional pattern to search for in logs
        max_files: Maximum number of log files to analyze (default: 10)

    Returns:
        Detailed analysis of the cluster logs
    """
    try:
        s3_client = S3LogClient()
        analyzer = LogAnalyzer()

        # Get list of log files
        log_files = await s3_client.list_cluster_logs(cluster_id)

        if not log_files:
            return f"No log files found for cluster: {cluster_id}"

        # Prioritize error-prone log types
        priority_types = ['stderr', 'log4j', 'driver', 'executor']
        sorted_files = sorted(log_files,
                            key=lambda x: (priority_types.index(x['log_type'])
                                         if x['log_type'] in priority_types else 99,
                                         -x['size']))

        # Download and analyze logs
        log_content = {}
        files_processed = 0

        for log_file in sorted_files[:max_files]:
            try:
                content = await s3_client.download_and_read_log(log_file['key'], max_size_mb=20)
                log_content[log_file['file_name']] = content
                files_processed += 1
            except Exception as e:
                print(f"Warning: Could not process {log_file['file_name']}: {str(e)}")
                continue

        if not log_content:
            return f"Could not read any log files for cluster: {cluster_id}"

        # Analyze the logs
        analysis = await analyzer.analyze_logs(log_content, search_pattern)

        result = {
            "cluster_id": cluster_id,
            "files_analyzed": files_processed,
            "search_pattern": search_pattern,
            "analysis": analysis
        }

        return json.dumps(result, indent=2, default=str)

    except Exception as e:
        return f"Error analyzing cluster logs: {str(e)}"

async def search_log_pattern(cluster_id: str,
                           search_pattern: str,
                           log_types: Optional[List[str]] = None) -> str:
    """
    Search for a specific pattern in cluster logs.

    Args:
        cluster_id: The cluster ID to search logs for
        search_pattern: Pattern to search for (supports regex)
        log_types: List of log types to search in (e.g., ['stderr', 'log4j'])

    Returns:
        Search results with matching log snippets
    """
    try:
        s3_client = S3LogClient()

        # Get list of log files
        log_files = await s3_client.list_cluster_logs(cluster_id)

        if not log_files:
            return f"No log files found for cluster: {cluster_id}"

        # Filter by log types if specified
        if log_types:
            log_files = [f for f in log_files if f['log_type'] in log_types]

        search_results = []

        for log_file in log_files:
            try:
                snippet = await s3_client.get_log_snippet(
                    log_file['key'],
                    search_pattern,
                    max_lines=50,
                    around_lines=3
                )

                if snippet and "Error reading log snippet" not in snippet:
                    search_results.append({
                        "file_name": log_file['file_name'],
                        "log_type": log_file['log_type'],
                        "matches": snippet
                    })

            except Exception as e:
                continue

        if not search_results:
            return f"Pattern '{search_pattern}' not found in any logs for cluster: {cluster_id}"

        result = {
            "cluster_id": cluster_id,
            "search_pattern": search_pattern,
            "matches_found": len(search_results),
            "results": search_results
        }

        return json.dumps(result, indent=2)

    except Exception as e:
        return f"Error searching log pattern: {str(e)}"

async def get_job_execution_timeline(job_name_pattern: str,
                                   hours_back: int = 24) -> str:
    """
    Get execution timeline for jobs matching the pattern.

    Args:
        job_name_pattern: Pattern to search for in job names
        hours_back: How many hours back to search (default: 24)

    Returns:
        JSON string with job execution timeline
    """
    try:
        client = DatabricksClient()

        # Find matching jobs
        jobs = await client.search_jobs_by_name(job_name_pattern)

        if not jobs:
            return f"No jobs found matching pattern: {job_name_pattern}"

        # Get runs for each job
        start_time = datetime.now() - timedelta(hours=hours_back)
        end_time = datetime.now()

        timeline = []

        for job in jobs:
            runs = await client.get_job_runs(job['job_id'], start_time, end_time, limit=20)

            for run in runs:
                timeline.append({
                    "job_name": job['job_name'],
                    "run_id": run['run_id'],
                    "cluster_id": run['cluster_id'],
                    "state": run['state'],
                    "start_time": run['start_time'],
                    "end_time": run['end_time'],
                    "duration_seconds": run['execution_duration']
                })

        # Sort by start time
        timeline.sort(key=lambda x: x['start_time'] or datetime.min, reverse=True)

        result = {
            "job_pattern": job_name_pattern,
            "time_period_hours": hours_back,
            "total_runs": len(timeline),
            "timeline": timeline
        }

        return json.dumps(result, indent=2, default=str)

    except Exception as e:
        return f"Error getting job timeline: {str(e)}"