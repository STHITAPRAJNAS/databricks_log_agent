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
            "analysis_method": "iterative" if use_iterative else "standard",
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

async def analyze_cluster_logs_smart(cluster_id: str,
                                   focus_areas: Optional[List[str]] = None) -> str:
    """
    Smart analysis that iteratively searches for errors with minimal iterations.

    Args:
        cluster_id: The cluster ID to analyze logs for
        focus_areas: List of error categories to focus on (e.g., ['memory', 'spark', 'io'])

    Returns:
        Optimized analysis results focusing on critical errors
    """
    try:
        s3_client = S3LogClient()

        # Use iterative pattern search for fast error discovery
        pattern_results = await s3_client.search_iterative_patterns(cluster_id, max_iterations=3)

        if not pattern_results['errors_by_category']:
            return f"No errors found in logs for cluster: {cluster_id}"

        # Focus on specific areas if requested
        if focus_areas:
            filtered_errors = {}
            area_mapping = {
                'memory': ['memory_issues', 'critical_errors'],
                'spark': ['spark_errors'],
                'io': ['io_errors'],
                'network': ['network_errors'],
                'auth': ['authentication_errors'],
                'config': ['configuration_errors'],
                'app': ['application_errors']
            }

            for area in focus_areas:
                categories = area_mapping.get(area, [area])
                for category in categories:
                    if category in pattern_results['errors_by_category']:
                        filtered_errors[category] = pattern_results['errors_by_category'][category]

            pattern_results['errors_by_category'] = filtered_errors

        # Format results for better readability
        summary = {
            "cluster_id": cluster_id,
            "analysis_method": "smart_iterative",
            "total_iterations": len(pattern_results['iteration_summary']),
            "focus_areas": focus_areas or "all",
            "critical_findings": [],
            "error_breakdown": pattern_results['errors_by_category'],
            "iteration_details": pattern_results['iteration_summary'],
            "recommendations": []
        }

        # Extract critical findings
        for category, errors in pattern_results['errors_by_category'].items():
            critical_errors = [e for e in errors if e.get('severity') == 'critical']
            high_errors = [e for e in errors if e.get('severity') == 'high']

            if critical_errors:
                summary['critical_findings'].append({
                    "category": category,
                    "severity": "critical",
                    "count": len(critical_errors),
                    "examples": critical_errors[:3]
                })
            elif high_errors:
                summary['critical_findings'].append({
                    "category": category,
                    "severity": "high",
                    "count": len(high_errors),
                    "examples": high_errors[:2]
                })

        # Add recommendations based on error patterns
        if 'memory_issues' in pattern_results['errors_by_category']:
            summary['recommendations'].append(
                "🔧 Memory optimization needed: Consider increasing executor memory or optimizing data partitioning"
            )

        if 'spark_errors' in pattern_results['errors_by_category']:
            summary['recommendations'].append(
                "⚡ Spark configuration review: Check job parameters and cluster settings"
            )

        if 'io_errors' in pattern_results['errors_by_category']:
            summary['recommendations'].append(
                "📁 I/O troubleshooting: Verify file paths, permissions, and storage connectivity"
            )

        return json.dumps(summary, indent=2, default=str)

    except Exception as e:
        return f"Error in smart log analysis: {str(e)}"

async def quick_error_scan(cluster_id: str) -> str:
    """
    Quick scan for critical errors only - optimized for minimal analysis time.

    Args:
        cluster_id: The cluster ID to scan

    Returns:
        Quick summary of critical errors found
    """
    try:
        s3_client = S3LogClient()

        # Single iteration focusing only on critical patterns
        critical_patterns = {
            'critical_errors': [
                r'(?i)\bfatal\s+error\b',
                r'(?i)\boutofmemoryerror\b',
                r'(?i)\bjob\s+\d+\s+failed\b',
                r'(?i)\banalysisexception\b'
            ]
        }

        errors = await s3_client.search_error_patterns(
            cluster_id,
            custom_patterns=critical_patterns,
            max_errors_per_category=10
        )

        if not errors or not errors.get('critical_errors'):
            return f"✅ No critical errors found in cluster: {cluster_id}"

        critical_count = len(errors['critical_errors'])

        # Extract key error details
        error_types = set()
        for error in errors['critical_errors'][:5]:
            if 'outofmemory' in error['line'].lower():
                error_types.add('Memory Issues')
            elif 'job' in error['line'].lower() and 'failed' in error['line'].lower():
                error_types.add('Job Failures')
            elif 'analysisexception' in error['line'].lower():
                error_types.add('SQL Errors')
            else:
                error_types.add('Critical Errors')

        result = {
            "cluster_id": cluster_id,
            "scan_type": "quick_critical_scan",
            "critical_errors_found": critical_count,
            "error_types": list(error_types),
            "sample_errors": errors['critical_errors'][:3],
            "recommendation": "Run full analysis for detailed diagnosis" if critical_count > 0 else "No immediate action needed"
        }

        return json.dumps(result, indent=2, default=str)

    except Exception as e:
        return f"Error in quick error scan: {str(e)}"