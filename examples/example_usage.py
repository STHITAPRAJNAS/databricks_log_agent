#!/usr/bin/env python3
"""
Example usage of the Databricks Log Analysis Agent

This script demonstrates how to use the agent programmatically
and provides examples of common analysis scenarios.
"""

import os
import sys
import asyncio
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from src.agents.databricks_log_agent import create_databricks_log_agent
from src.tools.databricks_tools import (
    search_databricks_jobs,
    get_job_cluster_ids,
    search_failed_job_runs,
    analyze_cluster_logs
)
from config.config import config

async def example_1_find_failed_jobs():
    """Example 1: Find all failed jobs in the last 24 hours"""
    print("🔍 Example 1: Finding failed jobs in the last 24 hours")
    print("=" * 60)

    try:
        result = await search_failed_job_runs(hours_back=24)
        print(result)
    except Exception as e:
        print(f"Error: {e}")

    print("\n")

async def example_2_analyze_specific_job():
    """Example 2: Analyze a specific job by name pattern"""
    print("🔍 Example 2: Analyzing ETL jobs")
    print("=" * 60)

    job_pattern = "etl"  # Search for jobs containing "etl"

    try:
        # Find jobs matching pattern
        jobs_result = await search_databricks_jobs(job_pattern)
        print("Found jobs:")
        print(jobs_result)

        # Get cluster IDs for recent runs
        start_time = (datetime.now() - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')
        end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        cluster_ids_result = await get_job_cluster_ids(job_pattern, start_time, end_time)
        print("\nCluster IDs:")
        print(cluster_ids_result)

    except Exception as e:
        print(f"Error: {e}")

    print("\n")

async def example_3_analyze_cluster_logs():
    """Example 3: Analyze logs for a specific cluster"""
    print("🔍 Example 3: Analyzing cluster logs")
    print("=" * 60)

    # Replace with an actual cluster ID from your environment
    cluster_id = "cluster-example-123"

    try:
        result = await analyze_cluster_logs(cluster_id, search_pattern="error", max_files=5)
        print(result)
    except Exception as e:
        print(f"Error analyzing cluster {cluster_id}: {e}")

    print("\n")

async def example_4_interactive_agent():
    """Example 4: Interactive agent conversation"""
    print("🤖 Example 4: Interactive agent conversation")
    print("=" * 60)

    # Create agent instance
    agent = create_databricks_log_agent()

    # Example conversation scenarios
    example_questions = [
        "Show me all failed jobs in the last 12 hours",
        "Find jobs with 'pipeline' in the name that failed today",
        "What are the most common error patterns in recent job failures?",
        "Analyze memory issues in cluster logs"
    ]

    print("Example questions you could ask the agent:")
    for i, question in enumerate(example_questions, 1):
        print(f"{i}. {question}")

    print("\nTo have an actual conversation, run: python main.py")
    print("\n")

async def example_5_monitoring_scenario():
    """Example 5: Monitoring and alerting scenario"""
    print("📊 Example 5: Monitoring scenario")
    print("=" * 60)

    print("This example shows how you might use the agent in a monitoring pipeline:")

    monitoring_code = '''
async def monitor_critical_jobs():
    """Monitor critical jobs and alert on failures"""
    critical_job_patterns = ["daily_etl", "ml_training", "data_export"]

    for job_pattern in critical_job_patterns:
        try:
            # Check for failures in last 2 hours
            failed_runs = await search_failed_job_runs(job_pattern, hours_back=2)

            if failed_runs:
                # Analyze the failure
                # Get cluster IDs and analyze logs
                cluster_ids = await get_job_cluster_ids(job_pattern)

                for cluster_id in cluster_ids:
                    analysis = await analyze_cluster_logs(cluster_id)
                    # Send alert with analysis
                    send_alert(f"Job {job_pattern} failed", analysis)

        except Exception as e:
            print(f"Error monitoring {job_pattern}: {e}")
'''

    print(monitoring_code)

def print_configuration_guide():
    """Print configuration setup guide"""
    print("⚙️  Configuration Setup Guide")
    print("=" * 60)

    print("Before running these examples, ensure you have configured:")
    print("\n1. Environment variables (see .env.example):")
    print("   - GOOGLE_API_KEY")
    print("   - DATABRICKS_HOST")
    print("   - DATABRICKS_TOKEN")
    print("   - AWS_ACCESS_KEY_ID")
    print("   - AWS_SECRET_ACCESS_KEY")
    print("   - DATABRICKS_LOGS_BUCKET")

    print("\n2. Databricks cluster log configuration:")
    print("   - Clusters must be configured to write logs to S3")
    print("   - S3 path should match DATABRICKS_LOGS_BUCKET/DATABRICKS_LOGS_PREFIX")

    print("\n3. AWS permissions:")
    print("   - Read access to the S3 bucket containing logs")
    print("   - Proper IAM policies for S3 operations")

    print("\n4. Databricks API permissions:")
    print("   - Token should have access to jobs and clusters APIs")

    print("\nRun 'python main.py --config-check' to verify your setup.")
    print("\n")

async def main():
    """Run all examples"""
    print("🚀 Databricks Log Analysis Agent - Examples")
    print("=" * 60)

    # Check if configuration is valid
    if not config.validate():
        print_configuration_guide()
        print("❌ Configuration is incomplete. Please set up your environment first.")
        return

    print("✅ Configuration looks good! Running examples...\n")

    # Run examples
    await example_1_find_failed_jobs()
    await example_2_analyze_specific_job()
    await example_3_analyze_cluster_logs()
    await example_4_interactive_agent()
    await example_5_monitoring_scenario()

    print("🎉 All examples completed!")
    print("\nNext steps:")
    print("1. Try running: python main.py")
    print("2. Ask the agent about your specific Databricks jobs")
    print("3. Integrate the agent into your monitoring workflows")

if __name__ == "__main__":
    asyncio.run(main())