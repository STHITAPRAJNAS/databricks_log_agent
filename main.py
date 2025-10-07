#!/usr/bin/env python3
"""
Databricks Log Analysis Agent - Main Entry Point

This script provides different ways to run the Databricks Log Analysis Agent:
1. Interactive CLI mode
2. Web UI mode (using ADK web interface)
3. API server mode

Usage:
    python main.py                  # Interactive CLI mode
    python main.py --web           # Launch web UI
    python main.py --api-server    # Launch API server
    python main.py --config-check  # Check configuration
"""

import os
import sys
import argparse
import asyncio
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from config.config import config
from src.agents.databricks_log_agent import create_databricks_log_agent

def check_configuration():
    """Check if all required configuration is present."""
    print("Checking configuration...")

    required_configs = [
        ("GOOGLE_API_KEY", config.google_api_key),
        ("DATABRICKS_HOST", config.databricks_host),
        ("DATABRICKS_TOKEN", config.databricks_token),
        ("AWS_ACCESS_KEY_ID", config.aws_access_key_id),
        ("AWS_SECRET_ACCESS_KEY", config.aws_secret_access_key),
        ("DATABRICKS_LOGS_BUCKET", config.databricks_logs_bucket),
    ]

    missing_configs = []
    for name, value in required_configs:
        status = "✓" if value else "✗"
        print(f"  {status} {name}: {'Set' if value else 'Missing'}")
        if not value:
            missing_configs.append(name)

    if missing_configs:
        print(f"\n❌ Missing required configuration: {', '.join(missing_configs)}")
        print("\nPlease set these environment variables or update your .env file.")
        print("See .env.example for reference.")
        return False
    else:
        print("\n✅ All required configuration is present!")
        return True

def run_interactive_mode():
    """Run the agent in interactive CLI mode."""
    if not check_configuration():
        sys.exit(1)

    print("\n🤖 Starting Databricks Log Analysis Agent...")
    print("=" * 50)

    agent = create_databricks_log_agent()

    print("\nAgent ready! You can ask questions about Databricks job logs.")
    print("\nExample questions:")
    print("- 'My ETL job failed this morning, can you help me find out why?'")
    print("- 'Show me all failed jobs in the last 24 hours'")
    print("- 'Analyze logs for job pattern \"data_pipeline\" from yesterday'")
    print("- 'Search for OutOfMemoryError in cluster logs'")
    print("\nType 'quit' or 'exit' to stop.\n")

    while True:
        try:
            user_input = input("\n👤 You: ").strip()

            if user_input.lower() in ['quit', 'exit', 'q']:
                print("\n👋 Goodbye!")
                break

            if not user_input:
                continue

            print("\n🤖 Agent: Analyzing your request...")

            # This would be the actual agent interaction in a real implementation
            # For now, we'll show a placeholder response
            print("Agent response would appear here after integration with ADK runtime.")

        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"\n❌ Error: {str(e)}")

def run_web_mode():
    """Launch the agent in web UI mode."""
    if not check_configuration():
        sys.exit(1)

    print("\n🌐 Launching Databricks Log Analysis Agent Web UI...")
    print("This would launch the ADK web interface.")
    print("Run: adk web")

def run_api_server():
    """Launch the agent as an API server."""
    if not check_configuration():
        sys.exit(1)

    print("\n🔗 Launching Databricks Log Analysis Agent API Server...")
    print("This would launch the ADK API server.")
    print("Run: adk api_server")

def main():
    parser = argparse.ArgumentParser(
        description="Databricks Log Analysis Agent",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                  # Interactive CLI mode
  python main.py --web           # Launch web UI
  python main.py --api-server    # Launch API server
  python main.py --config-check  # Check configuration only
        """
    )

    parser.add_argument(
        "--web",
        action="store_true",
        help="Launch web UI mode"
    )

    parser.add_argument(
        "--api-server",
        action="store_true",
        help="Launch API server mode"
    )

    parser.add_argument(
        "--config-check",
        action="store_true",
        help="Check configuration and exit"
    )

    args = parser.parse_args()

    if args.config_check:
        check_configuration()
        return

    if args.web:
        run_web_mode()
    elif args.api_server:
        run_api_server()
    else:
        run_interactive_mode()

if __name__ == "__main__":
    main()