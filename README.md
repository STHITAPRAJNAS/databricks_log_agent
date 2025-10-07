# Databricks Log Analysis Agent

A sophisticated AI agent built with Google's Agent Development Kit (ADK) that intelligently analyzes Databricks job logs stored in S3 to help diagnose and resolve Spark job failures and performance issues.

## 🎯 Features

- **Smart Job Discovery**: Search and identify Databricks jobs by name patterns
- **Intelligent Log Analysis**: Automatically prioritize and analyze log files for error patterns
- **Cluster ID Resolution**: Map job executions to cluster IDs for precise log retrieval
- **Error Pattern Recognition**: Identify common Spark errors like memory issues, serialization problems, and SQL errors
- **Token-Optimized Analysis**: Smart chunking and summarization to work within LLM token limits
- **Interactive Q&A**: Ask clarifying questions to narrow down issues effectively
- **Multiple Interfaces**: CLI, Web UI, and API server modes

## 🏗️ Architecture

```
databricks_log_agent/
├── src/
│   ├── agents/              # Main ADK agent implementation
│   ├── tools/               # Databricks and S3 interaction tools
│   └── utils/               # Core utilities (S3 client, log analyzer, etc.)
├── config/                  # Configuration management
├── tests/                   # Test suite
├── main.py                  # Main entry point
└── requirements.txt         # Dependencies
```

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- Google AI Studio API key or Vertex AI access
- Databricks workspace access with API token
- AWS credentials for S3 access
- Databricks cluster logs configured to be stored in S3

### Installation

1. **Clone and setup the project:**
```bash
git clone <repository-url>
cd databricks_log_agent
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

2. **Configure environment variables:**
```bash
cp .env.example .env
# Edit .env with your credentials
```

Required environment variables:
```env
# Google AI Studio API Key
GOOGLE_API_KEY=your_google_ai_studio_api_key

# Databricks Configuration
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your_databricks_token

# AWS Configuration for S3 Access
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_REGION=us-west-2

# S3 Bucket Configuration
DATABRICKS_LOGS_BUCKET=your-databricks-logs-bucket
DATABRICKS_LOGS_PREFIX=databrickslogs
```

3. **Verify configuration:**
```bash
python main.py --config-check
```

### Running the Agent

#### Interactive CLI Mode (Default)
```bash
python main.py
```

#### Web UI Mode
```bash
python main.py --web
# Or use ADK directly: adk web
```

#### API Server Mode
```bash
python main.py --api-server
# Or use ADK directly: adk api_server
```

## 💬 Usage Examples

### Example Conversations

**1. Investigating a Failed Job:**
```
👤 You: My ETL job failed this morning, can you help me find out why?

🤖 Agent: I'll help you analyze the ETL job failure. To get started, I need a few details:
1. What's the name or pattern of the ETL job?
2. What time did it run this morning (approximate time range)?
3. Did you see any specific error messages in the Databricks UI?

👤 You: The job is called "daily_etl_pipeline" and it failed around 6:30 AM EST

🤖 Agent: Let me search for your job and analyze what happened...
[Agent searches for jobs, finds failed runs, analyzes cluster logs, and provides detailed error analysis]
```

**2. Analyzing Recent Failures:**
```
👤 You: Show me all failed jobs in the last 24 hours

🤖 Agent: I'll search for all failed job runs in the last 24 hours...
[Agent provides comprehensive failure analysis with cluster IDs and error summaries]
```

**3. Specific Error Investigation:**
```
👤 You: Search for OutOfMemoryError in cluster logs for cluster-id-12345

🤖 Agent: Searching for OutOfMemoryError patterns in cluster-id-12345...
[Agent provides detailed memory error analysis with suggestions for memory optimization]
```

## 🧠 Agent Capabilities

### Core Tools

1. **`search_databricks_jobs`**: Find jobs by name patterns
2. **`get_job_cluster_ids`**: Get cluster IDs for specific job runs and time ranges
3. **`search_failed_job_runs`**: Find failed executions across jobs
4. **`list_cluster_logs`**: List available log files for a cluster
5. **`analyze_cluster_logs`**: Comprehensive log analysis with error detection
6. **`search_log_pattern`**: Search specific patterns in logs
7. **`get_job_execution_timeline`**: Track job execution patterns over time

### Intelligent Features

- **Error Pattern Recognition**: Automatically detects common Spark issues:
  - Memory errors (OutOfMemoryError, container kills)
  - SQL analysis errors (table not found, column resolution)
  - I/O errors (file not found, permission issues)
  - Network connectivity problems
  - Serialization issues

- **Smart Log Prioritization**: Focuses on high-value log files:
  - Prioritizes stderr, log4j, and driver logs
  - Scores log chunks by error density and relevance
  - Limits token usage while maintaining diagnostic value

- **Context-Aware Solutions**: Provides actionable recommendations:
  - Memory optimization strategies
  - Configuration adjustments
  - Code refactoring suggestions
  - Monitoring recommendations

## 🔧 Configuration

### Databricks Log Storage Setup

Ensure your Databricks clusters are configured to store logs in S3:

```python
cluster_config = {
    "cluster_log_conf": {
        "s3": {
            "destination": "s3://your-logs-bucket/databrickslogs",
            "region": "us-west-2"
        }
    }
}
```

### Expected S3 Log Structure

```
s3://your-bucket/databrickslogs/
├── cluster-id-1/
│   ├── driver/
│   │   ├── stderr
│   │   ├── stdout
│   │   └── log4j-active.log
│   ├── executor/
│   │   ├── stderr
│   │   ├── stdout
│   │   └── log4j-active.log.gz
│   └── init_scripts/
└── cluster-id-2/
    └── ...
```

## 🧪 Testing

Run the test suite:
```bash
python -m pytest tests/
```

Run specific test categories:
```bash
# Test S3 functionality
python -m pytest tests/test_s3_client.py

# Test log analysis
python -m pytest tests/test_log_analyzer.py

# Test Databricks integration
python -m pytest tests/test_databricks_client.py
```

## 🚀 Advanced Usage

### Custom Error Patterns

Extend the error detection by modifying `src/utils/log_analyzer.py`:

```python
self.error_patterns = {
    'custom_app_errors': {
        'patterns': [
            r'MyAppException',
            r'Custom error pattern'
        ],
        'severity': 'high',
        'category': 'Application'
    }
}
```

### Integration with Monitoring Systems

The agent can be integrated with monitoring systems:

```python
from src.agents.databricks_log_agent import create_databricks_log_agent

# Create agent instance
agent = create_databricks_log_agent()

# Use in monitoring pipeline
def check_job_health(job_pattern: str):
    failed_runs = await search_failed_job_runs(job_pattern, hours_back=1)
    if failed_runs:
        analysis = await analyze_cluster_logs(cluster_id)
        # Send alert with analysis
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Commit changes: `git commit -am 'Add your feature'`
4. Push to branch: `git push origin feature/your-feature`
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Troubleshooting

### Common Issues

**1. Configuration Errors:**
```bash
# Check configuration
python main.py --config-check
```

**2. S3 Access Issues:**
- Verify AWS credentials and permissions
- Ensure S3 bucket exists and is accessible
- Check bucket policy allows list and read operations

**3. Databricks API Issues:**
- Verify Databricks host URL format
- Check token permissions (needs jobs and clusters read access)
- Ensure workspace is accessible

**4. Log Analysis Issues:**
- Verify cluster logs are being written to S3
- Check log retention policies
- Ensure cluster IDs are correct

### Performance Optimization

- Adjust `MAX_TOKEN_LIMIT` for your LLM's context window
- Modify `LOG_SEARCH_CHUNK_SIZE` for memory optimization
- Use time range filters to limit log analysis scope

## 📞 Support

For questions and support:
1. Check the troubleshooting section above
2. Review the [Google ADK documentation](https://google.github.io/adk-docs/)
3. Open an issue in the repository
4. Contact the development team

---

Built with ❤️ using Google's Agent Development Kit