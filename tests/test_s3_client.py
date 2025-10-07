import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock
from src.utils.s3_client import S3LogClient

@pytest.fixture
def s3_client():
    with patch('src.utils.s3_client.config') as mock_config:
        mock_config.aws_access_key_id = "test_key"
        mock_config.aws_secret_access_key = "test_secret"
        mock_config.aws_region = "us-west-2"
        mock_config.databricks_logs_bucket = "test-bucket"
        mock_config.databricks_logs_prefix = "databrickslogs"

        return S3LogClient()

@pytest.mark.asyncio
async def test_build_log_path(s3_client):
    cluster_id = "cluster-123"
    expected_path = "databrickslogs/cluster-123"

    result = s3_client.build_log_path(cluster_id)
    assert result == expected_path

@pytest.mark.asyncio
async def test_classify_log_type(s3_client):
    test_cases = [
        ("stderr", "stderr"),
        ("stdout", "stdout"),
        ("log4j-active.log", "log4j"),
        ("driver.log", "driver"),
        ("executor-1.log", "executor"),
        ("app.log.gz", "compressed"),
        ("unknown.txt", "unknown")
    ]

    for filename, expected_type in test_cases:
        result = s3_client._classify_log_type(filename)
        assert result == expected_type

@pytest.mark.asyncio
async def test_list_cluster_logs_success(s3_client):
    cluster_id = "cluster-123"

    mock_objects = [
        {
            'Key': 'databrickslogs/cluster-123/driver/stderr',
            'Size': 1024,
            'LastModified': '2024-01-01T00:00:00Z'
        },
        {
            'Key': 'databrickslogs/cluster-123/driver/stdout',
            'Size': 512,
            'LastModified': '2024-01-01T00:01:00Z'
        }
    ]

    with patch.object(s3_client.s3_client, 'get_paginator') as mock_paginator:
        mock_paginator.return_value.paginate.return_value = [
            {'Contents': mock_objects}
        ]

        result = await s3_client.list_cluster_logs(cluster_id)

        assert len(result) == 2
        assert result[0]['file_name'] == 'stdout'  # Should be sorted by last_modified desc
        assert result[0]['log_type'] == 'stdout'
        assert result[1]['file_name'] == 'stderr'
        assert result[1]['log_type'] == 'stderr'

@pytest.mark.asyncio
async def test_search_error_patterns(s3_client):
    cluster_id = "cluster-123"

    mock_log_files = [
        {
            'key': 'databrickslogs/cluster-123/driver/stderr',
            'file_name': 'stderr',
            'log_type': 'stderr'
        }
    ]

    mock_content = """
    2024-01-01 10:00:00 INFO Starting job
    2024-01-01 10:01:00 ERROR java.lang.OutOfMemoryError: Java heap space
    2024-01-01 10:02:00 ERROR Job 123 failed due to stage failure
    """

    with patch.object(s3_client, 'list_cluster_logs', return_value=mock_log_files):
        with patch.object(s3_client, 'download_and_read_log', return_value=mock_content):
            result = await s3_client.search_error_patterns(cluster_id)

            assert 'memory_errors' in result
            assert 'job_failures' in result
            assert len(result['memory_errors']) > 0
            assert len(result['job_failures']) > 0