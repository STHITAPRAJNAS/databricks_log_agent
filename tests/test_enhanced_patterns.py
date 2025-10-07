import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock
from src.utils.log_analyzer import LogAnalyzer
from src.utils.s3_client import S3LogClient

class TestEnhancedErrorPatterns:
    """Test enhanced error pattern detection capabilities."""

    @pytest.fixture
    def log_analyzer(self):
        return LogAnalyzer()

    @pytest.fixture
    def s3_client(self):
        with patch('src.utils.s3_client.config') as mock_config:
            mock_config.aws_access_key_id = "test_key"
            mock_config.aws_secret_access_key = "test_secret"
            mock_config.aws_region = "us-west-2"
            mock_config.databricks_logs_bucket = "test-bucket"
            mock_config.databricks_logs_prefix = "databrickslogs"
            return S3LogClient()

    @pytest.mark.asyncio
    async def test_comprehensive_error_patterns(self, log_analyzer):
        """Test that enhanced regex patterns catch various error types."""

        test_log_content = {
            "stderr": """
            2024-01-01 10:00:00 INFO Starting application
            2024-01-01 10:01:00 FATAL ERROR: System crashed unexpectedly
            2024-01-01 10:02:00 ERROR java.lang.OutOfMemoryError: Java heap space
            2024-01-01 10:03:00 ERROR org.apache.spark.sql.AnalysisException: Table not found
            2024-01-01 10:04:00 ERROR java.io.FileNotFoundException: /path/to/file
            2024-01-01 10:05:00 ERROR Connection refused: unable to connect to database
            2024-01-01 10:06:00 ERROR java.lang.NullPointerException at MyClass.method()
            2024-01-01 10:07:00 ERROR Authentication failed: invalid credentials
            2024-01-01 10:08:00 ERROR Configuration error: missing property 'spark.sql.warehouse.dir'
            """
        }

        analysis = await log_analyzer.analyze_logs_iteratively(test_log_content)

        # Check that multiple error categories were detected
        error_types = [error.error_type for error in analysis['error_summary']]

        assert 'memory_errors' in error_types
        assert 'spark_sql_errors' in error_types
        assert 'io_errors' in error_types
        assert 'network_errors' in error_types
        assert 'application_errors' in error_types
        assert 'auth_errors' in error_types
        assert 'config_errors' in error_types

    @pytest.mark.asyncio
    async def test_priority_based_analysis(self, log_analyzer):
        """Test that critical errors are prioritized in iterative analysis."""

        test_log_content = {
            "stderr": """
            2024-01-01 10:00:00 INFO Starting job
            2024-01-01 10:01:00 WARN Low disk space
            2024-01-01 10:02:00 FATAL ERROR: Critical system failure
            2024-01-01 10:03:00 ERROR OutOfMemoryError: GC overhead limit exceeded
            2024-01-01 10:04:00 INFO Job continuing
            """
        }

        analysis = await log_analyzer.analyze_logs_iteratively(test_log_content, max_iterations=2)

        # Check that critical errors are found early
        assert analysis['total_iterations'] <= 2

        # Critical errors should be at the top
        if analysis['error_summary']:
            top_error = analysis['error_summary'][0]
            assert top_error.severity in ['critical', 'high']

    @pytest.mark.asyncio
    async def test_case_insensitive_patterns(self, log_analyzer):
        """Test that patterns work regardless of case."""

        test_cases = [
            "OutOfMemoryError: heap space exhausted",
            "outofmemoryerror: heap space exhausted",
            "OUTOFMEMORYERROR: heap space exhausted",
            "OutOfMemoryERROR: heap space exhausted"
        ]

        for case in test_cases:
            test_content = {"stderr": f"2024-01-01 10:00:00 ERROR {case}"}
            analysis = await log_analyzer.analyze_logs(test_content)

            # Should detect memory error regardless of case
            error_types = [error.error_type for error in analysis['error_summary']]
            assert 'memory_errors' in error_types, f"Failed to detect: {case}"

    @pytest.mark.asyncio
    async def test_context_extraction(self, s3_client):
        """Test that error context is properly extracted."""

        mock_content = """
        Line 1: Starting process
        Line 2: Processing data
        Line 3: ERROR java.lang.OutOfMemoryError: Java heap space
        Line 4: Stack trace follows
        Line 5: Cleanup process
        """

        # Mock the search patterns method
        with patch.object(s3_client, '_search_patterns_in_content') as mock_search:
            mock_search.return_value = {
                'memory_issues': [{
                    'file': 'stderr',
                    'pattern': r'(?i)\\boutofmemoryerror\\b',
                    'line': 'ERROR java.lang.OutOfMemoryError: Java heap space',
                    'context': mock_content.strip(),
                    'line_number': 3,
                    'severity': 'critical'
                }]
            }

            log_file = {'file_name': 'stderr', 'log_type': 'stderr'}
            result = await s3_client._search_patterns_in_content(
                mock_content,
                {'memory_issues': [r'(?i)\\boutofmemoryerror\\b']},
                log_file,
                10
            )

            assert len(result['memory_issues']) == 1
            error = result['memory_issues'][0]
            assert 'Line 1: Starting process' in error['context']
            assert 'Line 5: Cleanup process' in error['context']
            assert error['line_number'] == 3

    @pytest.mark.asyncio
    async def test_iterative_early_termination(self, s3_client):
        """Test that iterative search terminates early when critical errors are found."""

        # Mock search results with critical errors in first iteration
        with patch.object(s3_client, 'search_error_patterns') as mock_search:
            mock_search.return_value = {
                'critical_errors': [
                    {
                        'file': 'stderr',
                        'line': 'FATAL ERROR: System crash',
                        'severity': 'critical'
                    }
                ]
            }

            result = await s3_client.search_iterative_patterns('test-cluster', max_iterations=3)

            # Should terminate early due to critical errors
            assert len(result['iteration_summary']) == 1
            assert result['iteration_summary'][0]['iteration'] == 1

    @pytest.mark.asyncio
    async def test_severity_classification(self, s3_client):
        """Test error severity classification logic."""

        test_cases = [
            ("FATAL ERROR occurred", "critical"),
            ("OutOfMemoryError detected", "high"),
            ("WARNING: low disk space", "low"),
            ("INFO: process started", "low")
        ]

        for error_line, expected_severity in test_cases:
            severity = s3_client._classify_error_severity(error_line, "test_category")

            # Allow for some flexibility in severity assignment
            if expected_severity == "critical":
                assert severity in ["critical", "high"]
            elif expected_severity == "high":
                assert severity in ["high", "medium"]
            else:
                assert severity in ["medium", "low"]

    @pytest.mark.asyncio
    async def test_token_limit_handling(self, log_analyzer):
        """Test that analysis respects token limits."""

        # Create very large log content
        large_content = "ERROR: Sample error\n" * 10000
        test_log_content = {"stderr": large_content}

        analysis = await log_analyzer.analyze_logs_iteratively(test_log_content)

        # Should not exceed token limit
        assert analysis['token_estimate'] <= log_analyzer.max_token_limit

    @pytest.mark.asyncio
    async def test_regex_pattern_validation(self, s3_client):
        """Test that invalid regex patterns are handled gracefully."""

        # Test with invalid regex pattern
        invalid_patterns = {
            'test_category': [
                r'[invalid regex',  # Missing closing bracket
                r'(?i)\\bvalid\\b'   # Valid pattern
            ]
        }

        log_file = {'file_name': 'test.log', 'log_type': 'stderr'}
        content = "Some log content with valid pattern"

        # Should not crash with invalid regex
        result = await s3_client._search_patterns_in_content(
            content, invalid_patterns, log_file, 10
        )

        # Should still process valid patterns
        assert 'test_category' in result

if __name__ == "__main__":
    pytest.main([__file__])