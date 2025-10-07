import boto3
import gzip
import asyncio
import aiofiles
from typing import List, Dict, Optional, AsyncGenerator
from botocore.exceptions import ClientError, NoCredentialsError
from config.config import config
import tempfile
import os
from datetime import datetime
import re

class S3LogClient:
    def __init__(self):
        self.session = boto3.Session(
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            region_name=config.aws_region
        )
        self.s3_client = self.session.client('s3')
        self.bucket = config.databricks_logs_bucket
        self.prefix = config.databricks_logs_prefix

    def build_log_path(self, cluster_id: str) -> str:
        return f"{self.prefix}/{cluster_id}"

    async def list_cluster_logs(self, cluster_id: str) -> List[Dict[str, str]]:
        log_path = self.build_log_path(cluster_id)

        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket, Prefix=log_path)

            log_files = []
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        file_name = os.path.basename(key)
                        log_files.append({
                            'key': key,
                            'file_name': file_name,
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'].isoformat(),
                            'log_type': self._classify_log_type(file_name)
                        })

            return sorted(log_files, key=lambda x: x['last_modified'], reverse=True)

        except ClientError as e:
            raise Exception(f"Failed to list logs for cluster {cluster_id}: {str(e)}")

    def _classify_log_type(self, file_name: str) -> str:
        file_name = file_name.lower()
        if 'stderr' in file_name:
            return 'stderr'
        elif 'stdout' in file_name:
            return 'stdout'
        elif 'log4j' in file_name:
            return 'log4j'
        elif 'driver' in file_name:
            return 'driver'
        elif 'executor' in file_name:
            return 'executor'
        elif file_name.endswith('.gz'):
            return 'compressed'
        else:
            return 'unknown'

    async def download_and_read_log(self, s3_key: str, max_size_mb: int = 50) -> str:
        try:
            # Get object metadata to check size
            response = self.s3_client.head_object(Bucket=self.bucket, Key=s3_key)
            file_size = response['ContentLength']

            if file_size > max_size_mb * 1024 * 1024:
                raise Exception(f"Log file {s3_key} is too large ({file_size / (1024*1024):.1f}MB). "
                              f"Maximum allowed: {max_size_mb}MB")

            # Download the file
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                self.s3_client.download_fileobj(self.bucket, s3_key, temp_file)
                temp_file_path = temp_file.name

            try:
                # Read the file content
                if s3_key.endswith('.gz'):
                    with gzip.open(temp_file_path, 'rt', encoding='utf-8') as f:
                        content = f.read()
                else:
                    async with aiofiles.open(temp_file_path, 'r', encoding='utf-8') as f:
                        content = await f.read()

                return content
            finally:
                os.unlink(temp_file_path)

        except ClientError as e:
            raise Exception(f"Failed to download log {s3_key}: {str(e)}")
        except UnicodeDecodeError:
            raise Exception(f"Failed to decode log file {s3_key}. File may be binary or corrupted.")

    async def get_log_snippet(self, s3_key: str, search_pattern: str = None,
                            max_lines: int = 100, around_lines: int = 5) -> str:
        try:
            content = await self.download_and_read_log(s3_key, max_size_mb=10)
            lines = content.split('\n')

            if search_pattern:
                pattern = re.compile(search_pattern, re.IGNORECASE)
                matching_lines = []

                for i, line in enumerate(lines):
                    if pattern.search(line):
                        start = max(0, i - around_lines)
                        end = min(len(lines), i + around_lines + 1)

                        context_lines = []
                        for j in range(start, end):
                            prefix = ">>>" if j == i else "   "
                            context_lines.append(f"{prefix} {j+1:4d}: {lines[j]}")

                        matching_lines.extend(context_lines)
                        matching_lines.append("---")

                        if len(matching_lines) > max_lines * 2:
                            break

                return '\n'.join(matching_lines[:max_lines * 2])
            else:
                return '\n'.join([f"{i+1:4d}: {line}" for i, line in enumerate(lines[:max_lines])])

        except Exception as e:
            return f"Error reading log snippet: {str(e)}"

    async def search_error_patterns(self, cluster_id: str,
                                   custom_patterns: Dict[str, List[str]] = None,
                                   max_errors_per_category: int = 50) -> Dict[str, List[str]]:
        """
        Enhanced error pattern search with comprehensive regex patterns.
        """
        log_files = await self.list_cluster_logs(cluster_id)

        # Enhanced comprehensive error patterns
        default_patterns = {
            'critical_errors': [
                r'(?i)\bfatal\s+error\b',
                r'(?i)\bcritical\s+error\b',
                r'(?i)\boutofmemoryerror\b',
                r'(?i)\bsegmentation\s+fault\b',
                r'(?i)\bcore\s+dumped\b',
                r'(?i)\bpanic\b.*\berror\b',
                r'(?i)\bcrash\b.*\berror\b'
            ],
            'spark_errors': [
                r'(?i)\banalysisexception\b',
                r'(?i)\bsparkexception\b',
                r'(?i)org\.apache\.spark\..*exception',
                r'(?i)\bjob\s+\d+\s+failed\b',
                r'(?i)\bstage\s+\d+\s+failed\b',
                r'(?i)\btask\s+failed\b',
                r'(?i)\bexecutor\s+lost\b',
                r'(?i)\bdriver\s+stacktrace\b',
                r'(?i)\bshuffle\s+fetch\s+failed\b'
            ],
            'memory_issues': [
                r'(?i)\boutofmemoryerror\b',
                r'(?i)java\.lang\.outofmemoryerror',
                r'(?i)\bcontainer\s+killed.*memory\b',
                r'(?i)\bgc\s+overhead\s+limit\b',
                r'(?i)\bheap\s+space\s+exhausted\b',
                r'(?i)\bmetaspace\s+out\s+of\s+memory\b',
                r'(?i)\bdirect\s+buffer\s+memory\b'
            ],
            'io_errors': [
                r'(?i)\bfilenotfoundexception\b',
                r'(?i)\bioexception\b',
                r'(?i)\bno\s+such\s+file\s+or\s+directory\b',
                r'(?i)\bpermission\s+denied\b',
                r'(?i)\baccess\s+denied\b',
                r'(?i)\bdisk\s+space\s+exhausted\b',
                r'(?i)\bno\s+space\s+left\s+on\s+device\b',
                r'(?i)\bread\s+timed\s+out\b'
            ],
            'network_errors': [
                r'(?i)\bconnection\s+refused\b',
                r'(?i)\bconnection\s+timeout\b',
                r'(?i)\bconnection\s+reset\b',
                r'(?i)\bunknownhostexception\b',
                r'(?i)\bsockettimeoutexception\b',
                r'(?i)\bnetwork\s+is\s+unreachable\b',
                r'(?i)\bhost\s+is\s+unreachable\b',
                r'(?i)\bbroken\s+pipe\b'
            ],
            'application_errors': [
                r'(?i)\bnullpointerexception\b',
                r'(?i)\bclassnotfoundexception\b',
                r'(?i)\bclasscastexception\b',
                r'(?i)\bnumberformatexception\b',
                r'(?i)\billegalargumentexception\b',
                r'(?i)\billegalstateexception\b',
                r'(?i)\bunsupportedoperationexception\b',
                r'(?i)\bruntimeexception\b'
            ],
            'authentication_errors': [
                r'(?i)\bauthentication\s+failed\b',
                r'(?i)\bauthorization\s+failed\b',
                r'(?i)\baccess\s+token\s+expired\b',
                r'(?i)\binvalid\s+credentials\b',
                r'(?i)\bunauthorized\s+access\b',
                r'(?i)\bforbidden\s+access\b',
                r'(?i)\bssl\s+certificate\s+error\b'
            ],
            'configuration_errors': [
                r'(?i)\bconfiguration\s+error\b',
                r'(?i)\binvalid\s+configuration\b',
                r'(?i)\bmissing\s+configuration\b',
                r'(?i)\bproperty\s+not\s+found\b',
                r'(?i)\benvironment\s+variable\s+not\s+set\b',
                r'(?i)\bclasspath\s+error\b',
                r'(?i)\blibrary\s+not\s+found\b'
            ]
        }

        # Use custom patterns if provided, otherwise use defaults
        error_patterns = custom_patterns if custom_patterns else default_patterns
        found_errors = {category: [] for category in error_patterns}

        # Priority order for log files (most likely to contain errors first)
        priority_order = ['stderr', 'log4j', 'driver', 'executor', 'stdout', 'unknown']
        sorted_log_files = sorted(log_files,
                                key=lambda x: priority_order.index(x['log_type'])
                                if x['log_type'] in priority_order else 99)

        for log_file in sorted_log_files:
            try:
                content = await self.download_and_read_log(log_file['key'], max_size_mb=25)

                # Search for patterns in this file
                file_errors = await self._search_patterns_in_content(
                    content, error_patterns, log_file, max_errors_per_category
                )

                # Merge results
                for category, errors in file_errors.items():
                    found_errors[category].extend(errors)

                    # Trim if exceeded limit
                    if len(found_errors[category]) > max_errors_per_category:
                        found_errors[category] = found_errors[category][:max_errors_per_category]

            except Exception as e:
                print(f"Error searching in {log_file['key']}: {str(e)}")
                continue

        return found_errors

    async def _search_patterns_in_content(self, content: str,
                                        error_patterns: Dict[str, List[str]],
                                        log_file: Dict,
                                        max_per_category: int) -> Dict[str, List[Dict]]:
        """
        Search for error patterns within log content with enhanced context extraction.
        """
        file_errors = {category: [] for category in error_patterns}
        lines = content.split('\n')

        for category, patterns in error_patterns.items():
            for pattern in patterns:
                try:
                    matches = list(re.finditer(pattern, content, re.IGNORECASE | re.MULTILINE))

                    for match in matches:
                        if len(file_errors[category]) >= max_per_category:
                            break

                        # Find the line containing the match
                        line_start = content.rfind('\n', 0, match.start()) + 1
                        line_end = content.find('\n', match.end())
                        if line_end == -1:
                            line_end = len(content)

                        error_line = content[line_start:line_end].strip()

                        # Get surrounding context (2 lines before and after)
                        line_num = content[:match.start()].count('\n')
                        context_start = max(0, line_num - 2)
                        context_end = min(len(lines), line_num + 3)
                        context_lines = lines[context_start:context_end]

                        # Extract timestamp if present
                        timestamp_match = re.search(
                            r'\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}',
                            error_line
                        )
                        timestamp = timestamp_match.group() if timestamp_match else None

                        file_errors[category].append({
                            'file': log_file['file_name'],
                            'file_type': log_file['log_type'],
                            'pattern': pattern,
                            'matched_text': match.group(),
                            'line': error_line,
                            'line_number': line_num + 1,
                            'context': '\n'.join(context_lines),
                            'timestamp': timestamp,
                            'position': match.start(),
                            'severity': self._classify_error_severity(error_line, category)
                        })

                except re.error as e:
                    print(f"Invalid regex pattern '{pattern}': {str(e)}")
                    continue

                if len(file_errors[category]) >= max_per_category:
                    break

        return file_errors

    def _classify_error_severity(self, error_line: str, category: str) -> str:
        """
        Classify error severity based on keywords and category.
        """
        error_line_lower = error_line.lower()

        # Critical keywords
        if any(keyword in error_line_lower for keyword in
               ['fatal', 'critical', 'panic', 'crash', 'segmentation fault', 'core dumped']):
            return 'critical'

        # High severity keywords
        if any(keyword in error_line_lower for keyword in
               ['outofmemoryerror', 'failed', 'exception', 'severe']):
            return 'high'

        # Medium severity based on category
        if category in ['memory_issues', 'critical_errors', 'spark_errors']:
            return 'high'
        elif category in ['application_errors', 'io_errors', 'authentication_errors']:
            return 'medium'
        else:
            return 'low'

    async def search_iterative_patterns(self, cluster_id: str,
                                      max_iterations: int = 3) -> Dict[str, any]:
        """
        Iteratively search for error patterns, starting with most critical.
        """
        iteration_results = []
        all_errors = {}

        # Define pattern priority levels
        pattern_levels = [
            # Level 1: Critical errors only
            {
                'critical_errors': [
                    r'(?i)\bfatal\s+error\b',
                    r'(?i)\boutofmemoryerror\b',
                    r'(?i)\bsegmentation\s+fault\b'
                ]
            },
            # Level 2: Add Spark and memory errors
            {
                'spark_errors': [
                    r'(?i)\banalysisexception\b',
                    r'(?i)\bjob\s+\d+\s+failed\b',
                    r'(?i)\btask\s+failed\b'
                ],
                'memory_issues': [
                    r'(?i)\boutofmemoryerror\b',
                    r'(?i)\bcontainer\s+killed.*memory\b'
                ]
            },
            # Level 3: Comprehensive search
            {}
        ]

        for iteration in range(min(max_iterations, len(pattern_levels))):
            patterns = pattern_levels[iteration] if iteration < len(pattern_levels) else None

            results = await self.search_error_patterns(
                cluster_id,
                custom_patterns=patterns,
                max_errors_per_category=20 if iteration == 0 else 50
            )

            iteration_results.append({
                'iteration': iteration + 1,
                'patterns_used': list(patterns.keys()) if patterns else 'comprehensive',
                'errors_found': sum(len(errors) for errors in results.values()),
                'categories': list(results.keys())
            })

            # Merge results
            for category, errors in results.items():
                if category not in all_errors:
                    all_errors[category] = []
                all_errors[category].extend(errors)

            # Early termination if critical errors found
            if iteration == 0 and results.get('critical_errors'):
                break

        return {
            'errors_by_category': all_errors,
            'iteration_summary': iteration_results,
            'total_errors': sum(len(errors) for errors in all_errors.values()),
            'categories_found': list(all_errors.keys())
        }