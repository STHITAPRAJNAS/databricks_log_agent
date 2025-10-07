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

    async def search_error_patterns(self, cluster_id: str) -> Dict[str, List[str]]:
        log_files = await self.list_cluster_logs(cluster_id)
        error_patterns = {
            'spark_errors': [
                r'Exception', r'Error', r'FAILED', r'java\..*Exception',
                r'org\.apache\.spark\..*Exception', r'py4j\..*Error'
            ],
            'job_failures': [
                r'Job \d+ failed', r'Task failed', r'Stage \d+ failed',
                r'Application failed', r'Driver stacktrace'
            ],
            'resource_issues': [
                r'OutOfMemoryError', r'Container killed', r'Disk space',
                r'java\.lang\.OutOfMemoryError', r'No space left on device'
            ]
        }

        found_errors = {category: [] for category in error_patterns}

        for log_file in log_files:
            if log_file['log_type'] in ['stderr', 'log4j', 'driver']:
                try:
                    content = await self.download_and_read_log(log_file['key'], max_size_mb=20)

                    for category, patterns in error_patterns.items():
                        for pattern in patterns:
                            matches = re.finditer(pattern, content, re.IGNORECASE | re.MULTILINE)
                            for match in matches:
                                line_start = content.rfind('\n', 0, match.start()) + 1
                                line_end = content.find('\n', match.end())
                                if line_end == -1:
                                    line_end = len(content)

                                error_line = content[line_start:line_end].strip()
                                found_errors[category].append({
                                    'file': log_file['file_name'],
                                    'pattern': pattern,
                                    'line': error_line,
                                    'position': match.start()
                                })

                                if len(found_errors[category]) > 20:
                                    break

                        if len(found_errors[category]) > 20:
                            break

                except Exception as e:
                    print(f"Error searching in {log_file['key']}: {str(e)}")
                    continue

        return found_errors