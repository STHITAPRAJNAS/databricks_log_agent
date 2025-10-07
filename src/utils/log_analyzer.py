import re
import asyncio
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from config.config import config

@dataclass
class LogChunk:
    content: str
    chunk_id: int
    file_name: str
    log_type: str
    priority_score: float
    error_indicators: List[str]

@dataclass
class ErrorSummary:
    error_type: str
    description: str
    frequency: int
    severity: str
    suggested_solution: str
    relevant_logs: List[str]

class LogAnalyzer:
    def __init__(self):
        self.max_token_limit = config.max_token_limit
        self.chunk_size = config.log_search_chunk_size

        self.error_patterns = {
            'spark_sql_errors': {
                'patterns': [
                    r'AnalysisException',
                    r'org\.apache\.spark\.sql\.AnalysisException',
                    r'Table or view not found',
                    r'cannot resolve.*given input columns'
                ],
                'severity': 'high',
                'category': 'SQL Analysis'
            },
            'memory_errors': {
                'patterns': [
                    r'OutOfMemoryError',
                    r'java\.lang\.OutOfMemoryError',
                    r'Container killed.*memory',
                    r'Executor lost.*OutOfMemory'
                ],
                'severity': 'critical',
                'category': 'Memory Management'
            },
            'io_errors': {
                'patterns': [
                    r'FileNotFoundException',
                    r'IOException',
                    r'No such file or directory',
                    r'Permission denied'
                ],
                'severity': 'high',
                'category': 'File I/O'
            },
            'network_errors': {
                'patterns': [
                    r'Connection refused',
                    r'Connection timeout',
                    r'UnknownHostException',
                    r'SocketTimeoutException'
                ],
                'severity': 'medium',
                'category': 'Network'
            },
            'serialization_errors': {
                'patterns': [
                    r'SerializationException',
                    r'NotSerializableException',
                    r'Task not serializable'
                ],
                'severity': 'high',
                'category': 'Serialization'
            }
        }

        self.solution_templates = {
            'SQL Analysis': [
                "Check if all referenced tables/views exist and are accessible",
                "Verify column names and their spelling in the query",
                "Ensure proper schema qualification for tables",
                "Check permissions on referenced objects"
            ],
            'Memory Management': [
                "Increase driver or executor memory allocation",
                "Optimize partitioning to reduce data skew",
                "Use repartition() or coalesce() to manage partition sizes",
                "Consider caching intermediate results efficiently",
                "Review broadcast join thresholds"
            ],
            'File I/O': [
                "Verify file paths and permissions",
                "Check if files exist in the specified location",
                "Ensure proper credentials for S3/cloud storage access",
                "Review file format compatibility"
            ],
            'Network': [
                "Check network connectivity to external services",
                "Verify firewall and security group settings",
                "Increase timeout values if appropriate",
                "Check service endpoint availability"
            ],
            'Serialization': [
                "Ensure all objects used in transformations are serializable",
                "Avoid using non-serializable objects in closures",
                "Use broadcast variables for large read-only data",
                "Consider using case classes instead of regular classes"
            ]
        }

    async def analyze_logs(self, log_content: Dict[str, str],
                          search_query: Optional[str] = None) -> Dict[str, any]:
        chunked_logs = await self._chunk_and_prioritize_logs(log_content, search_query)
        error_summary = await self._analyze_errors(chunked_logs)
        optimized_content = await self._create_optimized_summary(chunked_logs, error_summary)

        return {
            'error_summary': error_summary,
            'prioritized_chunks': chunked_logs[:10],
            'optimized_content': optimized_content,
            'token_estimate': self._estimate_tokens(optimized_content)
        }

    async def _chunk_and_prioritize_logs(self, log_content: Dict[str, str],
                                       search_query: Optional[str] = None) -> List[LogChunk]:
        chunks = []
        chunk_id = 0

        for file_name, content in log_content.items():
            log_type = self._determine_log_type(file_name)
            lines = content.split('\n')

            for i in range(0, len(lines), self.chunk_size // 100):
                chunk_lines = lines[i:i + self.chunk_size // 100]
                chunk_content = '\n'.join(chunk_lines)

                if len(chunk_content.strip()) == 0:
                    continue

                priority_score = await self._calculate_priority_score(
                    chunk_content, log_type, search_query
                )

                error_indicators = self._find_error_indicators(chunk_content)

                chunk = LogChunk(
                    content=chunk_content,
                    chunk_id=chunk_id,
                    file_name=file_name,
                    log_type=log_type,
                    priority_score=priority_score,
                    error_indicators=error_indicators
                )

                chunks.append(chunk)
                chunk_id += 1

        return sorted(chunks, key=lambda x: x.priority_score, reverse=True)

    async def _calculate_priority_score(self, content: str, log_type: str,
                                      search_query: Optional[str] = None) -> float:
        score = 0.0

        log_type_weights = {
            'stderr': 1.0,
            'log4j': 0.8,
            'driver': 0.9,
            'executor': 0.7,
            'stdout': 0.3,
            'unknown': 0.1
        }

        score += log_type_weights.get(log_type, 0.1)

        for error_type, config in self.error_patterns.items():
            for pattern in config['patterns']:
                matches = len(re.findall(pattern, content, re.IGNORECASE))
                if matches > 0:
                    severity_weight = {'critical': 3.0, 'high': 2.0, 'medium': 1.0}.get(
                        config['severity'], 0.5
                    )
                    score += matches * severity_weight

        if search_query:
            query_matches = len(re.findall(re.escape(search_query), content, re.IGNORECASE))
            score += query_matches * 2.0

        error_keywords = ['exception', 'error', 'failed', 'failure', 'fatal']
        for keyword in error_keywords:
            matches = len(re.findall(keyword, content, re.IGNORECASE))
            score += matches * 0.5

        return score

    def _determine_log_type(self, file_name: str) -> str:
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
        else:
            return 'unknown'

    def _find_error_indicators(self, content: str) -> List[str]:
        indicators = []
        for error_type, config in self.error_patterns.items():
            for pattern in config['patterns']:
                if re.search(pattern, content, re.IGNORECASE):
                    indicators.append(error_type)
                    break
        return indicators

    async def _analyze_errors(self, chunks: List[LogChunk]) -> List[ErrorSummary]:
        error_counts = {}
        error_examples = {}

        for chunk in chunks:
            for error_indicator in chunk.error_indicators:
                if error_indicator not in error_counts:
                    error_counts[error_indicator] = 0
                    error_examples[error_indicator] = []

                error_counts[error_indicator] += 1
                if len(error_examples[error_indicator]) < 3:
                    error_examples[error_indicator].append({
                        'file': chunk.file_name,
                        'content': chunk.content[:500]
                    })

        summaries = []
        for error_type, count in error_counts.items():
            if error_type in self.error_patterns:
                config = self.error_patterns[error_type]
                category = config['category']

                summary = ErrorSummary(
                    error_type=error_type,
                    description=f"{category} related issues detected",
                    frequency=count,
                    severity=config['severity'],
                    suggested_solution='; '.join(self.solution_templates.get(category, [])),
                    relevant_logs=[ex['file'] for ex in error_examples[error_type]]
                )
                summaries.append(summary)

        return sorted(summaries, key=lambda x: (x.severity, x.frequency), reverse=True)

    async def _create_optimized_summary(self, chunks: List[LogChunk],
                                      error_summary: List[ErrorSummary]) -> str:
        summary_parts = []

        summary_parts.append("=== LOG ANALYSIS SUMMARY ===\n")

        if error_summary:
            summary_parts.append("IDENTIFIED ERRORS:")
            for error in error_summary[:5]:
                summary_parts.append(f"• {error.error_type.upper()} ({error.severity}): "
                                   f"Found {error.frequency} occurrences")
                summary_parts.append(f"  Solution: {error.suggested_solution[:200]}...")
                summary_parts.append("")

        summary_parts.append("MOST RELEVANT LOG EXCERPTS:")
        for i, chunk in enumerate(chunks[:5]):
            summary_parts.append(f"\n--- Excerpt {i+1} from {chunk.file_name} ---")
            summary_parts.append(f"Priority: {chunk.priority_score:.2f} | Errors: {chunk.error_indicators}")
            summary_parts.append(chunk.content[:1000])
            if len(chunk.content) > 1000:
                summary_parts.append("... (truncated)")

        full_summary = '\n'.join(summary_parts)

        if self._estimate_tokens(full_summary) > self.max_token_limit:
            return self._truncate_to_token_limit(full_summary)

        return full_summary

    def _estimate_tokens(self, text: str) -> int:
        return len(text.split()) * 1.3

    def _truncate_to_token_limit(self, text: str) -> str:
        words = text.split()
        target_words = int(self.max_token_limit / 1.3)

        if len(words) <= target_words:
            return text

        truncated = ' '.join(words[:target_words])
        truncated += f"\n\n... (Content truncated to stay within {self.max_token_limit} token limit)"
        return truncated