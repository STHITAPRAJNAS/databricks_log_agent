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
            # Critical System Errors
            'memory_errors': {
                'patterns': [
                    r'(?i)outofmemoryerror',
                    r'(?i)java\.lang\.outofmemoryerror',
                    r'(?i)container\s+killed.*memory',
                    r'(?i)executor\s+lost.*outofmemory',
                    r'(?i)gc\s+overhead\s+limit\s+exceeded',
                    r'(?i)unable\s+to\s+allocate\s+.*\s+bytes',
                    r'(?i)heap\s+space\s+exhausted',
                    r'(?i)metaspace\s+out\s+of\s+memory',
                    r'(?i)direct\s+buffer\s+memory\s+exceeded',
                    r'(?i)oom\s+killer',
                    r'(?i)memory\s+allocation\s+failed'
                ],
                'severity': 'critical',
                'category': 'Memory Management',
                'priority': 100
            },

            # Spark-specific Errors
            'spark_sql_errors': {
                'patterns': [
                    r'(?i)analysisexception',
                    r'(?i)org\.apache\.spark\.sql\.analysisexception',
                    r'(?i)table\s+or\s+view\s+not\s+found',
                    r'(?i)cannot\s+resolve.*given\s+input\s+columns',
                    r'(?i)column\s+.*\s+does\s+not\s+exist',
                    r'(?i)path\s+does\s+not\s+exist',
                    r'(?i)schema\s+mismatch',
                    r'(?i)invalid\s+column\s+reference',
                    r'(?i)ambiguous\s+reference\s+to\s+fields',
                    r'(?i)unresolved\s+attribute',
                    r'(?i)unsupported\s+data\s+type'
                ],
                'severity': 'high',
                'category': 'SQL Analysis',
                'priority': 90
            },

            'spark_execution_errors': {
                'patterns': [
                    r'(?i)sparkexception',
                    r'(?i)org\.apache\.spark\.sparkexception',
                    r'(?i)job\s+\d+\s+failed',
                    r'(?i)stage\s+\d+\s+failed',
                    r'(?i)task\s+failed',
                    r'(?i)executor\s+\d+\s+lost',
                    r'(?i)driver\s+stacktrace',
                    r'(?i)application\s+failed',
                    r'(?i)shuffle\s+fetch\s+failed',
                    r'(?i)broadcast\s+failed',
                    r'(?i)rdd\s+operation\s+failed'
                ],
                'severity': 'high',
                'category': 'Spark Execution',
                'priority': 85
            },

            # Generic Application Errors
            'application_errors': {
                'patterns': [
                    r'(?i)\b(?:exception|error|failure|fatal)\b.*\n?.*\bat\s+[a-zA-Z0-9_.$]+\.[a-zA-Z0-9_$]+\(',
                    r'(?i)\bexception\s+in\s+thread\b',
                    r'(?i)caused\s+by:.*exception',
                    r'(?i)\berror\s*:\s*\w+',
                    r'(?i)\bfatal\s+error\b',
                    r'(?i)\bcritical\s+error\b',
                    r'(?i)\bsevere\s+error\b',
                    r'(?i)\bunhandled\s+exception\b',
                    r'(?i)\bstack\s+overflow\b',
                    r'(?i)\bsegmentation\s+fault\b',
                    r'(?i)\bcore\s+dumped\b'
                ],
                'severity': 'high',
                'category': 'Application Runtime',
                'priority': 80
            },

            # I/O and Resource Errors
            'io_errors': {
                'patterns': [
                    r'(?i)filenotfoundexception',
                    r'(?i)ioexception',
                    r'(?i)no\s+such\s+file\s+or\s+directory',
                    r'(?i)permission\s+denied',
                    r'(?i)access\s+denied',
                    r'(?i)disk\s+space\s+exhausted',
                    r'(?i)no\s+space\s+left\s+on\s+device',
                    r'(?i)read\s+timed\s+out',
                    r'(?i)write\s+failed',
                    r'(?i)cannot\s+open\s+file',
                    r'(?i)file\s+is\s+corrupted',
                    r'(?i)bad\s+file\s+descriptor',
                    r'(?i)resource\s+temporarily\s+unavailable'
                ],
                'severity': 'high',
                'category': 'File I/O',
                'priority': 75
            },

            # Network and Connectivity Errors
            'network_errors': {
                'patterns': [
                    r'(?i)connection\s+refused',
                    r'(?i)connection\s+timeout',
                    r'(?i)connection\s+reset',
                    r'(?i)unknownhostexception',
                    r'(?i)sockettimeoutexception',
                    r'(?i)network\s+is\s+unreachable',
                    r'(?i)host\s+is\s+unreachable',
                    r'(?i)broken\s+pipe',
                    r'(?i)connection\s+aborted',
                    r'(?i)ssl\s+handshake\s+failed',
                    r'(?i)certificate\s+error',
                    r'(?i)dns\s+resolution\s+failed'
                ],
                'severity': 'medium',
                'category': 'Network',
                'priority': 70
            },

            # Serialization and Type Errors
            'serialization_errors': {
                'patterns': [
                    r'(?i)serializationexception',
                    r'(?i)notserializableexception',
                    r'(?i)task\s+not\s+serializable',
                    r'(?i)classnotfoundexception',
                    r'(?i)classcastexception',
                    r'(?i)nosuchmethodexception',
                    r'(?i)incompatible\s+types',
                    r'(?i)type\s+mismatch',
                    r'(?i)serialization\s+failed',
                    r'(?i)deserialization\s+failed'
                ],
                'severity': 'high',
                'category': 'Serialization',
                'priority': 65
            },

            # Configuration and Environment Errors
            'config_errors': {
                'patterns': [
                    r'(?i)configuration\s+error',
                    r'(?i)invalid\s+configuration',
                    r'(?i)missing\s+configuration',
                    r'(?i)property\s+not\s+found',
                    r'(?i)environment\s+variable\s+not\s+set',
                    r'(?i)classpath\s+error',
                    r'(?i)library\s+not\s+found',
                    r'(?i)dependency\s+conflict',
                    r'(?i)version\s+mismatch',
                    r'(?i)unsupported\s+version'
                ],
                'severity': 'medium',
                'category': 'Configuration',
                'priority': 60
            },

            # Authentication and Security Errors
            'auth_errors': {
                'patterns': [
                    r'(?i)authentication\s+failed',
                    r'(?i)authorization\s+failed',
                    r'(?i)access\s+token\s+expired',
                    r'(?i)invalid\s+credentials',
                    r'(?i)permission\s+denied',
                    r'(?i)unauthorized\s+access',
                    r'(?i)forbidden\s+access',
                    r'(?i)ssl\s+certificate\s+error',
                    r'(?i)kerberos\s+authentication\s+failed',
                    r'(?i)security\s+exception'
                ],
                'severity': 'high',
                'category': 'Authentication & Security',
                'priority': 55
            },

            # Database and Storage Errors
            'database_errors': {
                'patterns': [
                    r'(?i)sql\s+exception',
                    r'(?i)database\s+connection\s+failed',
                    r'(?i)table\s+does\s+not\s+exist',
                    r'(?i)duplicate\s+key\s+value',
                    r'(?i)constraint\s+violation',
                    r'(?i)deadlock\s+detected',
                    r'(?i)lock\s+timeout',
                    r'(?i)transaction\s+failed',
                    r'(?i)rollback\s+failed',
                    r'(?i)connection\s+pool\s+exhausted'
                ],
                'severity': 'high',
                'category': 'Database',
                'priority': 50
            },

            # Performance and Resource Issues
            'performance_issues': {
                'patterns': [
                    r'(?i)performance\s+degradation',
                    r'(?i)slow\s+query',
                    r'(?i)timeout\s+exceeded',
                    r'(?i)resource\s+exhausted',
                    r'(?i)thread\s+pool\s+exhausted',
                    r'(?i)cpu\s+usage\s+high',
                    r'(?i)gc\s+pressure',
                    r'(?i)long\s+running\s+task',
                    r'(?i)bottleneck\s+detected',
                    r'(?i)queue\s+full'
                ],
                'severity': 'medium',
                'category': 'Performance',
                'priority': 45
            },

            # Generic Critical Keywords
            'critical_keywords': {
                'patterns': [
                    r'(?i)\bfailed\b.*\berror\b',
                    r'(?i)\berror\b.*\bfailed\b',
                    r'(?i)\bfatal\b.*\b(?:error|exception|failure)\b',
                    r'(?i)\bcritical\b.*\b(?:error|exception|failure)\b',
                    r'(?i)\bsevere\b.*\b(?:error|exception|failure)\b',
                    r'(?i)\baborted\b.*\b(?:error|exception|failure)\b',
                    r'(?i)\bpanic\b.*\b(?:error|exception|failure)\b',
                    r'(?i)\bcrash\b.*\b(?:error|exception|failure)\b'
                ],
                'severity': 'medium',
                'category': 'Critical Keywords',
                'priority': 40
            }
        }

        self.solution_templates = {
            'Memory Management': [
                "Increase driver memory: --driver-memory 8g or spark.driver.memory=8g",
                "Increase executor memory: --executor-memory 4g or spark.executor.memory=4g",
                "Optimize partitioning to reduce data skew: df.repartition(200) or df.coalesce(100)",
                "Review broadcast join thresholds: spark.sql.adaptive.autoBroadcastJoinThreshold",
                "Use efficient caching strategies: df.cache().count() before reuse",
                "Consider spill-to-disk settings: spark.sql.adaptive.coalescePartitions.enabled=true",
                "Monitor GC settings: -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
            ],
            'SQL Analysis': [
                "Verify table/view existence: SHOW TABLES LIKE 'table_name'",
                "Check column names and case sensitivity in queries",
                "Ensure proper schema qualification: database.table.column",
                "Validate data types and schema compatibility",
                "Check table permissions and access rights",
                "Use DESCRIBE EXTENDED table_name for schema details",
                "Consider using fully qualified paths for external tables"
            ],
            'Spark Execution': [
                "Check cluster resource allocation and availability",
                "Review stage failure patterns for data skew issues",
                "Optimize shuffle operations: spark.sql.adaptive.enabled=true",
                "Consider dynamic allocation: spark.dynamicAllocation.enabled=true",
                "Monitor executor failures and restart cluster if needed",
                "Check for network issues between driver and executors",
                "Review task retry and backoff configurations"
            ],
            'Application Runtime': [
                "Review application logs for stack traces and root cause",
                "Check for null pointer exceptions and handle edge cases",
                "Validate input data quality and schema consistency",
                "Implement proper error handling and try-catch blocks",
                "Review code for infinite loops or recursive calls",
                "Monitor resource usage and optimize algorithms",
                "Use defensive programming practices for data validation"
            ],
            'File I/O': [
                "Verify file paths and S3 bucket permissions",
                "Check AWS credentials and IAM policies",
                "Ensure files exist at specified locations: aws s3 ls s3://bucket/path/",
                "Review file format compatibility (parquet, delta, json, csv)",
                "Check for special characters or encoding issues in file paths",
                "Monitor disk space on cluster nodes",
                "Consider using retry mechanisms for transient I/O failures"
            ],
            'Network': [
                "Test network connectivity: ping, telnet, curl commands",
                "Check security groups and firewall rules",
                "Verify DNS resolution for external services",
                "Increase timeout values: spark.network.timeout=800s",
                "Review VPC and subnet configurations",
                "Check for proxy or NAT gateway issues",
                "Monitor network latency and bandwidth utilization"
            ],
            'Serialization': [
                "Ensure all objects in closures are serializable",
                "Use broadcast variables for large read-only data: spark.broadcast(data)",
                "Avoid capturing non-serializable objects in UDFs",
                "Consider using case classes instead of regular classes",
                "Review custom serialization implementations",
                "Check for circular references in object graphs",
                "Use @transient annotation for non-serializable fields"
            ],
            'Configuration': [
                "Review Spark configuration settings in spark-defaults.conf",
                "Check environment variables and system properties",
                "Validate classpath and library dependencies",
                "Ensure compatible versions of Spark and libraries",
                "Review cluster initialization scripts",
                "Check for configuration conflicts between different sources",
                "Use spark.conf.get() to verify runtime configuration values"
            ],
            'Authentication & Security': [
                "Verify AWS credentials and refresh if expired",
                "Check IAM roles and policies for required permissions",
                "Review Kerberos configuration and ticket renewal",
                "Validate SSL certificates and trust stores",
                "Ensure proper token-based authentication setup",
                "Check for expired or revoked access tokens",
                "Review security group and network ACL configurations"
            ],
            'Database': [
                "Check database connectivity and credentials",
                "Review SQL query syntax and compatibility",
                "Monitor database connection pool settings",
                "Check for table locks and transaction conflicts",
                "Validate database schema and permissions",
                "Review indexing strategy for query performance",
                "Consider connection timeout and retry settings"
            ],
            'Performance': [
                "Monitor cluster resource utilization (CPU, memory, I/O)",
                "Enable Spark UI and analyze job execution metrics",
                "Use adaptive query execution: spark.sql.adaptive.enabled=true",
                "Optimize data formats: use Parquet or Delta Lake",
                "Review partitioning strategy for large datasets",
                "Consider data locality and shuffle optimization",
                "Implement query caching for repeated operations"
            ],
            'Critical Keywords': [
                "Review detailed error messages and stack traces",
                "Check system logs for additional context",
                "Identify patterns in error occurrence timing",
                "Implement comprehensive logging and monitoring",
                "Review recent changes that might have caused issues",
                "Consider rollback to last known good configuration",
                "Escalate to appropriate support channels if needed"
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

    async def analyze_logs_iteratively(self, log_content: Dict[str, str],
                                     search_query: Optional[str] = None,
                                     max_iterations: int = 3) -> Dict[str, any]:
        """
        Iteratively analyze logs to find errors in minimal iterations.
        Starts with highest priority patterns and progressively searches deeper.
        """
        all_errors = []
        analyzed_chunks = []
        iteration_results = []
        token_budget = self.max_token_limit

        # Sort error patterns by priority
        sorted_patterns = sorted(
            self.error_patterns.items(),
            key=lambda x: x[1].get('priority', 50),
            reverse=True
        )

        for iteration in range(max_iterations):
            if iteration == 0:
                # First iteration: Focus on critical errors only
                target_patterns = {k: v for k, v in sorted_patterns[:3]}  # Top 3 priority patterns
                chunk_limit = 5
            elif iteration == 1:
                # Second iteration: Include high-severity errors
                target_patterns = {k: v for k, v in sorted_patterns[:6]}  # Top 6 patterns
                chunk_limit = 10
            else:
                # Final iteration: Comprehensive search
                target_patterns = dict(sorted_patterns)
                chunk_limit = 20

            # Analyze with current pattern set
            iteration_result = await self._analyze_iteration(
                log_content, search_query, target_patterns, chunk_limit, token_budget
            )

            iteration_results.append({
                'iteration': iteration + 1,
                'patterns_searched': list(target_patterns.keys()),
                'errors_found': len(iteration_result['errors']),
                'chunks_analyzed': len(iteration_result['chunks']),
                'token_usage': iteration_result['token_usage']
            })

            # Accumulate results
            all_errors.extend(iteration_result['errors'])
            analyzed_chunks.extend(iteration_result['chunks'])
            token_budget -= iteration_result['token_usage']

            # Early termination if critical errors found
            critical_errors = [e for e in iteration_result['errors']
                             if e.severity == 'critical']
            if critical_errors and iteration == 0:
                break

            # Stop if token budget exhausted
            if token_budget < 1000:
                break

        # Deduplicate and sort errors
        unique_errors = self._deduplicate_errors(all_errors)
        sorted_errors = sorted(unique_errors,
                             key=lambda x: (x.severity, x.frequency),
                             reverse=True)

        # Create final summary
        final_summary = await self._create_iterative_summary(
            analyzed_chunks, sorted_errors, iteration_results
        )

        return {
            'error_summary': sorted_errors,
            'prioritized_chunks': analyzed_chunks[:15],
            'optimized_content': final_summary,
            'token_estimate': self._estimate_tokens(final_summary),
            'iteration_breakdown': iteration_results,
            'total_iterations': len(iteration_results)
        }

    async def _analyze_iteration(self, log_content: Dict[str, str],
                               search_query: Optional[str],
                               target_patterns: Dict[str, Dict],
                               chunk_limit: int,
                               token_budget: int) -> Dict[str, any]:
        """
        Analyze logs for a specific iteration with given patterns.
        """
        # Temporarily override patterns for this iteration
        original_patterns = self.error_patterns
        self.error_patterns = target_patterns

        try:
            chunked_logs = await self._chunk_and_prioritize_logs(log_content, search_query)
            limited_chunks = chunked_logs[:chunk_limit]

            error_summary = await self._analyze_errors(limited_chunks)

            # Estimate token usage for this iteration
            content_sample = '\n'.join([chunk.content[:500] for chunk in limited_chunks[:5]])
            token_usage = self._estimate_tokens(content_sample)

            return {
                'errors': error_summary,
                'chunks': limited_chunks,
                'token_usage': min(token_usage, token_budget)
            }
        finally:
            # Restore original patterns
            self.error_patterns = original_patterns

    def _deduplicate_errors(self, errors: List[ErrorSummary]) -> List[ErrorSummary]:
        """
        Remove duplicate errors based on error type and description.
        """
        seen = set()
        unique_errors = []

        for error in errors:
            key = (error.error_type, error.description)
            if key not in seen:
                seen.add(key)
                unique_errors.append(error)
            else:
                # Merge frequency counts for duplicates
                for existing in unique_errors:
                    if (existing.error_type, existing.description) == key:
                        existing.frequency += error.frequency
                        break

        return unique_errors

    async def _create_iterative_summary(self, chunks: List[LogChunk],
                                       errors: List[ErrorSummary],
                                       iteration_results: List[Dict]) -> str:
        """
        Create an optimized summary from iterative analysis.
        """
        summary_parts = []

        summary_parts.append("=== ITERATIVE LOG ANALYSIS SUMMARY ===")
        summary_parts.append(f"Analysis completed in {len(iteration_results)} iterations\n")

        # Iteration breakdown
        summary_parts.append("ITERATION BREAKDOWN:")
        for result in iteration_results:
            summary_parts.append(
                f"Iteration {result['iteration']}: "
                f"{result['errors_found']} errors found, "
                f"{result['chunks_analyzed']} chunks analyzed, "
                f"{result['token_usage']} tokens used"
            )
        summary_parts.append("")

        # Critical errors first
        critical_errors = [e for e in errors if e.severity == 'critical']
        if critical_errors:
            summary_parts.append("🚨 CRITICAL ERRORS DETECTED:")
            for error in critical_errors:
                summary_parts.append(
                    f"• {error.error_type.upper()}: {error.frequency} occurrences"
                )
                summary_parts.append(f"  Solution: {error.suggested_solution[:150]}...")
            summary_parts.append("")

        # High priority errors
        high_errors = [e for e in errors if e.severity == 'high'][:3]
        if high_errors:
            summary_parts.append("⚠️  HIGH PRIORITY ERRORS:")
            for error in high_errors:
                summary_parts.append(
                    f"• {error.error_type.upper()}: {error.frequency} occurrences"
                )
                summary_parts.append(f"  Solution: {error.suggested_solution[:150]}...")
            summary_parts.append("")

        # Most relevant log excerpts
        summary_parts.append("📋 MOST RELEVANT LOG EXCERPTS:")
        top_chunks = sorted(chunks, key=lambda x: x.priority_score, reverse=True)[:3]

        for i, chunk in enumerate(top_chunks):
            summary_parts.append(f"\n--- Excerpt {i+1} from {chunk.file_name} ---")
            summary_parts.append(f"Priority: {chunk.priority_score:.2f} | Errors: {chunk.error_indicators}")
            summary_parts.append(chunk.content[:800])
            if len(chunk.content) > 800:
                summary_parts.append("... (truncated)")

        full_summary = '\n'.join(summary_parts)

        # Truncate if needed
        if self._estimate_tokens(full_summary) > self.max_token_limit:
            return self._truncate_to_token_limit(full_summary)

        return full_summary

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
                    severity_weight = {'critical': 5.0, 'high': 3.0, 'medium': 2.0, 'low': 1.0}.get(
                        config['severity'], 0.5
                    )
                    priority_weight = config.get('priority', 50) / 100.0
                    score += matches * severity_weight * priority_weight

        if search_query:
            query_matches = len(re.findall(re.escape(search_query), content, re.IGNORECASE))
            score += query_matches * 2.0

        error_keywords = [
            (r'\bfatal\b', 2.0),
            (r'\bcritical\b', 2.0),
            (r'\bsevere\b', 1.8),
            (r'\berror\b', 1.5),
            (r'\bexception\b', 1.5),
            (r'\bfailed\b', 1.2),
            (r'\bfailure\b', 1.2),
            (r'\bwarning\b', 0.8),
            (r'\bwarn\b', 0.8)
        ]

        for keyword_pattern, weight in error_keywords:
            matches = len(re.findall(keyword_pattern, content, re.IGNORECASE))
            score += matches * weight

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

        # Sort by severity priority and frequency
        severity_priority = {'critical': 4, 'high': 3, 'medium': 2, 'low': 1}
        return sorted(summaries,
                     key=lambda x: (severity_priority.get(x.severity, 0), x.frequency),
                     reverse=True)

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