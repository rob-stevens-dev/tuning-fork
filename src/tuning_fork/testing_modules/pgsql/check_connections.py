"""
PostgreSQL connections checker module.

This module provides functionality to analyze active connections, identify
blocking queries, detect long-running transactions, and assess connection
pool health for PostgreSQL databases.

The module examines pg_stat_activity, pg_locks, and related system views to
provide actionable insights into connection management and query blocking issues.
"""

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Optional

from tuning_fork.config import Config
from tuning_fork.shared.decorators.command_logger import with_command_logging
from tuning_fork.shared.decorators.db_connection import with_database
from tuning_fork.shared.decorators.execution_timer import log_execution_time
from tuning_fork.shared.decorators.db_connection import get_db_connection

# Module-level logger
logger = logging.getLogger(__name__)


@dataclass
class ConnectionCheckResult:
    """
    Result from a connection analysis operation.
    
    Attributes:
        check_type: Type of check performed (e.g., 'blocking_query', 'idle_transaction').
        pid: Process ID of the connection.
        database: Database name.
        username: Username of the connection.
        application_name: Application name from the connection.
        state: Connection state (active, idle, idle in transaction, etc.).
        query: Current or last query text.
        duration_seconds: Duration of current state in seconds.
        wait_event_type: Type of wait event (if waiting).
        wait_event: Specific wait event name.
        blocked_by: List of PIDs blocking this connection (if blocked).
        blocking: List of PIDs this connection is blocking (if blocking others).
        status: Status indicator ('OK', 'WARNING', 'CRITICAL').
        message: Human-readable message about the finding.
        recommendation: Recommended action to address the issue.
    """
    check_type: str
    pid: int
    database: str
    username: str
    application_name: Optional[str]
    state: str
    query: str
    duration_seconds: float
    wait_event_type: Optional[str]
    wait_event: Optional[str]
    blocked_by: list[int]
    blocking: list[int]
    status: str
    message: str
    recommendation: str
    
    def to_dict(self) -> dict[str, Any]:
        """
        Convert result to dictionary.
        
        Returns:
            Dictionary representation of the check result.
        """
        return asdict(self)
    
    def to_json(self) -> str:
        """
        Convert result to JSON string.
        
        Returns:
            JSON string representation of the check result.
        """
        return json.dumps(self.to_dict(), default=str)


@dataclass
class ConnectionAction:
    """
    Record of a connection management action.
    
    Attributes:
        action_type: Type of action ('terminate', 'cancel', 'adjust_limit').
        pid: Process ID affected (if applicable).
        setting_name: Setting name affected (if applicable).
        old_value: Value before action.
        new_value: Value after action.
        timestamp: When the action was performed.
        action_id: Unique identifier for this action.
        applied_by: User or process that performed the action.
        rollback_status: Whether this action has been rolled back.
        success: Whether the action succeeded.
    """
    action_type: str
    pid: Optional[int]
    setting_name: Optional[str]
    old_value: Any
    new_value: Any
    timestamp: str
    action_id: int
    applied_by: str = "tuning_fork"
    rollback_status: bool = False
    success: bool = True
    
    def to_dict(self) -> dict[str, Any]:
        """Convert action to dictionary."""
        return asdict(self)


# Thresholds for connection analysis
CONNECTION_THRESHOLDS = {
    'max_connections_usage_warning': 0.80,  # 80% of max_connections
    'max_connections_usage_critical': 0.95,  # 95% of max_connections
    'idle_in_transaction_warning': 300,  # 5 minutes
    'idle_in_transaction_critical': 1800,  # 30 minutes
    'long_running_query_warning': 600,  # 10 minutes
    'long_running_query_critical': 3600,  # 1 hour
    'blocking_query_warning': 30,  # 30 seconds
    'blocking_query_critical': 300,  # 5 minutes
}


@log_execution_time
@with_command_logging
@with_database
def check_connections(
    config: Config,
    thresholds: Optional[dict[str, Any]] = None
) -> list[ConnectionCheckResult]:
    """
    Check PostgreSQL connections for issues and potential problems.
    
    Analyzes active connections, blocking queries, idle transactions,
    long-running queries, and connection pool utilization.
    
    Args:
        config: Configuration object with database connection details.
        thresholds: Optional custom thresholds for analysis.
    
    Returns:
        List of ConnectionCheckResult objects with findings.
    
    Raises:
        RuntimeError: If connection analysis fails.
    
    Example:
        >>> config = Config('config.yaml')
        >>> results = check_connections(config)
        >>> for result in results:
        ...     print(f"{result.status}: {result.message}")
    """
    results: list[ConnectionCheckResult] = []
    check_thresholds = {**CONNECTION_THRESHOLDS, **(thresholds or {})}
    
    logger.info("Starting connection analysis")
    
    try:
        from tuning_fork.shared.database import get_db_connection
        
        with get_db_connection(config) as (connection, cursor):
            # Check connection pool utilization
            pool_results = _check_connection_pool(cursor, check_thresholds)
            results.extend(pool_results)
            
            # Check for blocking queries
            blocking_results = _check_blocking_queries(cursor, check_thresholds)
            results.extend(blocking_results)
            
            # Check for idle in transaction
            idle_results = _check_idle_transactions(cursor, check_thresholds)
            results.extend(idle_results)
            
            # Check for long-running queries
            long_running_results = _check_long_running_queries(cursor, check_thresholds)
            results.extend(long_running_results)
            
            # Check wait events
            wait_results = _check_wait_events(cursor)
            results.extend(wait_results)
            
            logger.info(f"Connection analysis complete: {len(results)} findings")
            
    except Exception as exc:
        logger.error(f"Failed to analyze connections: {exc}")
        raise RuntimeError(f"Connection analysis failed: {exc}") from exc
    
    return results


def _check_connection_pool(
    cursor: Any,
    thresholds: dict[str, Any]
) -> list[ConnectionCheckResult]:
    """
    Check connection pool utilization against max_connections.
    
    Args:
        cursor: Database cursor.
        thresholds: Threshold configuration.
    
    Returns:
        List of results for connection pool checks.
    """
    results: list[ConnectionCheckResult] = []
    
    # Get max_connections setting
    cursor.execute("SHOW max_connections")
    max_connections = int(cursor.fetchone()[0])
    
    # Count active connections (excluding background processes)
    cursor.execute("""
        SELECT COUNT(*)
        FROM pg_stat_activity
        WHERE backend_type = 'client backend'
    """)
    active_connections = cursor.fetchone()[0]
    
    usage_ratio = active_connections / max_connections
    
    if usage_ratio >= thresholds['max_connections_usage_critical']:
        status = 'CRITICAL'
        message = (
            f"Connection pool at {usage_ratio:.1%} capacity "
            f"({active_connections}/{max_connections})"
        )
        recommendation = (
            "Increase max_connections or implement connection pooling. "
            "Review applications for connection leaks."
        )
    elif usage_ratio >= thresholds['max_connections_usage_warning']:
        status = 'WARNING'
        message = (
            f"Connection pool usage high: {usage_ratio:.1%} "
            f"({active_connections}/{max_connections})"
        )
        recommendation = "Monitor connection usage and consider increasing max_connections."
    else:
        status = 'OK'
        message = (
            f"Connection pool healthy: {usage_ratio:.1%} "
            f"({active_connections}/{max_connections})"
        )
        recommendation = "No action required."
    
    results.append(ConnectionCheckResult(
        check_type='connection_pool',
        pid=-1,
        database='ALL',
        username='ALL',
        application_name=None,
        state='N/A',
        query='N/A',
        duration_seconds=0.0,
        wait_event_type=None,
        wait_event=None,
        blocked_by=[],
        blocking=[],
        status=status,
        message=message,
        recommendation=recommendation
    ))
    
    return results


def _check_blocking_queries(
    cursor: Any,
    thresholds: dict[str, Any]
) -> list[ConnectionCheckResult]:
    """
    Identify blocking queries using pg_locks and pg_stat_activity.
    
    Args:
        cursor: Database cursor.
        thresholds: Threshold configuration.
    
    Returns:
        List of results for blocking query checks.
    """
    results: list[ConnectionCheckResult] = []
    
    # Query to find blocking relationships
    query = """
        SELECT
            blocked_activity.pid AS blocked_pid,
            blocked_activity.usename AS blocked_user,
            blocked_activity.datname AS blocked_db,
            blocked_activity.application_name AS blocked_app,
            blocked_activity.state AS blocked_state,
            blocked_activity.query AS blocked_query,
            EXTRACT(EPOCH FROM (NOW() - blocked_activity.state_change)) AS blocked_duration,
            blocked_activity.wait_event_type,
            blocked_activity.wait_event,
            blocking_activity.pid AS blocking_pid,
            blocking_activity.usename AS blocking_user,
            blocking_activity.query AS blocking_query,
            EXTRACT(EPOCH FROM (NOW() - blocking_activity.state_change)) AS blocking_duration
        FROM pg_catalog.pg_locks blocked_locks
        JOIN pg_catalog.pg_stat_activity blocked_activity 
            ON blocked_activity.pid = blocked_locks.pid
        JOIN pg_catalog.pg_locks blocking_locks
            ON blocking_locks.locktype = blocked_locks.locktype
            AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
            AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
            AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
            AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
            AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
            AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
            AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
            AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
            AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
            AND blocking_locks.pid != blocked_locks.pid
        JOIN pg_catalog.pg_stat_activity blocking_activity 
            ON blocking_activity.pid = blocking_locks.pid
        WHERE NOT blocked_locks.granted
        ORDER BY blocked_duration DESC
    """
    
    cursor.execute(query)
    blocking_rows = cursor.fetchall()
    
    for row in blocking_rows:
        (blocked_pid, blocked_user, blocked_db, blocked_app, blocked_state,
         blocked_query, blocked_duration, wait_event_type, wait_event,
         blocking_pid, blocking_user, blocking_query, blocking_duration) = row
        
        if blocked_duration >= thresholds['blocking_query_critical']:
            status = 'CRITICAL'
        elif blocked_duration >= thresholds['blocking_query_warning']:
            status = 'WARNING'
        else:
            status = 'OK'
        
        message = (
            f"PID {blocked_pid} blocked by PID {blocking_pid} "
            f"for {blocked_duration:.1f}s"
        )
        
        recommendation = (
            f"Review blocking query (PID {blocking_pid}). "
            f"Consider terminating blocking session if appropriate."
        )
        
        results.append(ConnectionCheckResult(
            check_type='blocking_query',
            pid=blocked_pid,
            database=blocked_db,
            username=blocked_user,
            application_name=blocked_app,
            state=blocked_state,
            query=blocked_query[:500],  # Truncate for readability
            duration_seconds=blocked_duration,
            wait_event_type=wait_event_type,
            wait_event=wait_event,
            blocked_by=[blocking_pid],
            blocking=[],
            status=status,
            message=message,
            recommendation=recommendation
        ))
    
    return results


def _check_idle_transactions(
    cursor: Any,
    thresholds: dict[str, Any]
) -> list[ConnectionCheckResult]:
    """
    Check for idle in transaction connections.
    
    Args:
        cursor: Database cursor.
        thresholds: Threshold configuration.
    
    Returns:
        List of results for idle transaction checks.
    """
    results: list[ConnectionCheckResult] = []
    
    query = """
        SELECT
            pid,
            usename,
            datname,
            application_name,
            state,
            query,
            EXTRACT(EPOCH FROM (NOW() - state_change)) AS duration,
            wait_event_type,
            wait_event
        FROM pg_stat_activity
        WHERE state IN ('idle in transaction', 'idle in transaction (aborted)')
            AND backend_type = 'client backend'
        ORDER BY duration DESC
    """
    
    cursor.execute(query)
    idle_rows = cursor.fetchall()
    
    for row in idle_rows:
        (pid, username, database, app_name, state, query,
         duration, wait_event_type, wait_event) = row
        
        if duration >= thresholds['idle_in_transaction_critical']:
            status = 'CRITICAL'
        elif duration >= thresholds['idle_in_transaction_warning']:
            status = 'WARNING'
        else:
            status = 'OK'
        
        message = f"Connection idle in transaction for {duration:.1f}s"
        recommendation = (
            "Investigate application connection handling. "
            "Consider setting idle_in_transaction_session_timeout."
        )
        
        results.append(ConnectionCheckResult(
            check_type='idle_in_transaction',
            pid=pid,
            database=database,
            username=username,
            application_name=app_name,
            state=state,
            query=query[:500],
            duration_seconds=duration,
            wait_event_type=wait_event_type,
            wait_event=wait_event,
            blocked_by=[],
            blocking=[],
            status=status,
            message=message,
            recommendation=recommendation
        ))
    
    return results


def _check_long_running_queries(
    cursor: Any,
    thresholds: dict[str, Any]
) -> list[ConnectionCheckResult]:
    """
    Check for long-running queries.
    
    Args:
        cursor: Database cursor.
        thresholds: Threshold configuration.
    
    Returns:
        List of results for long-running query checks.
    """
    results: list[ConnectionCheckResult] = []
    
    query = """
        SELECT
            pid,
            usename,
            datname,
            application_name,
            state,
            query,
            EXTRACT(EPOCH FROM (NOW() - query_start)) AS duration,
            wait_event_type,
            wait_event
        FROM pg_stat_activity
        WHERE state = 'active'
            AND backend_type = 'client backend'
            AND query NOT ILIKE '%pg_stat_activity%'
        ORDER BY duration DESC
    """
    
    cursor.execute(query)
    active_rows = cursor.fetchall()
    
    for row in active_rows:
        (pid, username, database, app_name, state, query,
         duration, wait_event_type, wait_event) = row
        
        if duration >= thresholds['long_running_query_critical']:
            status = 'CRITICAL'
        elif duration >= thresholds['long_running_query_warning']:
            status = 'WARNING'
        else:
            status = 'OK'
        
        message = f"Query running for {duration:.1f}s"
        recommendation = (
            "Review query performance. Consider adding EXPLAIN ANALYZE. "
            "Check for missing indexes or inefficient query patterns."
        )
        
        results.append(ConnectionCheckResult(
            check_type='long_running_query',
            pid=pid,
            database=database,
            username=username,
            application_name=app_name,
            state=state,
            query=query[:500],
            duration_seconds=duration,
            wait_event_type=wait_event_type,
            wait_event=wait_event,
            blocked_by=[],
            blocking=[],
            status=status,
            message=message,
            recommendation=recommendation
        ))
    
    return results


def _check_wait_events(cursor: Any) -> list[ConnectionCheckResult]:
    """
    Analyze wait events to identify resource contention.
    
    Args:
        cursor: Database cursor.
    
    Returns:
        List of results for wait event analysis.
    """
    results: list[ConnectionCheckResult] = []
    
    query = """
        SELECT
            wait_event_type,
            wait_event,
            COUNT(*) AS count,
            ARRAY_AGG(pid) AS pids
        FROM pg_stat_activity
        WHERE wait_event_type IS NOT NULL
            AND wait_event IS NOT NULL
            AND backend_type = 'client backend'
        GROUP BY wait_event_type, wait_event
        HAVING COUNT(*) >= 3
        ORDER BY count DESC
    """
    
    cursor.execute(query)
    wait_rows = cursor.fetchall()
    
    for row in wait_rows:
        wait_event_type, wait_event, count, pids = row
        
        if count >= 10:
            status = 'CRITICAL'
        elif count >= 5:
            status = 'WARNING'
        else:
            status = 'OK'
        
        message = (
            f"{count} connections waiting on {wait_event_type}:{wait_event}"
        )
        recommendation = _get_wait_event_recommendation(wait_event_type, wait_event)
        
        results.append(ConnectionCheckResult(
            check_type='wait_event',
            pid=-1,
            database='MULTIPLE',
            username='MULTIPLE',
            application_name=None,
            state='active',
            query=f"Waiting on {wait_event_type}:{wait_event}",
            duration_seconds=0.0,
            wait_event_type=wait_event_type,
            wait_event=wait_event,
            blocked_by=[],
            blocking=[],
            status=status,
            message=message,
            recommendation=recommendation
        ))
    
    return results


def _get_wait_event_recommendation(
    wait_event_type: str,
    wait_event: str
) -> str:
    """
    Get recommendation based on wait event type.
    
    Args:
        wait_event_type: Type of wait event.
        wait_event: Specific wait event name.
    
    Returns:
        Recommendation string.
    """
    recommendations = {
        'Lock': (
            "High lock contention detected. Review query patterns and consider "
            "optimizing transaction scope or adding FOR UPDATE SKIP LOCKED."
        ),
        'LWLock': (
            "Lightweight lock contention. May indicate buffer pool or WAL issues. "
            "Consider increasing shared_buffers or wal_buffers."
        ),
        'IO': (
            "I/O wait detected. Review storage performance, consider adding "
            "indexes, or increasing effective_io_concurrency."
        ),
        'Client': (
            "Waiting on client. Application may be processing slowly. "
            "Review application-side performance."
        ),
        'IPC': (
            "Inter-process communication wait. May indicate parallel query issues. "
            "Review max_parallel_workers settings."
        ),
    }
    
    return recommendations.get(
        wait_event_type,
        f"Review {wait_event_type} wait events in PostgreSQL documentation."
    )


def report_connections(
    results: list[ConnectionCheckResult],
    format: str = 'text',
    include_ok: bool = False
) -> str:
    """
    Generate a formatted report of connection check results.
    
    Args:
        results: List of ConnectionCheckResult objects.
        format: Output format ('text', 'json', 'html').
        include_ok: Whether to include OK status results.
    
    Returns:
        Formatted report string.
    
    Raises:
        ValueError: If format is not supported.
    
    Example:
        >>> results = check_connections(config)
        >>> report = report_connections(results, format='text')
        >>> print(report)
    """
    if format == 'json':
        return _report_connections_json(results, include_ok)
    elif format == 'html':
        return _report_connections_html(results, include_ok)
    elif format == 'text':
        return _report_connections_text(results, include_ok)
    else:
        raise ValueError(f"Unsupported format: {format}")


def _report_connections_text(
    results: list[ConnectionCheckResult],
    include_ok: bool
) -> str:
    """Generate text format report."""
    filtered_results = (
        results if include_ok
        else [r for r in results if r.status != 'OK']
    )
    
    if not filtered_results:
        return "No connection issues found. All checks passed."
    
    report_lines = [
        "=" * 80,
        "PostgreSQL Connection Analysis Report",
        "=" * 80,
        f"Generated: {datetime.now().isoformat()}",
        f"Total Findings: {len(filtered_results)}",
        ""
    ]
    
    # Group by check type
    by_type: dict[str, list[ConnectionCheckResult]] = {}
    for result in filtered_results:
        by_type.setdefault(result.check_type, []).append(result)
    
    for check_type, type_results in sorted(by_type.items()):
        report_lines.append(f"\n{check_type.upper().replace('_', ' ')}")
        report_lines.append("-" * 80)
        
        for result in type_results:
            report_lines.append(f"\n[{result.status}] {result.message}")
            
            if result.pid > 0:
                report_lines.append(f"  PID: {result.pid}")
                report_lines.append(f"  Database: {result.database}")
                report_lines.append(f"  User: {result.username}")
                
                if result.application_name:
                    report_lines.append(f"  Application: {result.application_name}")
                
                report_lines.append(f"  State: {result.state}")
                report_lines.append(f"  Duration: {result.duration_seconds:.1f}s")
                
                if result.wait_event:
                    report_lines.append(
                        f"  Wait: {result.wait_event_type}:{result.wait_event}"
                    )
                
                if result.blocked_by:
                    report_lines.append(f"  Blocked by: {result.blocked_by}")
                
                if result.blocking:
                    report_lines.append(f"  Blocking: {result.blocking}")
                
                if result.query and result.query != 'N/A':
                    report_lines.append(f"  Query: {result.query[:200]}...")
            
            report_lines.append(f"  Recommendation: {result.recommendation}")
    
    report_lines.append("\n" + "=" * 80)
    
    return "\n".join(report_lines)


def _report_connections_json(
    results: list[ConnectionCheckResult],
    include_ok: bool
) -> str:
    """Generate JSON format report."""
    filtered_results = (
        results if include_ok
        else [r for r in results if r.status != 'OK']
    )
    
    return json.dumps(
        {
            'generated': datetime.now().isoformat(),
            'total_findings': len(filtered_results),
            'results': [r.to_dict() for r in filtered_results]
        },
        indent=2,
        default=str
    )


def _report_connections_html(
    results: list[ConnectionCheckResult],
    include_ok: bool
) -> str:
    """Generate HTML format report."""
    filtered_results = (
        results if include_ok
        else [r for r in results if r.status != 'OK']
    )
    
    status_colors = {
        'OK': '#28a745',
        'WARNING': '#ffc107',
        'CRITICAL': '#dc3545'
    }
    
    html_parts = [
        '<!DOCTYPE html>',
        '<html>',
        '<head>',
        '<title>PostgreSQL Connection Analysis</title>',
        '<style>',
        'body { font-family: Arial, sans-serif; margin: 20px; }',
        'table { border-collapse: collapse; width: 100%; margin: 20px 0; }',
        'th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }',
        'th { background-color: #4CAF50; color: white; }',
        '.status { padding: 4px 8px; border-radius: 4px; color: white; }',
        '.query { font-family: monospace; font-size: 0.9em; }',
        '</style>',
        '</head>',
        '<body>',
        '<h1>PostgreSQL Connection Analysis Report</h1>',
        f'<p>Generated: {datetime.now().isoformat()}</p>',
        f'<p>Total Findings: {len(filtered_results)}</p>',
        '<table>',
        '<tr>',
        '<th>Type</th>',
        '<th>Status</th>',
        '<th>PID</th>',
        '<th>Database</th>',
        '<th>Duration</th>',
        '<th>Message</th>',
        '<th>Recommendation</th>',
        '</tr>'
    ]
    
    for result in filtered_results:
        color = status_colors.get(result.status, '#6c757d')
        html_parts.extend([
            '<tr>',
            f'<td>{result.check_type}</td>',
            f'<td><span class="status" style="background-color: {color}">'
            f'{result.status}</span></td>',
            f'<td>{result.pid if result.pid > 0 else "N/A"}</td>',
            f'<td>{result.database}</td>',
            f'<td>{result.duration_seconds:.1f}s</td>',
            f'<td>{result.message}</td>',
            f'<td>{result.recommendation}</td>',
            '</tr>'
        ])
    
    html_parts.extend([
        '</table>',
        '</body>',
        '</html>'
    ])
    
    return '\n'.join(html_parts)


@log_execution_time
@with_command_logging
@with_database
def apply_actions(
    config: Config,
    actions: dict[str, Any]
) -> list[ConnectionAction]:
    """
    Apply connection management actions.
    
    Actions can include terminating connections, canceling queries,
    or adjusting connection limits.
    
    Args:
        config: Configuration object with database connection details.
        actions: Dictionary of actions to apply.
            Format: {
                'terminate_pids': [pid1, pid2, ...],
                'cancel_pids': [pid3, pid4, ...],
                'adjust_settings': {'max_connections': 200, ...}
            }
    
    Returns:
        List of ConnectionAction objects recording what was done.
    
    Raises:
        ValueError: If actions contain invalid PIDs or settings.
        RuntimeError: If action application fails.
    
    Example:
        >>> actions = {'terminate_pids': [12345], 'cancel_pids': [12346]}
        >>> changes = apply_actions(config, actions)
    """
    action_log: list[ConnectionAction] = []
    action_id = int(datetime.now().timestamp())
    
    logger.info(f"Applying connection actions: {actions}")
    
    try:
        from tuning_fork.shared.database import get_db_connection
        
        with get_db_connection(config) as (connection, cursor):
            # Terminate connections
            if 'terminate_pids' in actions:
                for pid in actions['terminate_pids']:
                    try:
                        cursor.execute(
                            "SELECT pg_terminate_backend(%s)",
                            (pid,)
                        )
                        result = cursor.fetchone()[0]
                        
                        action_log.append(ConnectionAction(
                            action_type='terminate',
                            pid=pid,
                            setting_name=None,
                            old_value='running',
                            new_value='terminated',
                            timestamp=datetime.now().isoformat(),
                            action_id=action_id,
                            success=result
                        ))
                        
                        logger.info(f"Terminated PID {pid}: {result}")
                        action_id += 1
                        
                    except Exception as exc:
                        logger.error(f"Failed to terminate PID {pid}: {exc}")
                        action_log.append(ConnectionAction(
                            action_type='terminate',
                            pid=pid,
                            setting_name=None,
                            old_value='running',
                            new_value='error',
                            timestamp=datetime.now().isoformat(),
                            action_id=action_id,
                            success=False
                        ))
                        action_id += 1
            
            # Cancel queries
            if 'cancel_pids' in actions:
                for pid in actions['cancel_pids']:
                    try:
                        cursor.execute(
                            "SELECT pg_cancel_backend(%s)",
                            (pid,)
                        )
                        result = cursor.fetchone()[0]
                        
                        action_log.append(ConnectionAction(
                            action_type='cancel',
                            pid=pid,
                            setting_name=None,
                            old_value='running',
                            new_value='canceled',
                            timestamp=datetime.now().isoformat(),
                            action_id=action_id,
                            success=result
                        ))
                        
                        logger.info(f"Canceled PID {pid}: {result}")
                        action_id += 1
                        
                    except Exception as exc:
                        logger.error(f"Failed to cancel PID {pid}: {exc}")
                        action_log.append(ConnectionAction(
                            action_type='cancel',
                            pid=pid,
                            setting_name=None,
                            old_value='running',
                            new_value='error',
                            timestamp=datetime.now().isoformat(),
                            action_id=action_id,
                            success=False
                        ))
                        action_id += 1
            
            # Adjust settings (like max_connections)
            if 'adjust_settings' in actions:
                for setting_name, new_value in actions['adjust_settings'].items():
                    try:
                        # Get current value
                        cursor.execute(f"SHOW {setting_name}")
                        old_value = cursor.fetchone()[0]
                        
                        # Apply new value with ALTER SYSTEM
                        cursor.execute(
                            f"ALTER SYSTEM SET {setting_name} = %s",
                            (str(new_value),)
                        )
                        connection.commit()
                        
                        action_log.append(ConnectionAction(
                            action_type='adjust_limit',
                            pid=None,
                            setting_name=setting_name,
                            old_value=old_value,
                            new_value=new_value,
                            timestamp=datetime.now().isoformat(),
                            action_id=action_id,
                            success=True
                        ))
                        
                        logger.info(
                            f"Adjusted {setting_name}: {old_value} -> {new_value}"
                        )
                        logger.warning(
                            f"Setting {setting_name} requires server restart to take effect"
                        )
                        action_id += 1
                        
                    except Exception as exc:
                        logger.error(f"Failed to adjust {setting_name}: {exc}")
                        action_log.append(ConnectionAction(
                            action_type='adjust_limit',
                            pid=None,
                            setting_name=setting_name,
                            old_value=None,
                            new_value=new_value,
                            timestamp=datetime.now().isoformat(),
                            action_id=action_id,
                            success=False
                        ))
                        action_id += 1
            
            logger.info(f"Applied {len(action_log)} connection actions")
            
    except Exception as exc:
        logger.error(f"Failed to apply actions: {exc}")
        raise RuntimeError(f"Action application failed: {exc}") from exc
    
    return action_log


@log_execution_time
@with_command_logging
@with_database
def rollback_actions(
    config: Config,
    actions: list[ConnectionAction]
) -> list[ConnectionAction]:
    """
    Rollback connection management actions.
    
    Note: Connection terminations and query cancellations cannot be rolled back.
    Only setting adjustments can be reverted.
    
    Args:
        config: Configuration object with database connection details.
        actions: List of ConnectionAction objects to rollback.
    
    Returns:
        List of ConnectionAction objects recording rollback operations.
    
    Raises:
        RuntimeError: If rollback fails.
    
    Example:
        >>> changes = apply_actions(config, actions)
        >>> rollback_log = rollback_actions(config, changes)
    """
    rollback_log: list[ConnectionAction] = []
    action_id = int(datetime.now().timestamp())
    
    logger.info(f"Rolling back {len(actions)} actions")
    
    try:
        from tuning_fork.shared.database import get_db_connection
        
        with get_db_connection(config) as (connection, cursor):
            for action in actions:
                if action.rollback_status:
                    logger.info(
                        f"Skipping already rolled back action: "
                        f"{action.action_type} {action.action_id}"
                    )
                    continue
                
                # Only setting adjustments can be rolled back
                if action.action_type == 'adjust_limit' and action.success:
                    try:
                        cursor.execute(
                            f"ALTER SYSTEM SET {action.setting_name} = %s",
                            (str(action.old_value),)
                        )
                        connection.commit()
                        
                        rollback_log.append(ConnectionAction(
                            action_type='rollback_adjust_limit',
                            pid=None,
                            setting_name=action.setting_name,
                            old_value=action.new_value,
                            new_value=action.old_value,
                            timestamp=datetime.now().isoformat(),
                            action_id=action_id,
                            success=True
                        ))
                        
                        logger.info(
                            f"Rolled back {action.setting_name}: "
                            f"{action.new_value} -> {action.old_value}"
                        )
                        action_id += 1
                        
                    except Exception as exc:
                        logger.error(
                            f"Failed to rollback {action.setting_name}: {exc}"
                        )
                        rollback_log.append(ConnectionAction(
                            action_type='rollback_adjust_limit',
                            pid=None,
                            setting_name=action.setting_name,
                            old_value=action.new_value,
                            new_value=action.old_value,
                            timestamp=datetime.now().isoformat(),
                            action_id=action_id,
                            success=False
                        ))
                        action_id += 1
                
                elif action.action_type in ('terminate', 'cancel'):
                    logger.warning(
                        f"Cannot rollback {action.action_type} action for PID {action.pid}"
                    )
            
            logger.info(f"Completed {len(rollback_log)} rollback operations")
            
    except Exception as exc:
        logger.error(f"Failed to rollback actions: {exc}")
        raise RuntimeError(f"Rollback failed: {exc}") from exc
    
    return rollback_log