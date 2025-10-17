"""
PostgreSQL settings checker module.

This module provides functionality to check, report, apply, and rollback
PostgreSQL configuration settings against recommended best practices.

The module supports multiple workload types (OLTP, OLAP, Mixed) and includes
dynamic setting detection, scope management, and persistent configuration changes.
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

# Module-level logger
logger = logging.getLogger(__name__)


@dataclass
class CheckResult:
    """
    Result from a configuration check operation.
    
    Attributes:
        setting_name: Name of the PostgreSQL setting.
        current_value: Current value of the setting.
        recommended_value: Recommended value for the setting.
        status: Status indicator ('OK', 'WARNING', 'CRITICAL').
        message: Human-readable message about the check.
        impact: Description of what this setting affects.
        setting_unit: Unit of measurement for the setting (if applicable).
        workload_type: Workload type this recommendation applies to.
    """
    setting_name: str
    current_value: Any
    recommended_value: Any
    status: str
    message: str
    impact: str
    setting_unit: Optional[str] = None
    workload_type: str = 'OLTP'
    
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
class ChangeLogEntry:
    """
    Entry in the settings change log.
    
    Attributes:
        setting_name: Name of the setting that was changed.
        old_value: Value before the change.
        new_value: Value after the change.
        timestamp: When the change was made.
        change_id: Unique identifier for this change.
        applied_by: User or process that applied the change.
        rollback_status: Whether this change has been rolled back.
        scope: Setting scope ('SYSTEM' for ALTER SYSTEM, 'SESSION' for SET).
    """
    setting_name: str
    old_value: Any
    new_value: Any
    timestamp: str
    change_id: int
    applied_by: str = "tuning_fork"
    rollback_status: bool = False
    scope: str = "SYSTEM"
    
    def to_dict(self) -> dict[str, Any]:
        """Convert change log entry to dictionary."""
        return asdict(self)


# Recommended PostgreSQL settings for OLTP workloads
# Conservative defaults that work well for most OLTP scenarios
RECOMMENDED_SETTINGS_OLTP = {
    'max_connections': {
        'value': 100,
        'unit': None,
        'impact': 'Maximum number of concurrent database connections',
        'critical_threshold': 10,
        'context': 'postmaster',  # Requires restart
        'dynamic': False,
    },
    'shared_buffers': {
        'value': '256MB',
        'unit': 'memory',
        'impact': 'Amount of memory used for shared memory buffers',
        'critical_threshold': '128MB',
        'context': 'postmaster',  # Requires restart
        'dynamic': False,
    },
    'effective_cache_size': {
        'value': '4GB',
        'unit': 'memory',
        'impact': 'Planner\'s assumption about effective cache size',
        'critical_threshold': '1GB',
        'context': 'user',  # Can be changed without restart
        'dynamic': True,
    },
    'maintenance_work_mem': {
        'value': '64MB',
        'unit': 'memory',
        'impact': 'Memory for maintenance operations (VACUUM, CREATE INDEX)',
        'critical_threshold': '16MB',
        'context': 'user',
        'dynamic': True,
    },
    'checkpoint_completion_target': {
        'value': 0.9,
        'unit': None,
        'impact': 'Fraction of checkpoint interval to spread writes',
        'critical_threshold': 0.5,
        'context': 'sighup',  # Reload required
        'dynamic': True,
    },
    'wal_buffers': {
        'value': '16MB',
        'unit': 'memory',
        'impact': 'Amount of memory used for WAL data',
        'critical_threshold': '1MB',
        'context': 'postmaster',
        'dynamic': False,
    },
    'default_statistics_target': {
        'value': 100,
        'unit': None,
        'impact': 'Number of statistics samples for query planning',
        'critical_threshold': 50,
        'context': 'user',
        'dynamic': True,
    },
    'random_page_cost': {
        'value': 1.1,
        'unit': None,
        'impact': 'Planner\'s cost estimate for random page access (use lower for SSD)',
        'critical_threshold': 4.0,
        'context': 'user',
        'dynamic': True,
    },
    'effective_io_concurrency': {
        'value': 200,
        'unit': None,
        'impact': 'Number of concurrent disk I/O operations (for SSD)',
        'critical_threshold': 1,
        'context': 'user',
        'dynamic': True,
    },
    'work_mem': {
        'value': '4MB',
        'unit': 'memory',
        'impact': 'Memory for sorting and hash operations per query',
        'critical_threshold': '1MB',
        'context': 'user',
        'dynamic': True,
    },
    'max_worker_processes': {
        'value': 8,
        'unit': None,
        'impact': 'Maximum number of background worker processes',
        'critical_threshold': 0,
        'context': 'postmaster',
        'dynamic': False,
    },
    'max_parallel_workers_per_gather': {
        'value': 2,
        'unit': None,
        'impact': 'Maximum parallel workers per query execution node',
        'critical_threshold': 0,
        'context': 'user',
        'dynamic': True,
    },
}

# Recommended PostgreSQL settings for OLAP workloads
RECOMMENDED_SETTINGS_OLAP = {
    'max_connections': {
        'value': 50,
        'unit': None,
        'impact': 'Maximum number of concurrent database connections',
        'critical_threshold': 10,
        'context': 'postmaster',
        'dynamic': False,
    },
    'shared_buffers': {
        'value': '8GB',
        'unit': 'memory',
        'impact': 'Amount of memory used for shared memory buffers',
        'critical_threshold': '2GB',
        'context': 'postmaster',
        'dynamic': False,
    },
    'effective_cache_size': {
        'value': '24GB',
        'unit': 'memory',
        'impact': 'Planner\'s assumption about effective cache size',
        'critical_threshold': '4GB',
        'context': 'user',
        'dynamic': True,
    },
    'maintenance_work_mem': {
        'value': '2GB',
        'unit': 'memory',
        'impact': 'Memory for maintenance operations',
        'critical_threshold': '256MB',
        'context': 'user',
        'dynamic': True,
    },
    'checkpoint_completion_target': {
        'value': 0.9,
        'unit': None,
        'impact': 'Fraction of checkpoint interval to spread writes',
        'critical_threshold': 0.5,
        'context': 'sighup',
        'dynamic': True,
    },
    'wal_buffers': {
        'value': '64MB',
        'unit': 'memory',
        'impact': 'Amount of memory used for WAL data',
        'critical_threshold': '16MB',
        'context': 'postmaster',
        'dynamic': False,
    },
    'default_statistics_target': {
        'value': 500,
        'unit': None,
        'impact': 'Number of statistics samples for complex query planning',
        'critical_threshold': 100,
        'context': 'user',
        'dynamic': True,
    },
    'random_page_cost': {
        'value': 1.1,
        'unit': None,
        'impact': 'Planner\'s cost estimate for random page access',
        'critical_threshold': 4.0,
        'context': 'user',
        'dynamic': True,
    },
    'effective_io_concurrency': {
        'value': 200,
        'unit': None,
        'impact': 'Number of concurrent disk I/O operations',
        'critical_threshold': 1,
        'context': 'user',
        'dynamic': True,
    },
    'work_mem': {
        'value': '64MB',
        'unit': 'memory',
        'impact': 'Memory for sorting and hash operations per query (OLAP needs more)',
        'critical_threshold': '4MB',
        'context': 'user',
        'dynamic': True,
    },
    'max_worker_processes': {
        'value': 16,
        'unit': None,
        'impact': 'Maximum number of background worker processes',
        'critical_threshold': 4,
        'context': 'postmaster',
        'dynamic': False,
    },
    'max_parallel_workers_per_gather': {
        'value': 8,
        'unit': None,
        'impact': 'Maximum parallel workers per query (OLAP benefits from parallelism)',
        'critical_threshold': 2,
        'context': 'user',
        'dynamic': True,
    },
    'max_parallel_workers': {
        'value': 16,
        'unit': None,
        'impact': 'Maximum total parallel workers for all queries',
        'critical_threshold': 4,
        'context': 'user',
        'dynamic': True,
    },
}

# Recommended PostgreSQL settings for Mixed workloads
RECOMMENDED_SETTINGS_MIXED = {
    'max_connections': {
        'value': 75,
        'unit': None,
        'impact': 'Maximum number of concurrent database connections (balanced)',
        'critical_threshold': 10,
        'context': 'postmaster',
        'dynamic': False,
    },
    'shared_buffers': {
        'value': '2GB',
        'unit': 'memory',
        'impact': 'Amount of memory used for shared memory buffers (balanced)',
        'critical_threshold': '512MB',
        'context': 'postmaster',
        'dynamic': False,
    },
    'effective_cache_size': {
        'value': '12GB',
        'unit': 'memory',
        'impact': 'Planner\'s assumption about effective cache size',
        'critical_threshold': '2GB',
        'context': 'user',
        'dynamic': True,
    },
    'maintenance_work_mem': {
        'value': '256MB',
        'unit': 'memory',
        'impact': 'Memory for maintenance operations',
        'critical_threshold': '64MB',
        'context': 'user',
        'dynamic': True,
    },
    'checkpoint_completion_target': {
        'value': 0.9,
        'unit': None,
        'impact': 'Fraction of checkpoint interval to spread writes',
        'critical_threshold': 0.5,
        'context': 'sighup',
        'dynamic': True,
    },
    'wal_buffers': {
        'value': '32MB',
        'unit': 'memory',
        'impact': 'Amount of memory used for WAL data',
        'critical_threshold': '4MB',
        'context': 'postmaster',
        'dynamic': False,
    },
    'default_statistics_target': {
        'value': 200,
        'unit': None,
        'impact': 'Number of statistics samples for query planning (balanced)',
        'critical_threshold': 50,
        'context': 'user',
        'dynamic': True,
    },
    'random_page_cost': {
        'value': 1.1,
        'unit': None,
        'impact': 'Planner\'s cost estimate for random page access',
        'critical_threshold': 4.0,
        'context': 'user',
        'dynamic': True,
    },
    'effective_io_concurrency': {
        'value': 200,
        'unit': None,
        'impact': 'Number of concurrent disk I/O operations',
        'critical_threshold': 1,
        'context': 'user',
        'dynamic': True,
    },
    'work_mem': {
        'value': '16MB',
        'unit': 'memory',
        'impact': 'Memory for sorting and hash operations per query (balanced)',
        'critical_threshold': '2MB',
        'context': 'user',
        'dynamic': True,
    },
    'max_worker_processes': {
        'value': 12,
        'unit': None,
        'impact': 'Maximum number of background worker processes',
        'critical_threshold': 2,
        'context': 'postmaster',
        'dynamic': False,
    },
    'max_parallel_workers_per_gather': {
        'value': 4,
        'unit': None,
        'impact': 'Maximum parallel workers per query (balanced)',
        'critical_threshold': 0,
        'context': 'user',
        'dynamic': True,
    },
}


def _normalize_memory_value(value: str) -> int:
    """
    Convert PostgreSQL memory string to bytes.
    
    PostgreSQL accepts memory values with units like kB, MB, GB.
    This function normalizes all memory values to bytes for comparison.
    
    Args:
        value: Memory value string (e.g., '256MB', '4GB', '163848kB').
    
    Returns:
        Value in bytes.
    
    Raises:
        ValueError: If value format is invalid.
    
    Example:
        >>> _normalize_memory_value('256MB')
        268435456
        >>> _normalize_memory_value('4GB')
        4294967296
    """
    value_str = str(value).strip().upper()
    
    # Handle numeric-only values (assume bytes)
    if value_str.isdigit():
        return int(value_str)
    
    # Parse value with unit
    units = {
        'KB': 1024,
        'MB': 1024 * 1024,
        'GB': 1024 * 1024 * 1024,
        'TB': 1024 * 1024 * 1024 * 1024,
    }
    
    for unit, multiplier in units.items():
        if value_str.endswith(unit):
            numeric_part = value_str[:-len(unit)].strip()
            try:
                return int(float(numeric_part) * multiplier)
            except ValueError:
                raise ValueError(f"Invalid memory value: {value}")
    
    # If no recognized unit found, raise error
    raise ValueError(f"Invalid memory value format: {value}")


def _compare_setting_values(
    current: Any,
    recommended: Any,
    setting_unit: Optional[str],
    setting_name: str = ""
) -> tuple[str, str]:
    """
    Compare current and recommended setting values.
    
    Args:
        current: Current setting value.
        recommended: Recommended setting value.
        setting_unit: Unit type ('memory', None).
        setting_name: Name of the setting for special case handling.
    
    Returns:
        Tuple of (status, message) where status is 'OK', 'WARNING', or 'CRITICAL'.
    """
    if setting_unit == 'memory':
        current_bytes = _normalize_memory_value(current)
        recommended_bytes = _normalize_memory_value(recommended)
        
        if current_bytes >= recommended_bytes:
            return 'OK', f'Setting is optimal ({current} >= {recommended})'
        elif current_bytes >= recommended_bytes * 0.5:
            return 'WARNING', f'Setting below recommended ({current} < {recommended})'
        else:
            return 'CRITICAL', f'Setting critically low ({current} << {recommended})'
    
    # Numeric comparison
    try:
        current_num = float(current)
        recommended_num = float(recommended)
        
        if current_num >= recommended_num:
            return 'OK', f'Setting is optimal ({current} >= {recommended})'
        elif current_num >= recommended_num * 0.7:
            return 'WARNING', f'Setting below recommended ({current} < {recommended})'
        else:
            return 'CRITICAL', f'Setting critically low ({current} << {recommended})'
    except (ValueError, TypeError):
        # String comparison as fallback
        if str(current) == str(recommended):
            return 'OK', f'Setting matches recommended value ({current})'
        else:
            return 'WARNING', f'Setting differs from recommended ({current} vs {recommended})'


from tuning_fork.shared.decorators.db_connection import get_db_connection


def check_settings(
    config: Config,
    setting_names: Optional[list[str]] = None,
    workload_type: str = 'OLTP'
) -> list[CheckResult]:
    """
    Check PostgreSQL configuration settings against recommended values.
    
    This function queries PostgreSQL for current settings and compares them
    against recommended best practices for the specified workload type.
    
    Args:
        config: Configuration object for database connection.
        setting_names: Optional list of specific settings to check. If None,
                      checks all settings in RECOMMENDED_SETTINGS.
        workload_type: Type of workload ('OLTP', 'OLAP', 'Mixed').
    
    Returns:
        List of CheckResult objects with status and recommendations.
    
    Raises:
        ValueError: If setting_names contains invalid settings or unsupported
                   workload_type.
        DatabaseConnectionError: If database connection fails.
    
    Example:
        >>> config = Config('config.yaml')
        >>> results = check_settings(config, workload_type='OLTP')
        >>> for result in results:
        ...     if result.status != 'OK':
        ...         print(f"{result.setting_name}: {result.message}")
    """
    # Validate workload type and get appropriate settings
    if workload_type == 'OLTP':
        recommended_settings = RECOMMENDED_SETTINGS_OLTP
    elif workload_type == 'OLAP':
        recommended_settings = RECOMMENDED_SETTINGS_OLAP
    elif workload_type == 'Mixed':
        recommended_settings = RECOMMENDED_SETTINGS_MIXED
    else:
        raise ValueError(
            f"Unsupported workload type: {workload_type}. "
            "Use 'OLTP', 'OLAP', or 'Mixed'."
        )
    
    # Determine which settings to check
    settings_to_check = setting_names if setting_names else list(recommended_settings.keys())
    
    # Validate setting names
    invalid_settings = set(settings_to_check) - set(recommended_settings.keys())
    if invalid_settings:
        raise ValueError(f"Unknown settings: {', '.join(invalid_settings)}")
    
    results: list[CheckResult] = []
    
    # Use context manager for database connection
    with get_db_connection(config) as (conn, cursor):
        for setting_name in settings_to_check:
            try:
                # Query PostgreSQL for current setting value
                cursor.execute(
                    "SELECT setting, unit FROM pg_settings WHERE name = %s",
                    (setting_name,)
                )
                row = cursor.fetchone()
                
                if row is None:
                    logger.warning(f"Setting '{setting_name}' not found in pg_settings")
                    results.append(CheckResult(
                        setting_name=setting_name,
                        current_value='NOT_FOUND',
                        recommended_value=recommended_settings[setting_name]['value'],
                        status='CRITICAL',
                        message=f"Setting not found in database",
                        impact=recommended_settings[setting_name]['impact'],
                        setting_unit=recommended_settings[setting_name]['unit'],
                        workload_type=workload_type
                    ))
                    continue
                
                current_value, pg_unit = row
                recommended_config = recommended_settings[setting_name]
                recommended_value = recommended_config['value']
                setting_unit = recommended_config['unit']
                impact = recommended_config['impact']
                
                # Convert PostgreSQL value to string with unit if it's a memory setting
                display_value = current_value
                if pg_unit and setting_unit == 'memory':
                    # PostgreSQL returns numeric value with separate unit
                    # e.g., value=16384, unit='8kB' means 16384*8kB = 128MB
                    display_value = f"{current_value}{pg_unit}"
                    current_value = display_value
                
                # Compare values
                status, message = _compare_setting_values(
                    current_value,
                    recommended_value,
                    setting_unit,
                    setting_name
                )
                
                results.append(CheckResult(
                    setting_name=setting_name,
                    current_value=current_value,
                    recommended_value=recommended_value,
                    status=status,
                    message=message,
                    impact=impact,
                    setting_unit=pg_unit,
                    workload_type=workload_type
                ))
                
            except Exception as exc:
                logger.error(f"Error checking setting '{setting_name}': {exc}")
                results.append(CheckResult(
                    setting_name=setting_name,
                    current_value='ERROR',
                    recommended_value=recommended_settings[setting_name]['value'],
                    status='CRITICAL',
                    message=f"Error checking setting: {exc}",
                    impact=recommended_settings[setting_name]['impact'],
                    setting_unit=recommended_settings[setting_name]['unit'],
                    workload_type=workload_type
                ))
    
    return results


def report_settings(check_results: list[CheckResult], format: str = 'text') -> str:
    """
    Generate a human-readable report from check results.
    
    Args:
        check_results: List of CheckResult objects from check_settings().
        format: Output format ('text', 'json', 'markdown').
    
    Returns:
        Formatted report string.
    
    Raises:
        ValueError: If format is not supported.
    
    Example:
        >>> results = check_settings(config)
        >>> report = report_settings(results, format='text')
        >>> print(report)
    """
    if not check_results:
        return "No settings checked."
    
    if format == 'json':
        return json.dumps([r.to_dict() for r in check_results], indent=2, default=str)
    
    if format == 'markdown':
        lines = ["# PostgreSQL Settings Report", ""]
        
        # Add workload type info if available
        if check_results:
            workload = check_results[0].workload_type
            lines.append(f"**Workload Type:** {workload}")
        
        lines.append(f"**Generated:** {datetime.utcnow().isoformat()}")
        lines.append("")
        lines.append("| Setting | Status | Current | Recommended | Message |")
        lines.append("|---------|--------|---------|-------------|---------|")
        
        for result in check_results:
            lines.append(
                f"| {result.setting_name} | {result.status} | "
                f"{result.current_value} | {result.recommended_value} | "
                f"{result.message} |"
            )
        
        return "\n".join(lines)
    
    if format == 'text':
        lines = ["=" * 80]
        lines.append("PostgreSQL Settings Check Report")
        
        # Add workload type info
        if check_results:
            workload = check_results[0].workload_type
            lines.append(f"Workload Type: {workload}")
        
        lines.append("=" * 80)
        lines.append(f"Generated: {datetime.utcnow().isoformat()}")
        lines.append("")
        
        # Group by status
        critical = [r for r in check_results if r.status == 'CRITICAL']
        warnings = [r for r in check_results if r.status == 'WARNING']
        ok = [r for r in check_results if r.status == 'OK']
        
        if critical:
            lines.append("CRITICAL ISSUES:")
            lines.append("-" * 80)
            for result in critical:
                lines.append(f"  • {result.setting_name}")
                lines.append(f"    Current: {result.current_value}")
                lines.append(f"    Recommended: {result.recommended_value}")
                lines.append(f"    Impact: {result.impact}")
                lines.append(f"    Message: {result.message}")
                lines.append("")
        
        if warnings:
            lines.append("WARNINGS:")
            lines.append("-" * 80)
            for result in warnings:
                lines.append(f"  • {result.setting_name}")
                lines.append(f"    Current: {result.current_value}")
                lines.append(f"    Recommended: {result.recommended_value}")
                lines.append(f"    Impact: {result.impact}")
                lines.append(f"    Message: {result.message}")
                lines.append("")
        
        if ok:
            lines.append("OK:")
            lines.append("-" * 80)
            for result in ok:
                lines.append(f"  ✓ {result.setting_name}: {result.current_value}")
        
        lines.append("")
        lines.append("=" * 80)
        lines.append(f"Summary: {len(critical)} critical, {len(warnings)} warnings, {len(ok)} ok")
        lines.append("=" * 80)
        
        return "\n".join(lines)
    
    raise ValueError(
        f"Unsupported format: {format}. "
        "Use 'text', 'json', or 'markdown'."
    )


def apply_settings(
    config: Config,
    settings_to_apply: dict[str, Any],
    persist: bool = True,
    workload_type: str = 'OLTP'
) -> list[ChangeLogEntry]:
    """
    Apply PostgreSQL configuration settings.
    
    This function applies settings using ALTER SYSTEM SET (persist=True) or
    SET (persist=False) commands and logs all changes for potential rollback.
    
    WARNING: This function modifies database settings. Use with caution.
    Some settings require a server reload or restart to take effect.
    
    Args:
        config: Configuration object.
        settings_to_apply: Dictionary mapping setting names to new values.
        persist: If True, use ALTER SYSTEM SET to persist settings across
                restarts. If False, use SET for session-only changes.
        workload_type: Type of workload to validate settings against.
    
    Returns:
        List of ChangeLogEntry objects documenting the changes.
    
    Raises:
        ValueError: If settings contain invalid names.
        RuntimeError: If applying a setting fails.
    
    Example:
        >>> config = Config('config.yaml')
        >>> changes = apply_settings(config, {'work_mem': '8MB'})
        >>> print(f"Applied {len(changes)} changes")
    
    Notes:
        - ALTER SYSTEM SET requires PostgreSQL 9.4+
        - Settings with context='postmaster' require server restart
        - Settings with context='sighup' require pg_reload_conf()
        - Settings with context='user' take effect immediately
    """
    # Get recommended settings for validation
    if workload_type == 'OLTP':
        recommended_settings = RECOMMENDED_SETTINGS_OLTP
    elif workload_type == 'OLAP':
        recommended_settings = RECOMMENDED_SETTINGS_OLAP
    elif workload_type == 'Mixed':
        recommended_settings = RECOMMENDED_SETTINGS_MIXED
    else:
        raise ValueError(f"Unsupported workload type: {workload_type}")
    
    # Validate setting names
    invalid_settings = set(settings_to_apply.keys()) - set(recommended_settings.keys())
    if invalid_settings:
        raise ValueError(f"Unknown settings: {', '.join(invalid_settings)}")
    
    change_log: list[ChangeLogEntry] = []
    
    # Use context manager for database connection
    with get_db_connection(config) as (conn, cursor):
        for setting_name, new_value in settings_to_apply.items():
            try:
                # Get current value and context before changing
                cursor.execute(
                    "SELECT setting, context FROM pg_settings WHERE name = %s",
                    (setting_name,)
                )
                row = cursor.fetchone()
                old_value = row[0] if row else None
                context = row[1] if row else None
                
                # Apply the new setting
                scope = "SYSTEM" if persist else "SESSION"
                
                if persist:
                    # Use ALTER SYSTEM SET for persistent changes
                    cursor.execute(
                        f"ALTER SYSTEM SET {setting_name} = %s",
                        (str(new_value),)
                    )
                    logger.info(
                        f"Applied setting: {setting_name} = {new_value} "
                        f"(was: {old_value}) [ALTER SYSTEM]"
                    )
                else:
                    # Use SET for session-only changes
                    cursor.execute(
                        f"SET {setting_name} = %s",
                        (str(new_value),)
                    )
                    logger.info(
                        f"Applied setting: {setting_name} = {new_value} "
                        f"(was: {old_value}) [SESSION]"
                    )
                
                # Warn about non-dynamic settings
                setting_config = recommended_settings[setting_name]
                if not setting_config['dynamic']:
                    if context == 'postmaster':
                        logger.warning(
                            f"Setting '{setting_name}' requires a server restart to take effect."
                        )
                    elif context == 'sighup':
                        logger.warning(
                            f"Setting '{setting_name}' requires pg_reload_conf() to take effect."
                        )
                
                # Create change log entry
                change_entry = ChangeLogEntry(
                    setting_name=setting_name,
                    old_value=old_value,
                    new_value=new_value,
                    timestamp=datetime.utcnow().isoformat(),
                    change_id=len(change_log) + 1,
                    scope=scope
                )
                change_log.append(change_entry)
                
            except Exception as exc:
                error_msg = f"Failed to apply setting '{setting_name}': {exc}"
                logger.error(error_msg)
                raise RuntimeError(error_msg) from exc
    
    # Log summary about reload/restart requirements
    if change_log:
        non_dynamic = [
            name for name in settings_to_apply.keys()
            if not recommended_settings[name]['dynamic']
        ]
        
        if non_dynamic:
            contexts = {}
            with get_db_connection(config) as (conn, cursor):
                for setting_name in non_dynamic:
                    cursor.execute(
                        "SELECT context FROM pg_settings WHERE name = %s",
                        (setting_name,)
                    )
                    row = cursor.fetchone()
                    if row:
                        context = row[0]
                        contexts.setdefault(context, []).append(setting_name)
            
            if 'postmaster' in contexts:
                logger.warning(
                    f"The following settings require a server RESTART: "
                    f"{', '.join(contexts['postmaster'])}"
                )
            if 'sighup' in contexts:
                logger.warning(
                    f"The following settings require pg_reload_conf(): "
                    f"{', '.join(contexts['sighup'])}"
                )
    
    return change_log


def rollback_settings(
    config: Config,
    change_log_entries: list[ChangeLogEntry],
    persist: bool = True
) -> list[str]:
    """
    Rollback previously applied settings to their old values.
    
    Args:
        config: Configuration object.
        change_log_entries: List of ChangeLogEntry objects to rollback.
        persist: If True, use ALTER SYSTEM SET to persist rollback.
                If False, use SET for session-only rollback.
    
    Returns:
        List of setting names that were successfully rolled back.
    
    Raises:
        ValueError: If change log is empty.
        RuntimeError: If rollback fails.
    
    Example:
        >>> config = Config('config.yaml')
        >>> # After applying settings
        >>> changes = apply_settings(config, {'work_mem': '8MB'})
        >>> # Rollback if something went wrong
        >>> rolled_back = rollback_settings(config, changes)
    """
    if not change_log_entries:
        raise ValueError("No change log entries provided for rollback")
    
    rolled_back: list[str] = []
    
    # Use context manager for database connection
    with get_db_connection(config) as (conn, cursor):
        for entry in change_log_entries:
            if entry.rollback_status:
                logger.info(f"Setting '{entry.setting_name}' already rolled back, skipping")
                continue
            
            try:
                # Restore old value
                if persist or entry.scope == "SYSTEM":
                    cursor.execute(
                        f"ALTER SYSTEM SET {entry.setting_name} = %s",
                        (str(entry.old_value),)
                    )
                    logger.info(
                        f"Rolled back setting: {entry.setting_name} = {entry.old_value} "
                        f"(was: {entry.new_value}) [ALTER SYSTEM]"
                    )
                else:
                    cursor.execute(
                        f"SET {entry.setting_name} = %s",
                        (str(entry.old_value),)
                    )
                    logger.info(
                        f"Rolled back setting: {entry.setting_name} = {entry.old_value} "
                        f"(was: {entry.new_value}) [SESSION]"
                    )
                
                entry.rollback_status = True
                rolled_back.append(entry.setting_name)
                
            except Exception as exc:
                error_msg = f"Failed to rollback setting '{entry.setting_name}': {exc}"
                logger.error(error_msg)
                raise RuntimeError(error_msg) from exc
    
    if rolled_back:
        logger.warning(
            "Settings rolled back. Run 'SELECT pg_reload_conf();' or restart PostgreSQL "
            "for changes to take effect (depending on setting context)."
        )
    
    return rolled_back