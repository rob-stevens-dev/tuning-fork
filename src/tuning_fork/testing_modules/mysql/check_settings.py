"""
MySQL/MariaDB settings checker module.

This module provides functionality to check, report, apply, and rollback
MySQL/MariaDB configuration settings against recommended best practices.

The module supports multiple workload types (OLTP, OLAP, Mixed) and can be
extended to load recommendations from external YAML/JSON files.
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
        setting_name: Name of the MySQL/MariaDB setting.
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
        scope: Setting scope ('GLOBAL' or 'SESSION').
    """
    setting_name: str
    old_value: Any
    new_value: Any
    timestamp: str
    change_id: int
    applied_by: str = "tuning_fork"
    rollback_status: bool = False
    scope: str = "GLOBAL"
    
    def to_dict(self) -> dict[str, Any]:
        """Convert change log entry to dictionary."""
        return asdict(self)


# Recommended MySQL/MariaDB settings for OLTP workloads
# These are conservative defaults that work well for most OLTP scenarios
# TODO: Move to external YAML/JSON file for easier customization
RECOMMENDED_SETTINGS_OLTP = {
    'max_connections': {
        'value': 151,  # MySQL default, good for most cases
        'unit': None,
        'impact': 'Maximum number of concurrent client connections',
        'critical_threshold': 50,
        'scope': 'GLOBAL',
        'dynamic': True,
    },
    'innodb_buffer_pool_size': {
        'value': '2G',  # Should be 70-80% of available RAM
        'unit': 'memory',
        'impact': 'InnoDB buffer pool size for caching data and indexes',
        'critical_threshold': '512M',
        'scope': 'GLOBAL',
        'dynamic': False,  # Requires restart
    },
    'innodb_log_file_size': {
        'value': '512M',
        'unit': 'memory',
        'impact': 'Size of each InnoDB redo log file',
        'critical_threshold': '48M',
        'scope': 'GLOBAL',
        'dynamic': False,  # Requires restart in MySQL 5.7, dynamic in 8.0+
    },
    'innodb_flush_log_at_trx_commit': {
        'value': 1,  # ACID compliance, safest but slower
        'unit': None,
        'impact': 'Controls durability of transactions (0=fast/risky, 1=safe, 2=balanced)',
        'critical_threshold': None,
        'scope': 'GLOBAL',
        'dynamic': True,
    },
    'innodb_flush_method': {
        'value': 'O_DIRECT',
        'unit': None,
        'impact': 'How InnoDB flushes data to disk',
        'critical_threshold': None,
        'scope': 'GLOBAL',
        'dynamic': False,
    },
    'innodb_file_per_table': {
        'value': 'ON',
        'unit': None,
        'impact': 'Store each InnoDB table in its own file',
        'critical_threshold': None,
        'scope': 'GLOBAL',
        'dynamic': True,
    },
    'max_allowed_packet': {
        'value': '64M',
        'unit': 'memory',
        'impact': 'Maximum size of one packet or generated/intermediate string',
        'critical_threshold': '1M',
        'scope': 'GLOBAL',
        'dynamic': True,
    },
    'thread_cache_size': {
        'value': 32,
        'unit': None,
        'impact': 'Number of threads server caches for reuse',
        'critical_threshold': 8,
        'scope': 'GLOBAL',
        'dynamic': True,
    },
    'query_cache_size': {
        'value': 0,  # Deprecated in MySQL 8.0, should be disabled
        'unit': 'memory',
        'impact': 'Query cache size (deprecated in MySQL 8.0)',
        'critical_threshold': None,
        'scope': 'GLOBAL',
        'dynamic': True,
    },
    'query_cache_type': {
        'value': 0,  # OFF (deprecated in MySQL 8.0)
        'unit': None,
        'impact': 'Query cache type (deprecated in MySQL 8.0)',
        'critical_threshold': None,
        'scope': 'GLOBAL',
        'dynamic': True,
    },
    'tmp_table_size': {
        'value': '64M',
        'unit': 'memory',
        'impact': 'Maximum size of internal in-memory temporary tables',
        'critical_threshold': '16M',
        'scope': 'GLOBAL',
        'dynamic': True,
    },
    'max_heap_table_size': {
        'value': '64M',
        'unit': 'memory',
        'impact': 'Maximum size for user-created MEMORY tables',
        'critical_threshold': '16M',
        'scope': 'GLOBAL',
        'dynamic': True,
    },
    'sort_buffer_size': {
        'value': '2M',
        'unit': 'memory',
        'impact': 'Buffer size for sorting operations',
        'critical_threshold': '256K',
        'scope': 'BOTH',
        'dynamic': True,
    },
    'read_buffer_size': {
        'value': '1M',
        'unit': 'memory',
        'impact': 'Buffer size for sequential table scans',
        'critical_threshold': '128K',
        'scope': 'BOTH',
        'dynamic': True,
    },
    'read_rnd_buffer_size': {
        'value': '2M',
        'unit': 'memory',
        'impact': 'Buffer size for reading rows in sorted order',
        'critical_threshold': '256K',
        'scope': 'BOTH',
        'dynamic': True,
    },
    'join_buffer_size': {
        'value': '2M',
        'unit': 'memory',
        'impact': 'Buffer size for joins without indexes',
        'critical_threshold': '256K',
        'scope': 'BOTH',
        'dynamic': True,
    },
    'table_open_cache': {
        'value': 4000,
        'unit': None,
        'impact': 'Number of open tables for all threads',
        'critical_threshold': 400,
        'scope': 'GLOBAL',
        'dynamic': True,
    },
}


def _normalize_memory_value(value: str) -> int:
    """
    Convert MySQL/MariaDB memory string to bytes.
    
    Args:
        value: Memory value string (e.g., '256M', '4G', '1024K').
    
    Returns:
        Value in bytes.
    
    Example:
        >>> _normalize_memory_value('256M')
        268435456
        >>> _normalize_memory_value('4G')
        4294967296
    """
    if isinstance(value, (int, float)):
        return int(value)
    
    value_str = str(value).strip().upper()
    
    # Handle numeric-only values (assume bytes)
    if value_str.isdigit():
        return int(value_str)
    
    # Parse value with unit
    multipliers = {
        'K': 1024,
        'M': 1024 ** 2,
        'G': 1024 ** 3,
        'T': 1024 ** 4,
    }
    
    for unit, multiplier in multipliers.items():
        if value_str.endswith(unit):
            numeric_part = value_str[:-1].strip()
            try:
                return int(float(numeric_part) * multiplier)
            except ValueError:
                logger.warning(f"Could not parse memory value: {value}")
                return 0
    
    # If no unit matched, try to parse as integer
    try:
        return int(value_str)
    except ValueError:
        logger.warning(f"Could not parse memory value: {value}")
        return 0


def _compare_setting_values(
    current: Any,
    recommended: Any,
    setting_unit: Optional[str],
    setting_name: str
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
    # Special case for query_cache (deprecated in MySQL 8.0)
    if 'query_cache' in setting_name.lower():
        # In MySQL 8.0+, query cache is removed, so any value is OK
        return 'OK', 'Setting is acceptable (query cache deprecated in MySQL 8.0+)'
    
    # Special case for ON/OFF settings
    if isinstance(recommended, str) and recommended.upper() in ('ON', 'OFF'):
        current_str = str(current).upper()
        recommended_str = recommended.upper()
        if current_str == recommended_str:
            return 'OK', f'Setting is optimal ({current} = {recommended})'
        else:
            return 'WARNING', f'Setting differs from recommended ({current} vs {recommended})'
    
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
    Check MySQL/MariaDB configuration settings against recommended values.
    
    This function queries MySQL/MariaDB for current settings and compares them
    against recommended best practices for the specified workload type.
    
    Args:
        config: Configuration object for database connection.
        setting_names: Optional list of specific settings to check. If None,
                      checks all settings in RECOMMENDED_SETTINGS.
        workload_type: Type of workload ('OLTP', 'OLAP', 'Mixed'). Currently
                      only 'OLTP' is supported.
    
    Returns:
        List of CheckResult objects with status and recommendations.
    
    Raises:
        ValueError: If setting_names contains invalid settings or unsupported
                   workload_type.
        DatabaseConnectionError: If database connection fails.
    
    Example:
        >>> config = Config('config.yaml')
        >>> results = check_settings(config)
        >>> for result in results:
        ...     if result.status != 'OK':
        ...         print(f"{result.setting_name}: {result.message}")
    """
    # Validate workload type
    if workload_type != 'OLTP':
        raise ValueError(
            f"Unsupported workload type: {workload_type}. Currently only 'OLTP' is supported."
        )
    
    # Get recommended settings for workload type
    recommended_settings = RECOMMENDED_SETTINGS_OLTP
    
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
                # Query MySQL/MariaDB for current setting value
                cursor.execute(
                    "SHOW VARIABLES LIKE %s",
                    (setting_name,)
                )
                row = cursor.fetchone()
                
                if row is None:
                    # Special handling for deprecated query_cache settings
                    if 'query_cache' in setting_name.lower():
                        logger.info(f"Setting '{setting_name}' not found (removed in MySQL 8.0+)")
                        results.append(CheckResult(
                            setting_name=setting_name,
                            current_value='REMOVED',
                            recommended_value=recommended_settings[setting_name]['value'],
                            status='OK',
                            message=f"Setting removed in MySQL 8.0+ (expected)",
                            impact=recommended_settings[setting_name]['impact'],
                            setting_unit=recommended_settings[setting_name]['unit'],
                            workload_type=workload_type
                        ))
                    else:
                        logger.warning(f"Setting '{setting_name}' not found in MySQL variables")
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
                
                _, current_value = row
                recommended_config = recommended_settings[setting_name]
                recommended_value = recommended_config['value']
                setting_unit = recommended_config['unit']
                impact = recommended_config['impact']
                
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
                    setting_unit=setting_unit,
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


def report_settings(
    check_results: list[CheckResult],
    format: str = 'text'
) -> str:
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
        lines = ["# MySQL/MariaDB Settings Report", ""]
        lines.append(f"**Generated:** {datetime.utcnow().isoformat()}")
        lines.append(f"**Workload Type:** {check_results[0].workload_type}")
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
        lines.append("MySQL/MariaDB Settings Check Report")
        lines.append("=" * 80)
        lines.append(f"Generated: {datetime.utcnow().isoformat()}")
        lines.append(f"Workload Type: {check_results[0].workload_type}")
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
    
    raise ValueError(f"Unsupported format: {format}. Use 'text', 'json', or 'markdown'.")


def apply_settings(
    config: Config,
    settings_to_apply: dict[str, Any],
    persist: bool = True
) -> list[ChangeLogEntry]:
    """
    Apply MySQL/MariaDB configuration settings.
    
    This function applies settings using SET GLOBAL commands and optionally
    persists them. Logs all changes to the command log database for potential rollback.
    
    WARNING: This function modifies database settings. Use with caution.
    Some settings may require a server restart to take effect.
    
    Args:
        config: Configuration object.
        settings_to_apply: Dictionary mapping setting names to new values.
        persist: If True, use SET PERSIST (MySQL 8.0+) to persist settings
                across restarts. If False, use SET GLOBAL (session-only).
    
    Returns:
        List of ChangeLogEntry objects documenting the changes.
    
    Raises:
        ValueError: If settings contain invalid names.
        RuntimeError: If applying a setting fails.
    
    Example:
        >>> config = Config('config.yaml')
        >>> changes = apply_settings(config, {'max_connections': 200})
        >>> print(f"Applied {len(changes)} changes")
    
    Notes:
        - SET PERSIST requires MySQL 8.0+ or MariaDB 10.5+
        - Some settings are not dynamic and require restart
        - Check RECOMMENDED_SETTINGS['setting_name']['dynamic']
    """
    # Validate setting names
    recommended_settings = RECOMMENDED_SETTINGS_OLTP
    invalid_settings = set(settings_to_apply.keys()) - set(recommended_settings.keys())
    if invalid_settings:
        raise ValueError(f"Unknown settings: {', '.join(invalid_settings)}")
    
    change_log: list[ChangeLogEntry] = []
    
    # Use context manager for database connection
    with get_db_connection(config) as (conn, cursor):
        # Check MySQL version for PERSIST support
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()[0]
        supports_persist = False
        
        if 'MariaDB' in version:
            # MariaDB 10.5+ supports SET PERSIST
            version_num = float('.'.join(version.split('-')[0].split('.')[:2]))
            supports_persist = version_num >= 10.5
        else:
            # MySQL 8.0+ supports SET PERSIST
            version_num = float('.'.join(version.split('.')[0:2]))
            supports_persist = version_num >= 8.0
        
        if persist and not supports_persist:
            logger.warning(
                f"SET PERSIST not supported in {version}. "
                "Using SET GLOBAL instead (changes won't persist across restarts)."
            )
            persist = False
        
        for setting_name, new_value in settings_to_apply.items():
            try:
                # Get current value before changing
                cursor.execute("SHOW VARIABLES LIKE %s", (setting_name,))
                row = cursor.fetchone()
                old_value = row[1] if row else None
                
                # Check if setting is dynamic
                setting_config = recommended_settings[setting_name]
                if not setting_config['dynamic']:
                    logger.warning(
                        f"Setting '{setting_name}' is not dynamic. "
                        "A server restart will be required for changes to take effect."
                    )
                
                # Apply the new setting
                set_command = "SET PERSIST" if persist else "SET GLOBAL"
                
                # Format value appropriately
                if isinstance(new_value, str):
                    cursor.execute(f"{set_command} {setting_name} = %s", (new_value,))
                else:
                    cursor.execute(f"{set_command} {setting_name} = {new_value}")
                
                logger.info(f"Applied setting: {setting_name} = {new_value} (was: {old_value})")
                
                # Create change log entry
                change_entry = ChangeLogEntry(
                    setting_name=setting_name,
                    old_value=old_value,
                    new_value=new_value,
                    timestamp=datetime.utcnow().isoformat(),
                    change_id=len(change_log) + 1,
                    scope='GLOBAL'
                )
                change_log.append(change_entry)
                
            except Exception as exc:
                error_msg = f"Failed to apply setting '{setting_name}': {exc}"
                logger.error(error_msg)
                raise RuntimeError(error_msg) from exc
    
    # Log reminder about non-dynamic settings
    non_dynamic = [
        name for name in settings_to_apply.keys()
        if not recommended_settings[name]['dynamic']
    ]
    
    if non_dynamic:
        logger.warning(
            f"The following settings are not dynamic and require a restart: "
            f"{', '.join(non_dynamic)}"
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
        persist: If True, use SET PERSIST to persist rollback across restarts.
    
    Returns:
        List of setting names that were successfully rolled back.
    
    Raises:
        ValueError: If change log is empty.
        RuntimeError: If rollback fails.
    
    Example:
        >>> config = Config('config.yaml')
        >>> # After applying settings
        >>> changes = apply_settings(config, {'max_connections': 200})
        >>> # Rollback if something went wrong
        >>> rolled_back = rollback_settings(config, changes)
    """
    if not change_log_entries:
        raise ValueError("No change log entries provided for rollback")
    
    rolled_back: list[str] = []
    
    # Use context manager for database connection
    with get_db_connection(config) as (conn, cursor):
        # Check MySQL version for PERSIST support
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()[0]
        supports_persist = False
        
        if 'MariaDB' in version:
            version_num = float('.'.join(version.split('-')[0].split('.')[:2]))
            supports_persist = version_num >= 10.5
        else:
            version_num = float('.'.join(version.split('.')[0:2]))
            supports_persist = version_num >= 8.0
        
        if persist and not supports_persist:
            logger.warning(
                f"SET PERSIST not supported in {version}. "
                "Using SET GLOBAL instead (changes won't persist across restarts)."
            )
            persist = False
        
        for entry in change_log_entries:
            if entry.rollback_status:
                logger.info(f"Setting '{entry.setting_name}' already rolled back, skipping")
                continue
            
            try:
                # Restore old value
                set_command = "SET PERSIST" if persist else "SET GLOBAL"
                
                if isinstance(entry.old_value, str):
                    cursor.execute(
                        f"{set_command} {entry.setting_name} = %s",
                        (entry.old_value,)
                    )
                else:
                    cursor.execute(f"{set_command} {entry.setting_name} = {entry.old_value}")
                
                logger.info(
                    f"Rolled back setting: {entry.setting_name} = {entry.old_value} "
                    f"(was: {entry.new_value})"
                )
                
                entry.rollback_status = True
                rolled_back.append(entry.setting_name)
                
            except Exception as exc:
                error_msg = f"Failed to rollback setting '{entry.setting_name}': {exc}"
                logger.error(error_msg)
                raise RuntimeError(error_msg) from exc
    
    if rolled_back:
        logger.info(f"Successfully rolled back {len(rolled_back)} settings")
    
    return rolled_back