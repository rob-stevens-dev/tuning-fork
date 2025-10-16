"""
Command logging decorator module.

This module provides decorators for logging database commands, their execution
time, and optionally their results to a SQLite database for audit and analysis.
"""

import functools
import json
import logging
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional, TypeVar, cast

from tuning_fork.config import Config

# Type variables for generic decorator typing
F = TypeVar('F', bound=Callable[..., Any])

# Module-level logger
logger = logging.getLogger(__name__)


class CommandLoggerError(Exception):
    """Raised when command logging operations fail."""
    pass


class CommandLogger:
    """
    SQLite-based command logger for tracking database operations.
    
    This class manages a SQLite database for logging command execution,
    including the command text, execution time, results, and metadata.
    """
    
    def __init__(
        self,
        log_file: str,
        auto_create: bool = True,
        auto_backup: bool = False
    ) -> None:
        """
        Initialize command logger.
        
        Args:
            log_file: Path to SQLite log database file.
            auto_create: If True, automatically create log database and tables.
            auto_backup: If True, create backup before modifications (future feature).
        
        Raises:
            CommandLoggerError: If database initialization fails.
        """
        self.log_file = log_file
        self.auto_create = auto_create
        self.auto_backup = auto_backup
        
        if auto_create:
            self._ensure_log_database()
    
    def _ensure_log_database(self) -> None:
        """
        Ensure log database exists and has correct schema.
        
        Creates the directory structure and initializes the database
        with the required logging table.
        
        Raises:
            CommandLoggerError: If database creation fails.
        """
        try:
            # Create directory if it doesn't exist
            log_path = Path(self.log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Connect and create schema
            with sqlite3.connect(self.log_file) as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS command_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT NOT NULL,
                        function_name TEXT NOT NULL,
                        command_type TEXT,
                        command_text TEXT,
                        execution_time_seconds REAL NOT NULL,
                        row_count INTEGER,
                        success BOOLEAN NOT NULL,
                        error_message TEXT,
                        result_summary TEXT,
                        metadata TEXT
                    )
                """)
                
                # Create indexes for common queries
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_timestamp 
                    ON command_log(timestamp)
                """)
                
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_function_name 
                    ON command_log(function_name)
                """)
                
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_success 
                    ON command_log(success)
                """)
                
                conn.commit()
                logger.info(f"Command log database initialized: {self.log_file}")
                
        except sqlite3.Error as exc:
            raise CommandLoggerError(
                f"Failed to initialize log database: {exc}"
            ) from exc
    
    def log_command(
        self,
        function_name: str,
        command_text: Optional[str],
        execution_time: float,
        success: bool,
        command_type: Optional[str] = None,
        row_count: Optional[int] = None,
        error_message: Optional[str] = None,
        result_summary: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None
    ) -> int:
        """
        Log a command execution to the database.
        
        Args:
            function_name: Name of the function that executed the command.
            command_text: The SQL command or operation text.
            execution_time: Execution time in seconds.
            success: Whether the command succeeded.
            command_type: Type of command (SELECT, INSERT, UPDATE, etc.).
            row_count: Number of rows affected/returned.
            error_message: Error message if command failed.
            result_summary: Brief summary of results (not full data).
            metadata: Additional metadata as dictionary.
        
        Returns:
            Log entry ID.
        
        Raises:
            CommandLoggerError: If logging fails.
        """
        try:
            with sqlite3.connect(self.log_file) as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    INSERT INTO command_log (
                        timestamp, function_name, command_type, command_text,
                        execution_time_seconds, row_count, success,
                        error_message, result_summary, metadata
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    datetime.utcnow().isoformat(),
                    function_name,
                    command_type,
                    command_text,
                    execution_time,
                    row_count,
                    success,
                    error_message,
                    result_summary,
                    json.dumps(metadata) if metadata else None
                ))
                
                conn.commit()
                log_id = cursor.lastrowid
                
                logger.debug(
                    f"Logged command for '{function_name}': "
                    f"{'success' if success else 'failure'} in {execution_time:.4f}s"
                )
                
                return log_id
                
        except sqlite3.Error as exc:
            logger.error(f"Failed to log command: {exc}")
            raise CommandLoggerError(f"Failed to log command: {exc}") from exc


def with_command_logging(
    config: Config,
    log_results: bool = False,
    max_result_length: int = 1000,
    extract_command_type: bool = True
) -> Callable[[F], F]:
    """
    Decorator that logs database commands to SQLite.
    
    This decorator captures command execution details including timing,
    success/failure, and optionally result summaries. It's designed to
    stack with other decorators like @log_execution_time and @with_database.
    
    Args:
        config: Configuration object containing logging settings.
        log_results: If True, log a summary of results (default: False).
        max_result_length: Maximum length of result summary (default: 1000).
        extract_command_type: If True, try to extract command type from cursor
                             operations (default: True).
    
    Returns:
        Decorated function with command logging.
    
    Example:
        >>> config = Config('config.yaml')
        >>> @log_execution_time()
        ... @with_command_logging(config, log_results=True)
        ... @with_database(config, autocommit=True)
        ... def get_users(cursor=None):
        ...     cursor.execute("SELECT * FROM users")
        ...     return cursor.fetchall()
        >>> users = get_users()
        # Logs: command, execution time, row count to SQLite
    
    Notes:
        - Stack order matters: timing -> logging -> database
        - Uses config['logging']['filename'] for log database path
        - Automatically creates log database if it doesn't exist
        - Thread-safe via SQLite's built-in concurrency handling
    """
    def decorator(func: F) -> F:
        # Get logging configuration
        log_config = config.config.get('logging', {})
        log_file = log_config.get('filename', 'logging/commands.db')
        auto_create = log_config.get('auto_create', True)
        auto_backup = log_config.get('auto_backup', False)
        
        # Initialize command logger
        cmd_logger = CommandLogger(log_file, auto_create, auto_backup)
        
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            """Wrapper function that performs command logging."""
            func_name = func.__qualname__
            start_time = time.perf_counter()
            command_text: Optional[str] = None
            command_type: Optional[str] = None
            row_count: Optional[int] = None
            result_summary: Optional[str] = None
            error_message: Optional[str] = None
            success = False
            result = None
            
            try:
                # Execute the wrapped function
                result = func(*args, **kwargs)
                success = True
                
                # Try to extract command information from cursor if available
                cursor = kwargs.get('cursor')
                if cursor and hasattr(cursor, 'query'):
                    # PostgreSQL psycopg2 cursor has query attribute
                    if cursor.query:
                        command_text = cursor.query.decode('utf-8') if isinstance(
                            cursor.query, bytes
                        ) else str(cursor.query)
                
                if cursor and hasattr(cursor, 'rowcount') and cursor.rowcount >= 0:
                    row_count = cursor.rowcount
                
                # Extract command type from query text
                if extract_command_type and command_text:
                    command_type = command_text.strip().split()[0].upper()
                
                # Create result summary if requested
                if log_results and result is not None:
                    if isinstance(result, (list, tuple)):
                        result_summary = f"{len(result)} rows returned"
                        if len(result) > 0 and max_result_length > 0:
                            # Include first row as sample
                            sample = str(result[0])
                            if len(sample) > max_result_length:
                                sample = sample[:max_result_length] + "..."
                            result_summary += f", sample: {sample}"
                    else:
                        result_str = str(result)
                        if len(result_str) > max_result_length:
                            result_str = result_str[:max_result_length] + "..."
                        result_summary = result_str
                
                return result
                
            except Exception as exc:
                success = False
                error_message = f"{type(exc).__name__}: {exc}"
                raise
            
            finally:
                # Calculate execution time
                end_time = time.perf_counter()
                execution_time = end_time - start_time
                
                # Log the command execution
                try:
                    cmd_logger.log_command(
                        function_name=func_name,
                        command_text=command_text,
                        execution_time=execution_time,
                        success=success,
                        command_type=command_type,
                        row_count=row_count,
                        error_message=error_message,
                        result_summary=result_summary,
                        metadata={
                            'args_count': len(args),
                            'kwargs_keys': list(kwargs.keys())
                        }
                    )
                except CommandLoggerError as log_exc:
                    # Don't fail the original function if logging fails
                    logger.warning(f"Failed to log command: {log_exc}")
        
        return cast(F, wrapper)
    
    return decorator