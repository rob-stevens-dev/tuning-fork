"""
Database connection decorator module.

This module provides decorators for automatic database connection and cursor
management with proper resource cleanup and error handling.
"""

import functools
import logging
from contextlib import contextmanager
from typing import Any, Callable, Optional, TypeVar, cast

import psycopg2
import psycopg2.extensions
from psycopg2 import pool

from tuning_fork.config import Config

# Type variables for generic decorator typing
F = TypeVar('F', bound=Callable[..., Any])

# Module-level logger
logger = logging.getLogger(__name__)


class DatabaseConnectionError(Exception):
    """Raised when database connection fails."""
    pass


class DatabaseConnectionPool:
    """
    Singleton connection pool manager for PostgreSQL databases.
    
    This class manages a connection pool to avoid creating new connections
    for every decorated function call, improving performance significantly.
    """
    
    _instance: Optional['DatabaseConnectionPool'] = None
    _pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None
    
    def __new__(cls) -> 'DatabaseConnectionPool':
        """Ensure only one instance of the connection pool exists."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def initialize(
        self,
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        min_connections: int = 1,
        max_connections: int = 10
    ) -> None:
        """
        Initialize the connection pool with database credentials.
        
        Args:
            host: Database host address.
            port: Database port number.
            database: Database name.
            username: Database username.
            password: Database password.
            min_connections: Minimum number of connections in pool.
            max_connections: Maximum number of connections in pool.
        
        Raises:
            DatabaseConnectionError: If pool initialization fails.
        """
        if self._pool is not None:
            logger.debug("Connection pool already initialized")
            return
        
        try:
            self._pool = psycopg2.pool.ThreadedConnectionPool(
                min_connections,
                max_connections,
                host=host,
                port=port,
                database=database,
                user=username,
                password=password,
                connect_timeout=10
            )
            logger.info(
                f"Database connection pool initialized: {username}@{host}:{port}/{database}"
            )
        except psycopg2.Error as exc:
            raise DatabaseConnectionError(
                f"Failed to initialize connection pool: {exc}"
            ) from exc
    
    def get_connection(self) -> psycopg2.extensions.connection:
        """
        Get a connection from the pool.
        
        Returns:
            Database connection object.
        
        Raises:
            DatabaseConnectionError: If pool is not initialized or connection fails.
        """
        if self._pool is None:
            raise DatabaseConnectionError(
                "Connection pool not initialized. Call initialize() first."
            )
        
        try:
            return self._pool.getconn()
        except psycopg2.Error as exc:
            raise DatabaseConnectionError(
                f"Failed to get connection from pool: {exc}"
            ) from exc
    
    def return_connection(
        self,
        connection: psycopg2.extensions.connection,
        close: bool = False
    ) -> None:
        """
        Return a connection to the pool.
        
        Args:
            connection: Database connection to return.
            close: If True, close the connection instead of returning to pool.
        """
        if self._pool is None:
            logger.warning("Attempting to return connection to uninitialized pool")
            return
        
        try:
            if close:
                self._pool.putconn(connection, close=True)
            else:
                self._pool.putconn(connection)
        except psycopg2.Error as exc:
            logger.error(f"Error returning connection to pool: {exc}")
    
    def close_all(self) -> None:
        """Close all connections in the pool."""
        if self._pool is not None:
            self._pool.closeall()
            self._pool = None
            logger.info("All database connections closed")


@contextmanager
def get_db_connection(config: Config):
    """
    Context manager for database connections with automatic cleanup.
    
    Args:
        config: Configuration object containing database settings.
    
    Yields:
        tuple: (connection, cursor) objects.
    
    Raises:
        DatabaseConnectionError: If connection or cursor creation fails.
    
    Example:
        >>> config = Config('config.yaml')
        >>> with get_db_connection(config) as (conn, cursor):
        ...     cursor.execute("SELECT 1")
        ...     result = cursor.fetchone()
    """
    pool = DatabaseConnectionPool()
    connection = None
    cursor = None
    
    try:
        # Get database configuration
        db_config = config.config.get('database', {})
        if not db_config:
            raise DatabaseConnectionError("No database configuration found")
        
        # Initialize pool if needed
        pool.initialize(
            host=db_config.get('host', 'localhost'),
            port=db_config.get('port', 5432),
            database=db_config.get('database', 'postgres'),
            username=db_config.get('username', 'postgres'),
            password=db_config.get('password', '')
        )
        
        # Get connection and cursor
        connection = pool.get_connection()
        cursor = connection.cursor()
        
        logger.debug("Database connection and cursor established")
        
        yield connection, cursor
        
        # Commit if no exception occurred
        connection.commit()
        logger.debug("Transaction committed successfully")
        
    except psycopg2.Error as exc:
        # Rollback on database errors
        if connection:
            connection.rollback()
            logger.warning(f"Transaction rolled back due to error: {exc}")
        raise DatabaseConnectionError(f"Database operation failed: {exc}") from exc
    
    except Exception as exc:
        # Rollback on any other errors
        if connection:
            connection.rollback()
            logger.error(f"Unexpected error, transaction rolled back: {exc}")
        raise
    
    finally:
        # Clean up resources
        if cursor:
            try:
                cursor.close()
                logger.debug("Cursor closed")
            except psycopg2.Error as exc:
                logger.warning(f"Error closing cursor: {exc}")
        
        if connection:
            pool.return_connection(connection)
            logger.debug("Connection returned to pool")


def with_database(
    config: Config,
    autocommit: bool = False,
    pass_connection: bool = False,
    pass_cursor: bool = True
) -> Callable[[F], F]:
    """
    Decorator that provides database connection and cursor to wrapped function.
    
    This decorator automatically handles connection acquisition, cursor creation,
    transaction management, and resource cleanup. The decorated function receives
    connection and/or cursor as keyword arguments.
    
    Args:
        config: Configuration object containing database settings.
        autocommit: If True, commit after function execution. If False, function
                   must handle commits explicitly (default: False).
        pass_connection: If True, pass connection as 'connection' kwarg (default: False).
        pass_cursor: If True, pass cursor as 'cursor' kwarg (default: True).
    
    Returns:
        Decorated function with database connection management.
    
    Raises:
        DatabaseConnectionError: If connection or cursor creation fails.
    
    Example:
        >>> config = Config('config.yaml')
        >>> @with_database(config, autocommit=True)
        ... def get_users(cursor=None):
        ...     cursor.execute("SELECT * FROM users")
        ...     return cursor.fetchall()
        >>> users = get_users()
    
    Notes:
        - Uses connection pooling for better performance
        - Automatically rolls back on exceptions
        - Thread-safe for concurrent execution
        - Connection and cursor are injected as keyword arguments
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            """Wrapper function that manages database connection lifecycle."""
            pool = DatabaseConnectionPool()
            connection = None
            cursor = None
            
            try:
                # Get database configuration
                db_config = config.config.get('database', {})
                if not db_config:
                    raise DatabaseConnectionError("No database configuration found")
                
                # Initialize pool if needed
                pool.initialize(
                    host=db_config.get('host', 'localhost'),
                    port=db_config.get('port', 5432),
                    database=db_config.get('database', 'postgres'),
                    username=db_config.get('username', 'postgres'),
                    password=db_config.get('password', '')
                )
                
                # Get connection and cursor
                connection = pool.get_connection()
                cursor = connection.cursor()
                
                # Inject connection and/or cursor into kwargs
                if pass_connection:
                    kwargs['connection'] = connection
                if pass_cursor:
                    kwargs['cursor'] = cursor
                
                logger.debug(f"Executing function '{func.__qualname__}' with database connection")
                
                # Execute the wrapped function
                result = func(*args, **kwargs)
                
                # Commit if autocommit is enabled
                if autocommit:
                    connection.commit()
                    logger.debug(f"Transaction auto-committed for '{func.__qualname__}'")
                
                return result
                
            except psycopg2.Error as exc:
                # Rollback on database errors
                if connection:
                    connection.rollback()
                    logger.error(
                        f"Database error in '{func.__qualname__}', rolled back: {exc}"
                    )
                raise DatabaseConnectionError(
                    f"Database operation failed in '{func.__qualname__}': {exc}"
                ) from exc
            
            except Exception as exc:
                # Rollback on any other errors
                if connection:
                    connection.rollback()
                    logger.error(
                        f"Error in '{func.__qualname__}', rolled back: {exc}"
                    )
                raise
            
            finally:
                # Clean up resources
                if cursor:
                    try:
                        cursor.close()
                    except psycopg2.Error as exc:
                        logger.warning(f"Error closing cursor: {exc}")
                
                if connection:
                    pool.return_connection(connection)
        
        return cast(F, wrapper)
    
    return decorator