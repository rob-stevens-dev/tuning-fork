"""
Database connection decorator module.

This module provides decorators for automatic database connection and cursor
management with proper resource cleanup and error handling. Supports PostgreSQL
and MySQL/MariaDB.
"""

import functools
import logging
from contextlib import contextmanager
from typing import Any, Callable, Optional, TypeVar, Union, cast

import psycopg2
import psycopg2.extensions
from psycopg2 import pool as pg_pool

try:
    import mysql.connector
    from mysql.connector import pooling as mysql_pool
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False
    mysql = None
    mysql_pool = None

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
    Singleton connection pool manager for PostgreSQL and MySQL/MariaDB databases.
    
    This class manages connection pools to avoid creating new connections
    for every decorated function call, improving performance significantly.
    """
    
    _instance: Optional['DatabaseConnectionPool'] = None
    _pg_pool: Optional[pg_pool.ThreadedConnectionPool] = None
    _mysql_pool: Optional[Any] = None  # mysql.connector.pooling.MySQLConnectionPool
    _db_type: Optional[str] = None
    
    def __new__(cls) -> 'DatabaseConnectionPool':
        """Ensure only one instance of the connection pool exists."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def initialize_postgresql(
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
        Initialize PostgreSQL connection pool with database credentials.
        
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
        if self._pg_pool is not None:
            logger.debug("PostgreSQL connection pool already initialized")
            return
        
        try:
            self._pg_pool = pg_pool.ThreadedConnectionPool(
                min_connections,
                max_connections,
                host=host,
                port=port,
                database=database,
                user=username,
                password=password,
                connect_timeout=10
            )
            self._db_type = 'postgresql'
            logger.info(
                f"PostgreSQL connection pool initialized: {username}@{host}:{port}/{database}"
            )
        except psycopg2.Error as exc:
            raise DatabaseConnectionError(
                f"Failed to initialize PostgreSQL connection pool: {exc}"
            ) from exc
    
    def initialize_mysql(
        self,
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        pool_size: int = 10
    ) -> None:
        """
        Initialize MySQL/MariaDB connection pool with database credentials.
        
        Args:
            host: Database host address.
            port: Database port number.
            database: Database name.
            username: Database username.
            password: Database password.
            pool_size: Number of connections in pool.
        
        Raises:
            DatabaseConnectionError: If pool initialization fails or MySQL not installed.
        """
        if not MYSQL_AVAILABLE:
            raise DatabaseConnectionError(
                "MySQL support not available. Install mysql-connector-python: "
                "pip install mysql-connector-python"
            )
        
        if self._mysql_pool is not None:
            logger.debug("MySQL connection pool already initialized")
            return
        
        try:
            self._mysql_pool = mysql_pool.MySQLConnectionPool(
                pool_name="tuning_fork_mysql_pool",
                pool_size=pool_size,
                pool_reset_session=True,
                host=host,
                port=port,
                database=database,
                user=username,
                password=password,
                connect_timeout=10,
                autocommit=False
            )
            self._db_type = 'mysql'
            logger.info(
                f"MySQL connection pool initialized: {username}@{host}:{port}/{database}"
            )
        except mysql.connector.Error as exc:
            raise DatabaseConnectionError(
                f"Failed to initialize MySQL connection pool: {exc}"
            ) from exc
    
    def get_postgresql_connection(self) -> psycopg2.extensions.connection:
        """
        Get a PostgreSQL connection from the pool.
        
        Returns:
            Database connection object.
        
        Raises:
            DatabaseConnectionError: If pool is not initialized or connection fails.
        """
        if self._pg_pool is None:
            raise DatabaseConnectionError(
                "PostgreSQL connection pool not initialized. Call initialize_postgresql() first."
            )
        
        try:
            return self._pg_pool.getconn()
        except psycopg2.Error as exc:
            raise DatabaseConnectionError(
                f"Failed to get PostgreSQL connection from pool: {exc}"
            ) from exc
    
    def get_mysql_connection(self) -> Any:
        """
        Get a MySQL connection from the pool.
        
        Returns:
            Database connection object.
        
        Raises:
            DatabaseConnectionError: If pool is not initialized or connection fails.
        """
        if self._mysql_pool is None:
            raise DatabaseConnectionError(
                "MySQL connection pool not initialized. Call initialize_mysql() first."
            )
        
        try:
            return self._mysql_pool.get_connection()
        except mysql.connector.Error as exc:
            raise DatabaseConnectionError(
                f"Failed to get MySQL connection from pool: {exc}"
            ) from exc
    
    def return_postgresql_connection(
        self,
        connection: psycopg2.extensions.connection,
        close: bool = False
    ) -> None:
        """
        Return a PostgreSQL connection to the pool.
        
        Args:
            connection: Database connection to return.
            close: If True, close the connection instead of returning to pool.
        """
        if self._pg_pool is None:
            logger.warning("Attempting to return connection to uninitialized PostgreSQL pool")
            return
        
        try:
            if close:
                self._pg_pool.putconn(connection, close=True)
            else:
                self._pg_pool.putconn(connection)
        except psycopg2.Error as exc:
            logger.error(f"Error returning PostgreSQL connection to pool: {exc}")
    
    def return_mysql_connection(self, connection: Any) -> None:
        """
        Return a MySQL connection to the pool.
        
        Args:
            connection: Database connection to return.
        """
        if self._mysql_pool is None:
            logger.warning("Attempting to return connection to uninitialized MySQL pool")
            return
        
        try:
            connection.close()  # mysql-connector-python pool handles return on close
        except mysql.connector.Error as exc:
            logger.error(f"Error returning MySQL connection to pool: {exc}")
    
    def close_all(self) -> None:
        """Close all connections in all pools."""
        if self._pg_pool is not None:
            self._pg_pool.closeall()
            self._pg_pool = None
            logger.info("All PostgreSQL connections closed")
        
        if self._mysql_pool is not None:
            # MySQL pool doesn't have a closeall method, but setting to None
            # will let garbage collection handle it
            self._mysql_pool = None
            logger.info("MySQL connection pool cleared")
        
        self._db_type = None


@contextmanager
def get_db_connection(config: Config):
    """
    Context manager for database connections with automatic cleanup.
    
    Supports PostgreSQL and MySQL/MariaDB based on config['database']['type'].
    
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
        
        db_type = db_config.get('type', 'postgresql').lower()
        host = db_config.get('host', 'localhost')
        port = db_config.get('port')
        database = db_config.get('database')
        username = db_config.get('username')
        password = db_config.get('password', '')
        
        # Set default ports if not specified
        if port is None:
            port = 5432 if db_type == 'postgresql' else 3306
        
        # Initialize appropriate pool and get connection
        if db_type == 'postgresql':
            pool.initialize_postgresql(
                host=host,
                port=port,
                database=database,
                username=username,
                password=password
            )
            connection = pool.get_postgresql_connection()
            cursor = connection.cursor()
            
        elif db_type in ('mysql', 'mariadb'):
            pool.initialize_mysql(
                host=host,
                port=port,
                database=database,
                username=username,
                password=password
            )
            connection = pool.get_mysql_connection()
            cursor = connection.cursor()
            
        else:
            raise DatabaseConnectionError(f"Unsupported database type: {db_type}")
        
        logger.debug(f"Database connection and cursor established ({db_type})")
        
        yield connection, cursor
        
        # Commit if no exception occurred
        connection.commit()
        logger.debug("Transaction committed successfully")
        
    except (psycopg2.Error, Exception) as exc:
        # Handle both PostgreSQL and MySQL errors
        if connection:
            connection.rollback()
            logger.warning(f"Transaction rolled back due to error: {exc}")
        
        # Re-raise as DatabaseConnectionError for consistency
        if not isinstance(exc, DatabaseConnectionError):
            raise DatabaseConnectionError(f"Database operation failed: {exc}") from exc
        raise
    
    finally:
        # Clean up resources
        if cursor:
            try:
                cursor.close()
                logger.debug("Cursor closed")
            except Exception as exc:
                logger.warning(f"Error closing cursor: {exc}")
        
        if connection:
            db_type = db_config.get('type', 'postgresql').lower()
            if db_type == 'postgresql':
                pool.return_postgresql_connection(connection)
            elif db_type in ('mysql', 'mariadb'):
                pool.return_mysql_connection(connection)
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
    transaction management, and resource cleanup. Works with both PostgreSQL
    and MySQL/MariaDB based on config['database']['type'].
    
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
        >>> config = Config('config.yaml')  # type: postgresql or mysql
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
        - Supports both PostgreSQL and MySQL/MariaDB
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
                
                db_type = db_config.get('type', 'postgresql').lower()
                host = db_config.get('host', 'localhost')
                port = db_config.get('port')
                database = db_config.get('database')
                username = db_config.get('username')
                password = db_config.get('password', '')
                
                # Set default ports if not specified
                if port is None:
                    port = 5432 if db_type == 'postgresql' else 3306
                
                # Initialize appropriate pool and get connection
                if db_type == 'postgresql':
                    pool.initialize_postgresql(
                        host=host,
                        port=port,
                        database=database,
                        username=username,
                        password=password
                    )
                    connection = pool.get_postgresql_connection()
                    
                elif db_type in ('mysql', 'mariadb'):
                    pool.initialize_mysql(
                        host=host,
                        port=port,
                        database=database,
                        username=username,
                        password=password
                    )
                    connection = pool.get_mysql_connection()
                    
                else:
                    raise DatabaseConnectionError(f"Unsupported database type: {db_type}")
                
                cursor = connection.cursor()
                
                # Inject connection and/or cursor into kwargs
                if pass_connection:
                    kwargs['connection'] = connection
                if pass_cursor:
                    kwargs['cursor'] = cursor
                
                logger.debug(
                    f"Executing function '{func.__qualname__}' with {db_type} connection"
                )
                
                # Execute the wrapped function
                result = func(*args, **kwargs)
                
                # Commit if autocommit is enabled
                if autocommit:
                    connection.commit()
                    logger.debug(f"Transaction auto-committed for '{func.__qualname__}'")
                
                return result
                
            except Exception as exc:
                # Rollback on any errors
                if connection:
                    try:
                        connection.rollback()
                        logger.error(
                            f"Error in '{func.__qualname__}', rolled back: {exc}"
                        )
                    except Exception as rollback_exc:
                        logger.error(f"Rollback failed: {rollback_exc}")
                
                # Re-raise as DatabaseConnectionError for consistency
                if not isinstance(exc, DatabaseConnectionError):
                    raise DatabaseConnectionError(
                        f"Database operation failed in '{func.__qualname__}': {exc}"
                    ) from exc
                raise
            
            finally:
                # Clean up resources
                if cursor:
                    try:
                        cursor.close()
                    except Exception as exc:
                        logger.warning(f"Error closing cursor: {exc}")
                
                if connection:
                    db_type = db_config.get('type', 'postgresql').lower()
                    if db_type == 'postgresql':
                        pool.return_postgresql_connection(connection)
                    elif db_type in ('mysql', 'mariadb'):
                        pool.return_mysql_connection(connection)
        
        return cast(F, wrapper)
    
    return decorator