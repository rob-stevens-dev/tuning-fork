"""
Unit tests for db_connection module (PostgreSQL).

This test suite ensures comprehensive coverage of the database connection
decorator and connection pool management, using mocks to avoid actual database
connections during testing.
"""

import unittest
from unittest.mock import MagicMock, Mock, patch
from typing import Any

import psycopg2

from tuning_fork.config import Config
from tuning_fork.shared.decorators.db_connection import (
    DatabaseConnectionError,
    DatabaseConnectionPool,
    get_db_connection,
    with_database,
)


class TestDatabaseConnectionPool(unittest.TestCase):
    """Test suite for DatabaseConnectionPool singleton class."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        # Reset singleton for each test
        DatabaseConnectionPool._instance = None
        DatabaseConnectionPool._pg_pool = None
        DatabaseConnectionPool._mysql_pool = None
        DatabaseConnectionPool._db_type = None
    
    def tearDown(self) -> None:
        """Clean up test fixtures."""
        pool = DatabaseConnectionPool()
        if pool._pg_pool:
            pool.close_all()
        DatabaseConnectionPool._instance = None
        DatabaseConnectionPool._pg_pool = None
        DatabaseConnectionPool._mysql_pool = None
        DatabaseConnectionPool._db_type = None
    
    def test_singleton_pattern(self) -> None:
        """Test that DatabaseConnectionPool is a singleton."""
        pool1 = DatabaseConnectionPool()
        pool2 = DatabaseConnectionPool()
        
        self.assertIs(pool1, pool2)
    
    @patch('tuning_fork.shared.decorators.db_connection.pg_pool.ThreadedConnectionPool')
    def test_initialize_pool_success(self, mock_pool_class: Mock) -> None:
        """Test successful pool initialization."""
        mock_pool = MagicMock()
        mock_pool_class.return_value = mock_pool
        
        pool = DatabaseConnectionPool()
        pool.initialize_postgresql(
            host='localhost',
            port=5432,
            database='testdb',
            username='testuser',
            password='testpass'
        )
        
        mock_pool_class.assert_called_once_with(
            1, 10,
            host='localhost',
            port=5432,
            database='testdb',
            user='testuser',
            password='testpass',
            connect_timeout=10
        )
        self.assertIsNotNone(pool._pg_pool)
    
    @patch('tuning_fork.shared.decorators.db_connection.pg_pool.ThreadedConnectionPool')
    def test_initialize_pool_with_custom_connections(self, mock_pool_class: Mock) -> None:
        """Test pool initialization with custom min/max connections."""
        mock_pool = MagicMock()
        mock_pool_class.return_value = mock_pool
        
        pool = DatabaseConnectionPool()
        pool.initialize_postgresql(
            host='localhost',
            port=5432,
            database='testdb',
            username='testuser',
            password='testpass',
            min_connections=5,
            max_connections=20
        )
        
        mock_pool_class.assert_called_once()
        call_args = mock_pool_class.call_args
        self.assertEqual(call_args[0][0], 5)  # min_connections
        self.assertEqual(call_args[0][1], 20)  # max_connections
    
    @patch('tuning_fork.shared.decorators.db_connection.pg_pool.ThreadedConnectionPool')
    def test_initialize_pool_already_initialized(self, mock_pool_class: Mock) -> None:
        """Test that re-initialization is skipped if pool exists."""
        mock_pool = MagicMock()
        mock_pool_class.return_value = mock_pool
        
        pool = DatabaseConnectionPool()
        pool.initialize_postgresql(
            host='localhost',
            port=5432,
            database='testdb',
            username='testuser',
            password='testpass'
        )
        
        # Call initialize again
        pool.initialize_postgresql(
            host='localhost',
            port=5432,
            database='testdb',
            username='testuser',
            password='testpass'
        )
        
        # Should only be called once
        mock_pool_class.assert_called_once()
    
    @patch('tuning_fork.shared.decorators.db_connection.pg_pool.ThreadedConnectionPool')
    def test_initialize_pool_failure(self, mock_pool_class: Mock) -> None:
        """Test pool initialization failure handling."""
        mock_pool_class.side_effect = psycopg2.OperationalError("Connection failed")
        
        pool = DatabaseConnectionPool()
        
        with self.assertRaises(DatabaseConnectionError) as context:
            pool.initialize_postgresql(
                host='localhost',
                port=5432,
                database='testdb',
                username='testuser',
                password='testpass'
            )
        
        self.assertIn("Failed to initialize", str(context.exception))
    
    @patch('tuning_fork.shared.decorators.db_connection.pg_pool.ThreadedConnectionPool')
    def test_get_connection_success(self, mock_pool_class: Mock) -> None:
        """Test successful connection retrieval from pool."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_pool.getconn.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        pool = DatabaseConnectionPool()
        pool.initialize_postgresql(
            host='localhost',
            port=5432,
            database='testdb',
            username='testuser',
            password='testpass'
        )
        
        connection = pool.get_postgresql_connection()
        
        self.assertEqual(connection, mock_connection)
        mock_pool.getconn.assert_called_once()
    
    def test_get_connection_pool_not_initialized(self) -> None:
        """Test getting connection when pool is not initialized."""
        pool = DatabaseConnectionPool()
        
        with self.assertRaises(DatabaseConnectionError) as context:
            pool.get_postgresql_connection()
        
        self.assertIn("not initialized", str(context.exception))
    
    @patch('tuning_fork.shared.decorators.db_connection.pg_pool.ThreadedConnectionPool')
    def test_get_connection_failure(self, mock_pool_class: Mock) -> None:
        """Test connection retrieval failure."""
        mock_pool = MagicMock()
        mock_pool.getconn.side_effect = psycopg2.OperationalError("No connections available")
        mock_pool_class.return_value = mock_pool
        
        pool = DatabaseConnectionPool()
        pool.initialize_postgresql(
            host='localhost',
            port=5432,
            database='testdb',
            username='testuser',
            password='testpass'
        )
        
        with self.assertRaises(DatabaseConnectionError) as context:
            pool.get_postgresql_connection()
        
        self.assertIn("Failed to get", str(context.exception))
    
    @patch('tuning_fork.shared.decorators.db_connection.pg_pool.ThreadedConnectionPool')
    def test_return_connection_success(self, mock_pool_class: Mock) -> None:
        """Test successful connection return to pool."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_pool_class.return_value = mock_pool
        
        pool = DatabaseConnectionPool()
        pool.initialize_postgresql(
            host='localhost',
            port=5432,
            database='testdb',
            username='testuser',
            password='testpass'
        )
        
        pool.return_postgresql_connection(mock_connection)
        
        mock_pool.putconn.assert_called_once_with(mock_connection)
    
    @patch('tuning_fork.shared.decorators.db_connection.pg_pool.ThreadedConnectionPool')
    def test_return_connection_with_close(self, mock_pool_class: Mock) -> None:
        """Test returning connection with close flag."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_pool_class.return_value = mock_pool
        
        pool = DatabaseConnectionPool()
        pool.initialize_postgresql(
            host='localhost',
            port=5432,
            database='testdb',
            username='testuser',
            password='testpass'
        )
        
        pool.return_postgresql_connection(mock_connection, close=True)
        
        mock_pool.putconn.assert_called_once_with(mock_connection, close=True)
    
    def test_return_connection_pool_not_initialized(self) -> None:
        """Test returning connection when pool is not initialized."""
        pool = DatabaseConnectionPool()
        mock_connection = MagicMock()
        
        # Should not raise exception, just log warning
        pool.return_postgresql_connection(mock_connection)
    
    @patch('tuning_fork.shared.decorators.db_connection.pg_pool.ThreadedConnectionPool')
    def test_close_all_connections(self, mock_pool_class: Mock) -> None:
        """Test closing all connections in pool."""
        mock_pool = MagicMock()
        mock_pool_class.return_value = mock_pool
        
        pool = DatabaseConnectionPool()
        pool.initialize_postgresql(
            host='localhost',
            port=5432,
            database='testdb',
            username='testuser',
            password='testpass'
        )
        
        pool.close_all()
        
        mock_pool.closeall.assert_called_once()
        self.assertIsNone(pool._pg_pool)
    
    @patch('tuning_fork.shared.decorators.db_connection.pg_pool.ThreadedConnectionPool')
    def test_return_connection_with_error(self, mock_pool_class: Mock) -> None:
        """Test returning connection with error."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_pool.putconn.side_effect = psycopg2.Error("Error returning connection")
        mock_pool_class.return_value = mock_pool
        
        pool = DatabaseConnectionPool()
        pool.initialize_postgresql(
            host='localhost',
            port=5432,
            database='testdb',
            username='testuser',
            password='testpass'
        )
        
        # Should not raise exception, just log warning
        pool.return_postgresql_connection(mock_connection)


class TestGetDbConnection(unittest.TestCase):
    """Test suite for get_db_connection context manager."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        DatabaseConnectionPool._instance = None
        DatabaseConnectionPool._pg_pool = None
        DatabaseConnectionPool._mysql_pool = None
        DatabaseConnectionPool._db_type = None
        
        # Create mock config
        self.mock_config = MagicMock(spec=Config)
        self.mock_config.config = {
            'database': {
                'type': 'postgresql',
                'host': 'localhost',
                'port': 5432,
                'database': 'testdb',
                'username': 'testuser',
                'password': 'testpass'
            }
        }
    
    def tearDown(self) -> None:
        """Clean up test fixtures."""
        pool = DatabaseConnectionPool()
        if pool._pg_pool:
            pool.close_all()
        DatabaseConnectionPool._instance = None
        DatabaseConnectionPool._pg_pool = None
        DatabaseConnectionPool._mysql_pool = None
        DatabaseConnectionPool._db_type = None
    
    @patch('tuning_fork.shared.decorators.db_connection.pg_pool.ThreadedConnectionPool')
    def test_context_manager_success(self, mock_pool_class: Mock) -> None:
        """Test successful context manager usage."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.getconn.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        with get_db_connection(self.mock_config) as (conn, cursor):
            self.assertEqual(conn, mock_connection)
            self.assertEqual(cursor, mock_cursor)
        
        mock_connection.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_pool.putconn.assert_called_once_with(mock_connection)
    
    @patch('tuning_fork.shared.decorators.db_connection.pg_pool.ThreadedConnectionPool')
    def test_context_manager_with_exception(self, mock_pool_class: Mock) -> None:
        """Test context manager rollback on exception."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.getconn.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        with self.assertRaises(DatabaseConnectionError):
            with get_db_connection(self.mock_config) as (conn, cursor):
                raise ValueError("Test error")
        
        mock_connection.rollback.assert_called_once()
        mock_connection.commit.assert_not_called()
        mock_cursor.close.assert_called_once()
    
    @patch('tuning_fork.shared.decorators.db_connection.pg_pool.ThreadedConnectionPool')
    def test_context_manager_with_database_exception(self, mock_pool_class: Mock) -> None:
        """Test context manager rollback on database exception."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.getconn.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        with self.assertRaises(DatabaseConnectionError):
            with get_db_connection(self.mock_config) as (conn, cursor):
                raise psycopg2.DatabaseError("Test database error")
        
        mock_connection.rollback.assert_called_once()
        mock_connection.commit.assert_not_called()
        mock_cursor.close.assert_called_once()
    
    def test_context_manager_no_database_config(self) -> None:
        """Test context manager with missing database configuration."""
        self.mock_config.config = {}
        
        with self.assertRaises(DatabaseConnectionError) as context:
            with get_db_connection(self.mock_config) as (conn, cursor):
                pass
        
        self.assertIn("No database configuration found", str(context.exception))


class TestWithDatabaseDecorator(unittest.TestCase):
    """Test suite for with_database decorator."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        DatabaseConnectionPool._instance = None
        DatabaseConnectionPool._pg_pool = None
        DatabaseConnectionPool._mysql_pool = None
        DatabaseConnectionPool._db_type = None
        
        # Create mock config
        self.mock_config = MagicMock(spec=Config)
        self.mock_config.config = {
            'database': {
                'type': 'postgresql',
                'host': 'localhost',
                'port': 5432,
                'database': 'testdb',
                'username': 'testuser',
                'password': 'testpass'
            }
        }
    
    def tearDown(self) -> None:
        """Clean up test fixtures."""
        pool = DatabaseConnectionPool()
        if pool._pg_pool:
            pool.close_all()
        DatabaseConnectionPool._instance = None
        DatabaseConnectionPool._pg_pool = None
        DatabaseConnectionPool._mysql_pool = None
        DatabaseConnectionPool._db_type = None
    
    @patch('tuning_fork.shared.decorators.db_connection.pg_pool.ThreadedConnectionPool')
    def test_decorator_basic_usage(self, mock_pool_class: Mock) -> None:
        """Test basic decorator usage with cursor injection."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.getconn.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        @with_database(self.mock_config, pass_cursor=True, pass_connection=False)
        def test_function(cursor=None) -> str:
            self.assertIsNotNone(cursor)
            return "success"
        
        result = test_function()
        
        self.assertEqual(result, "success")
        mock_cursor.close.assert_called_once()
        mock_pool.putconn.assert_called_once()
    
    @patch('tuning_fork.shared.decorators.db_connection.pg_pool.ThreadedConnectionPool')
    def test_decorator_with_autocommit(self, mock_pool_class: Mock) -> None:
        """Test decorator with autocommit enabled."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.getconn.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        @with_database(self.mock_config, autocommit=True, pass_cursor=True, pass_connection=False)
        def test_function(cursor=None) -> str:
            return "success"
        
        result = test_function()
        
        self.assertEqual(result, "success")
        mock_connection.commit.assert_called_once()
    
    @patch('tuning_fork.shared.decorators.db_connection.pg_pool.ThreadedConnectionPool')
    def test_decorator_pass_connection_and_cursor(self, mock_pool_class: Mock) -> None:
        """Test decorator passing both connection and cursor."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.getconn.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        @with_database(self.mock_config, pass_connection=True, pass_cursor=True)
        def test_function(connection=None, cursor=None) -> str:
            self.assertIsNotNone(connection)
            self.assertIsNotNone(cursor)
            return "success"
        
        result = test_function()
        self.assertEqual(result, "success")
    
    @patch('tuning_fork.shared.decorators.db_connection.pg_pool.ThreadedConnectionPool')
    def test_decorator_pass_only_connection(self, mock_pool_class: Mock) -> None:
        """Test decorator passing only connection."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.getconn.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        @with_database(self.mock_config, pass_connection=True, pass_cursor=False)
        def test_function(connection=None, cursor=None) -> str:
            self.assertIsNotNone(connection)
            self.assertIsNone(cursor)
            return "success"
        
        result = test_function()
        self.assertEqual(result, "success")
    
    @patch('tuning_fork.shared.decorators.db_connection.pg_pool.ThreadedConnectionPool')
    def test_decorator_with_function_args(self, mock_pool_class: Mock) -> None:
        """Test decorator with function arguments."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.getconn.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        @with_database(self.mock_config, pass_cursor=True, pass_connection=False)
        def test_function(x: int, y: str, cursor=None) -> str:
            return f"{x}-{y}"
        
        result = test_function(42, "test")
        self.assertEqual(result, "42-test")
    
    @patch('tuning_fork.shared.decorators.db_connection.pg_pool.ThreadedConnectionPool')
    def test_decorator_with_exception(self, mock_pool_class: Mock) -> None:
        """Test decorator rollback on exception."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.getconn.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        @with_database(self.mock_config, autocommit=True, pass_cursor=True, pass_connection=False)
        def test_function(cursor=None) -> None:
            raise ValueError("Test error")
        
        with self.assertRaises(DatabaseConnectionError):
            test_function()
        
        mock_connection.rollback.assert_called_once()
        mock_connection.commit.assert_not_called()
    
    @patch('tuning_fork.shared.decorators.db_connection.pg_pool.ThreadedConnectionPool')
    def test_decorator_with_database_error(self, mock_pool_class: Mock) -> None:
        """Test decorator handling of database errors."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.getconn.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        @with_database(self.mock_config, pass_cursor=True, pass_connection=False)
        def test_function(cursor=None) -> None:
            raise psycopg2.DatabaseError("Database error")
        
        with self.assertRaises(DatabaseConnectionError):
            test_function()
        
        mock_connection.rollback.assert_called_once()
    
    @patch('tuning_fork.shared.decorators.db_connection.pg_pool.ThreadedConnectionPool')
    def test_decorator_preserves_function_metadata(self, mock_pool_class: Mock) -> None:
        """Test that decorator preserves original function metadata."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.getconn.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        @with_database(self.mock_config, pass_cursor=True, pass_connection=False)
        def documented_function(cursor=None) -> str:
            """This is a test function."""
            return "test"
        
        self.assertEqual(documented_function.__name__, "documented_function")
        self.assertEqual(documented_function.__doc__, "This is a test function.")
    
    def test_decorator_no_database_config(self) -> None:
        """Test decorator with missing database configuration."""
        self.mock_config.config = {}
        
        @with_database(self.mock_config, pass_cursor=True, pass_connection=False)
        def test_function(cursor=None) -> str:
            return "success"
        
        with self.assertRaises(DatabaseConnectionError) as context:
            test_function()
        
        self.assertIn("No database configuration found", str(context.exception))
    
    @patch('tuning_fork.shared.decorators.db_connection.pg_pool.ThreadedConnectionPool')
    def test_decorator_with_return_value(self, mock_pool_class: Mock) -> None:
        """Test that decorator properly returns function result."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.getconn.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        @with_database(self.mock_config, pass_cursor=True, pass_connection=False)
        def test_function(cursor=None) -> dict[str, Any]:
            return {"status": "ok", "data": [1, 2, 3]}
        
        result = test_function()
        self.assertEqual(result, {"status": "ok", "data": [1, 2, 3]})
    
    @patch('tuning_fork.shared.decorators.db_connection.pg_pool.ThreadedConnectionPool')
    def test_decorator_cursor_close_error(self, mock_pool_class: Mock) -> None:
        """Test decorator handles cursor close errors gracefully."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.close.side_effect = psycopg2.Error("Error closing cursor")
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.getconn.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        @with_database(self.mock_config, pass_cursor=True, pass_connection=False)
        def test_function(cursor=None) -> str:
            return "success"
        
        # Should complete successfully despite cursor close error
        result = test_function()
        self.assertEqual(result, "success")


if __name__ == '__main__':
    unittest.main()