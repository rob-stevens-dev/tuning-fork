"""
Unit tests for MySQL/MariaDB support in db_connection module.

This test suite ensures comprehensive coverage of the MySQL/MariaDB database
connection decorator and connection pool management, using mocks to avoid
actual database connections during testing.
"""

import unittest
from unittest.mock import MagicMock, Mock, patch, call
from typing import Any

from tuning_fork.config import Config
from tuning_fork.shared.decorators.db_connection import (
    DatabaseConnectionError,
    DatabaseConnectionPool,
    get_db_connection,
    with_database,
    MYSQL_AVAILABLE,
)


@unittest.skipIf(not MYSQL_AVAILABLE, "MySQL connector not installed")
class TestDatabaseConnectionPoolMySQL(unittest.TestCase):
    """Test suite for MySQL/MariaDB in DatabaseConnectionPool."""
    
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
        if pool._pg_pool or pool._mysql_pool:
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
    
    @patch('tuning_fork.shared.decorators.db_connection.mysql_pool.MySQLConnectionPool')
    def test_initialize_mysql_pool_success(self, mock_pool_class: Mock) -> None:
        """Test successful MySQL pool initialization."""
        mock_pool = MagicMock()
        mock_pool_class.return_value = mock_pool
        
        pool = DatabaseConnectionPool()
        pool.initialize_mysql(
            host='localhost',
            port=3306,
            database='testdb',
            username='testuser',
            password='testpass'
        )
        
        mock_pool_class.assert_called_once_with(
            pool_name='tuning_fork_mysql_pool',
            pool_size=10,
            pool_reset_session=True,
            host='localhost',
            port=3306,
            database='testdb',
            user='testuser',
            password='testpass',
            connect_timeout=10,
            autocommit=False
        )
        self.assertIsNotNone(pool._mysql_pool)
        self.assertEqual(pool._db_type, 'mysql')
    
    @patch('tuning_fork.shared.decorators.db_connection.mysql_pool.MySQLConnectionPool')
    def test_initialize_mysql_pool_custom_size(self, mock_pool_class: Mock) -> None:
        """Test MySQL pool initialization with custom pool size."""
        mock_pool = MagicMock()
        mock_pool_class.return_value = mock_pool
        
        pool = DatabaseConnectionPool()
        pool.initialize_mysql(
            host='localhost',
            port=3306,
            database='testdb',
            username='testuser',
            password='testpass',
            pool_size=20
        )
        
        call_kwargs = mock_pool_class.call_args[1]
        self.assertEqual(call_kwargs['pool_size'], 20)
    
    @patch('tuning_fork.shared.decorators.db_connection.mysql_pool.MySQLConnectionPool')
    def test_initialize_mysql_pool_already_initialized(self, mock_pool_class: Mock) -> None:
        """Test that re-initialization is skipped if MySQL pool exists."""
        mock_pool = MagicMock()
        mock_pool_class.return_value = mock_pool
        
        pool = DatabaseConnectionPool()
        pool.initialize_mysql(
            host='localhost',
            port=3306,
            database='testdb',
            username='testuser',
            password='testpass'
        )
        
        # Call initialize again
        pool.initialize_mysql(
            host='localhost',
            port=3306,
            database='testdb',
            username='testuser',
            password='testpass'
        )
        
        # Should only be called once
        mock_pool_class.assert_called_once()
    
    @patch('tuning_fork.shared.decorators.db_connection.mysql_pool.MySQLConnectionPool')
    @patch('tuning_fork.shared.decorators.db_connection.mysql.connector.Error', Exception)
    def test_initialize_mysql_pool_failure(self, mock_pool_class: Mock) -> None:
        """Test MySQL pool initialization failure handling."""
        import mysql.connector
        mock_pool_class.side_effect = mysql.connector.Error("Connection failed")
        
        pool = DatabaseConnectionPool()
        
        with self.assertRaises(DatabaseConnectionError) as context:
            pool.initialize_mysql(
                host='localhost',
                port=3306,
                database='testdb',
                username='testuser',
                password='testpass'
            )
        
        self.assertIn("Failed to initialize MySQL connection pool", str(context.exception))
    
    @patch('tuning_fork.shared.decorators.db_connection.mysql_pool.MySQLConnectionPool')
    def test_get_mysql_connection_success(self, mock_pool_class: Mock) -> None:
        """Test successful MySQL connection retrieval from pool."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        pool = DatabaseConnectionPool()
        pool.initialize_mysql(
            host='localhost',
            port=3306,
            database='testdb',
            username='testuser',
            password='testpass'
        )
        
        connection = pool.get_mysql_connection()
        
        self.assertEqual(connection, mock_connection)
        mock_pool.get_connection.assert_called_once()
    
    def test_get_mysql_connection_pool_not_initialized(self) -> None:
        """Test getting MySQL connection when pool is not initialized."""
        pool = DatabaseConnectionPool()
        
        with self.assertRaises(DatabaseConnectionError) as context:
            pool.get_mysql_connection()
        
        self.assertIn("MySQL connection pool not initialized", str(context.exception))
    
    @patch('tuning_fork.shared.decorators.db_connection.mysql_pool.MySQLConnectionPool')
    @patch('tuning_fork.shared.decorators.db_connection.mysql.connector.Error', Exception)
    def test_get_mysql_connection_failure(self, mock_pool_class: Mock) -> None:
        """Test MySQL connection retrieval failure."""
        import mysql.connector
        mock_pool = MagicMock()
        mock_pool.get_connection.side_effect = mysql.connector.Error("No connections available")
        mock_pool_class.return_value = mock_pool
        
        pool = DatabaseConnectionPool()
        pool.initialize_mysql(
            host='localhost',
            port=3306,
            database='testdb',
            username='testuser',
            password='testpass'
        )
        
        with self.assertRaises(DatabaseConnectionError) as context:
            pool.get_mysql_connection()
        
        self.assertIn("Failed to get MySQL connection from pool", str(context.exception))
    
    @patch('tuning_fork.shared.decorators.db_connection.mysql_pool.MySQLConnectionPool')
    def test_return_mysql_connection_success(self, mock_pool_class: Mock) -> None:
        """Test successful MySQL connection return to pool."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_pool_class.return_value = mock_pool
        
        pool = DatabaseConnectionPool()
        pool.initialize_mysql(
            host='localhost',
            port=3306,
            database='testdb',
            username='testuser',
            password='testpass'
        )
        
        pool.return_mysql_connection(mock_connection)
        
        # MySQL connector returns connection to pool on close()
        mock_connection.close.assert_called_once()
    
    def test_return_mysql_connection_pool_not_initialized(self) -> None:
        """Test returning MySQL connection when pool is not initialized."""
        pool = DatabaseConnectionPool()
        mock_connection = MagicMock()
        
        # Should not raise exception, just log warning
        pool.return_mysql_connection(mock_connection)
    
    @patch('tuning_fork.shared.decorators.db_connection.mysql_pool.MySQLConnectionPool')
    @patch('tuning_fork.shared.decorators.db_connection.mysql.connector.Error', Exception)
    def test_return_mysql_connection_with_error(self, mock_pool_class: Mock) -> None:
        """Test returning MySQL connection with error."""
        import mysql.connector
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_connection.close.side_effect = mysql.connector.Error("Error returning connection")
        mock_pool_class.return_value = mock_pool
        
        pool = DatabaseConnectionPool()
        pool.initialize_mysql(
            host='localhost',
            port=3306,
            database='testdb',
            username='testuser',
            password='testpass'
        )
        
        # Should not raise exception, just log error
        pool.return_mysql_connection(mock_connection)
    
    @patch('tuning_fork.shared.decorators.db_connection.mysql_pool.MySQLConnectionPool')
    def test_close_all_mysql(self, mock_pool_class: Mock) -> None:
        """Test closing MySQL connection pool."""
        mock_pool = MagicMock()
        mock_pool_class.return_value = mock_pool
        
        pool = DatabaseConnectionPool()
        pool.initialize_mysql(
            host='localhost',
            port=3306,
            database='testdb',
            username='testuser',
            password='testpass'
        )
        
        pool.close_all()
        
        self.assertIsNone(pool._mysql_pool)
        self.assertIsNone(pool._db_type)
    
    @patch('tuning_fork.shared.decorators.db_connection.pg_pool.ThreadedConnectionPool')
    @patch('tuning_fork.shared.decorators.db_connection.mysql_pool.MySQLConnectionPool')
    def test_close_all_both_pools(self, mock_mysql_pool: Mock, mock_pg_pool: Mock) -> None:
        """Test closing both PostgreSQL and MySQL pools."""
        mock_pg = MagicMock()
        mock_mysql = MagicMock()
        mock_pg_pool.return_value = mock_pg
        mock_mysql_pool.return_value = mock_mysql
        
        pool = DatabaseConnectionPool()
        pool.initialize_postgresql(
            host='localhost',
            port=5432,
            database='testdb',
            username='testuser',
            password='testpass'
        )
        pool.initialize_mysql(
            host='localhost',
            port=3306,
            database='testdb',
            username='testuser',
            password='testpass'
        )
        
        pool.close_all()
        
        mock_pg.closeall.assert_called_once()
        self.assertIsNone(pool._pg_pool)
        self.assertIsNone(pool._mysql_pool)
        self.assertIsNone(pool._db_type)


@unittest.skipIf(not MYSQL_AVAILABLE, "MySQL connector not installed")
class TestGetDbConnectionMySQL(unittest.TestCase):
    """Test suite for get_db_connection context manager with MySQL."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        DatabaseConnectionPool._instance = None
        DatabaseConnectionPool._pg_pool = None
        DatabaseConnectionPool._mysql_pool = None
        DatabaseConnectionPool._db_type = None
        
        # Create mock config for MySQL
        self.mock_config = MagicMock(spec=Config)
        self.mock_config.config = {
            'database': {
                'type': 'mysql',
                'host': 'localhost',
                'port': 3306,
                'database': 'testdb',
                'username': 'testuser',
                'password': 'testpass'
            }
        }
    
    def tearDown(self) -> None:
        """Clean up test fixtures."""
        pool = DatabaseConnectionPool()
        if pool._pg_pool or pool._mysql_pool:
            pool.close_all()
        DatabaseConnectionPool._instance = None
        DatabaseConnectionPool._pg_pool = None
        DatabaseConnectionPool._mysql_pool = None
        DatabaseConnectionPool._db_type = None
    
    @patch('tuning_fork.shared.decorators.db_connection.mysql_pool.MySQLConnectionPool')
    def test_context_manager_mysql_success(self, mock_pool_class: Mock) -> None:
        """Test successful MySQL context manager usage."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        with get_db_connection(self.mock_config) as (conn, cursor):
            self.assertEqual(conn, mock_connection)
            self.assertEqual(cursor, mock_cursor)
        
        mock_connection.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()
    
    @patch('tuning_fork.shared.decorators.db_connection.mysql_pool.MySQLConnectionPool')
    def test_context_manager_mysql_with_exception(self, mock_pool_class: Mock) -> None:
        """Test MySQL context manager rollback on exception."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        with self.assertRaises(DatabaseConnectionError):
            with get_db_connection(self.mock_config) as (conn, cursor):
                raise ValueError("Test error")
        
        mock_connection.rollback.assert_called_once()
        mock_connection.commit.assert_not_called()
        mock_cursor.close.assert_called_once()
    
    @patch('tuning_fork.shared.decorators.db_connection.mysql_pool.MySQLConnectionPool')
    def test_context_manager_mysql_default_port(self, mock_pool_class: Mock) -> None:
        """Test MySQL context manager uses default port when not specified."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        # Remove port from config
        self.mock_config.config['database'].pop('port')
        
        with get_db_connection(self.mock_config) as (conn, cursor):
            pass
        
        # Verify default port 3306 was used
        call_kwargs = mock_pool_class.call_args[1]
        self.assertEqual(call_kwargs['port'], 3306)
    
    @patch('tuning_fork.shared.decorators.db_connection.mysql_pool.MySQLConnectionPool')
    def test_context_manager_mariadb_type(self, mock_pool_class: Mock) -> None:
        """Test context manager recognizes 'mariadb' as MySQL."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        # Set type to mariadb
        self.mock_config.config['database']['type'] = 'mariadb'
        
        with get_db_connection(self.mock_config) as (conn, cursor):
            self.assertEqual(conn, mock_connection)
        
        mock_pool_class.assert_called_once()
    
    def test_context_manager_unsupported_type(self) -> None:
        """Test context manager with unsupported database type."""
        self.mock_config.config['database']['type'] = 'oracle'
        
        with self.assertRaises(DatabaseConnectionError) as context:
            with get_db_connection(self.mock_config) as (conn, cursor):
                pass
        
        self.assertIn("Unsupported database type", str(context.exception))


@unittest.skipIf(not MYSQL_AVAILABLE, "MySQL connector not installed")
class TestWithDatabaseDecoratorMySQL(unittest.TestCase):
    """Test suite for with_database decorator with MySQL."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        DatabaseConnectionPool._instance = None
        DatabaseConnectionPool._pg_pool = None
        DatabaseConnectionPool._mysql_pool = None
        DatabaseConnectionPool._db_type = None
        
        # Create mock config for MySQL
        self.mock_config = MagicMock(spec=Config)
        self.mock_config.config = {
            'database': {
                'type': 'mysql',
                'host': 'localhost',
                'port': 3306,
                'database': 'testdb',
                'username': 'testuser',
                'password': 'testpass'
            }
        }
    
    def tearDown(self) -> None:
        """Clean up test fixtures."""
        pool = DatabaseConnectionPool()
        if pool._pg_pool or pool._mysql_pool:
            pool.close_all()
        DatabaseConnectionPool._instance = None
        DatabaseConnectionPool._pg_pool = None
        DatabaseConnectionPool._mysql_pool = None
        DatabaseConnectionPool._db_type = None
    
    @patch('tuning_fork.shared.decorators.db_connection.mysql_pool.MySQLConnectionPool')
    def test_decorator_mysql_basic_usage(self, mock_pool_class: Mock) -> None:
        """Test basic MySQL decorator usage with cursor injection."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        @with_database(self.mock_config, pass_cursor=True, pass_connection=False)
        def test_function(cursor=None) -> str:
            self.assertIsNotNone(cursor)
            return "success"
        
        result = test_function()
        
        self.assertEqual(result, "success")
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()
    
    @patch('tuning_fork.shared.decorators.db_connection.mysql_pool.MySQLConnectionPool')
    def test_decorator_mysql_with_autocommit(self, mock_pool_class: Mock) -> None:
        """Test MySQL decorator with autocommit enabled."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        @with_database(self.mock_config, autocommit=True, pass_cursor=True)
        def test_function(cursor=None) -> str:
            return "success"
        
        result = test_function()
        
        self.assertEqual(result, "success")
        mock_connection.commit.assert_called_once()
    
    @patch('tuning_fork.shared.decorators.db_connection.mysql_pool.MySQLConnectionPool')
    def test_decorator_mysql_pass_both(self, mock_pool_class: Mock) -> None:
        """Test MySQL decorator passing both connection and cursor."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        @with_database(self.mock_config, pass_connection=True, pass_cursor=True)
        def test_function(connection=None, cursor=None) -> str:
            self.assertIsNotNone(connection)
            self.assertIsNotNone(cursor)
            return "success"
        
        result = test_function()
        self.assertEqual(result, "success")
    
    @patch('tuning_fork.shared.decorators.db_connection.mysql_pool.MySQLConnectionPool')
    def test_decorator_mysql_with_function_args(self, mock_pool_class: Mock) -> None:
        """Test MySQL decorator with function arguments."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        @with_database(self.mock_config, pass_cursor=True)
        def test_function(x: int, y: str, cursor=None) -> str:
            return f"{x}-{y}"
        
        result = test_function(42, "test")
        self.assertEqual(result, "42-test")
    
    @patch('tuning_fork.shared.decorators.db_connection.mysql_pool.MySQLConnectionPool')
    def test_decorator_mysql_with_exception(self, mock_pool_class: Mock) -> None:
        """Test MySQL decorator rollback on exception."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        @with_database(self.mock_config, autocommit=True, pass_cursor=True)
        def test_function(cursor=None) -> None:
            raise ValueError("Test error")
        
        with self.assertRaises(DatabaseConnectionError):
            test_function()
        
        mock_connection.rollback.assert_called_once()
        mock_connection.commit.assert_not_called()
    
    @patch('tuning_fork.shared.decorators.db_connection.mysql_pool.MySQLConnectionPool')
    def test_decorator_mysql_preserves_metadata(self, mock_pool_class: Mock) -> None:
        """Test that MySQL decorator preserves function metadata."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        @with_database(self.mock_config, pass_cursor=True)
        def documented_function(cursor=None) -> str:
            """This is a test function."""
            return "test"
        
        self.assertEqual(documented_function.__name__, "documented_function")
        self.assertEqual(documented_function.__doc__, "This is a test function.")
    
    @patch('tuning_fork.shared.decorators.db_connection.mysql_pool.MySQLConnectionPool')
    def test_decorator_mysql_with_return_value(self, mock_pool_class: Mock) -> None:
        """Test that MySQL decorator properly returns function result."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        @with_database(self.mock_config, pass_cursor=True)
        def test_function(cursor=None) -> dict[str, Any]:
            return {"status": "ok", "data": [1, 2, 3]}
        
        result = test_function()
        self.assertEqual(result, {"status": "ok", "data": [1, 2, 3]})
    
    @patch('tuning_fork.shared.decorators.db_connection.mysql_pool.MySQLConnectionPool')
    def test_decorator_mysql_cursor_close_error(self, mock_pool_class: Mock) -> None:
        """Test MySQL decorator handles cursor close errors gracefully."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.close.side_effect = Exception("Error closing cursor")
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        @with_database(self.mock_config, pass_cursor=True)
        def test_function(cursor=None) -> str:
            return "success"
        
        # Should complete successfully despite cursor close error
        result = test_function()
        self.assertEqual(result, "success")
    
    @patch('tuning_fork.shared.decorators.db_connection.mysql_pool.MySQLConnectionPool')
    def test_decorator_mysql_default_port(self, mock_pool_class: Mock) -> None:
        """Test MySQL decorator uses default port when not specified."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        # Remove port from config
        self.mock_config.config['database'].pop('port')
        
        @with_database(self.mock_config, pass_cursor=True)
        def test_function(cursor=None) -> str:
            return "success"
        
        test_function()
        
        # Verify default port 3306 was used
        call_kwargs = mock_pool_class.call_args[1]
        self.assertEqual(call_kwargs['port'], 3306)
    
    @patch('tuning_fork.shared.decorators.db_connection.mysql_pool.MySQLConnectionPool')
    def test_decorator_mariadb_type(self, mock_pool_class: Mock) -> None:
        """Test decorator recognizes 'mariadb' as MySQL."""
        mock_pool = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        # Set type to mariadb
        self.mock_config.config['database']['type'] = 'mariadb'
        
        @with_database(self.mock_config, pass_cursor=True)
        def test_function(cursor=None) -> str:
            return "success"
        
        result = test_function()
        self.assertEqual(result, "success")
        mock_pool_class.assert_called_once()
    
    def test_decorator_unsupported_type(self) -> None:
        """Test decorator with unsupported database type."""
        self.mock_config.config['database']['type'] = 'oracle'
        
        @with_database(self.mock_config, pass_cursor=True)
        def test_function(cursor=None) -> str:
            return "success"
        
        with self.assertRaises(DatabaseConnectionError) as context:
            test_function()
        
        self.assertIn("Unsupported database type", str(context.exception))
    
    def test_decorator_no_database_config(self) -> None:
        """Test MySQL decorator with missing database configuration."""
        self.mock_config.config = {}
        
        @with_database(self.mock_config, pass_cursor=True)
        def test_function(cursor=None) -> str:
            return "success"
        
        with self.assertRaises(DatabaseConnectionError) as context:
            test_function()
        
        self.assertIn("No database configuration found", str(context.exception))


@unittest.skipIf(not MYSQL_AVAILABLE, "MySQL connector not installed")
class TestMixedPostgreSQLMySQL(unittest.TestCase):
    """Test suite for mixed PostgreSQL and MySQL usage."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        DatabaseConnectionPool._instance = None
        DatabaseConnectionPool._pg_pool = None
        DatabaseConnectionPool._mysql_pool = None
        DatabaseConnectionPool._db_type = None
    
    def tearDown(self) -> None:
        """Clean up test fixtures."""
        pool = DatabaseConnectionPool()
        if pool._pg_pool or pool._mysql_pool:
            pool.close_all()
        DatabaseConnectionPool._instance = None
        DatabaseConnectionPool._pg_pool = None
        DatabaseConnectionPool._mysql_pool = None
        DatabaseConnectionPool._db_type = None
    
    @patch('tuning_fork.shared.decorators.db_connection.mysql_pool.MySQLConnectionPool')
    @patch('tuning_fork.shared.decorators.db_connection.pg_pool.ThreadedConnectionPool')
    def test_can_initialize_both_pools(self, mock_pg_pool: Mock, mock_mysql_pool: Mock) -> None:
        """Test that both PostgreSQL and MySQL pools can be initialized."""
        mock_pg = MagicMock()
        mock_mysql = MagicMock()
        mock_pg_pool.return_value = mock_pg
        mock_mysql_pool.return_value = mock_mysql
        
        pool = DatabaseConnectionPool()
        
        pool.initialize_postgresql(
            host='localhost',
            port=5432,
            database='pgdb',
            username='pguser',
            password='pgpass'
        )
        
        pool.initialize_mysql(
            host='localhost',
            port=3306,
            database='mysqldb',
            username='mysqluser',
            password='mysqlpass'
        )
        
        self.assertIsNotNone(pool._pg_pool)
        self.assertIsNotNone(pool._mysql_pool)
    
    @patch('tuning_fork.shared.decorators.db_connection.mysql_pool.MySQLConnectionPool')
    @patch('tuning_fork.shared.decorators.db_connection.pg_pool.ThreadedConnectionPool')
    def test_can_get_connections_from_both_pools(
        self,
        mock_pg_pool: Mock,
        mock_mysql_pool: Mock
    ) -> None:
        """Test getting connections from both pools."""
        mock_pg = MagicMock()
        mock_mysql = MagicMock()
        mock_pg_conn = MagicMock()
        mock_mysql_conn = MagicMock()
        mock_pg.getconn.return_value = mock_pg_conn
        mock_mysql.get_connection.return_value = mock_mysql_conn
        mock_pg_pool.return_value = mock_pg
        mock_mysql_pool.return_value = mock_mysql
        
        pool = DatabaseConnectionPool()
        
        pool.initialize_postgresql(
            host='localhost',
            port=5432,
            database='pgdb',
            username='pguser',
            password='pgpass'
        )
        
        pool.initialize_mysql(
            host='localhost',
            port=3306,
            database='mysqldb',
            username='mysqluser',
            password='mysqlpass'
        )
        
        pg_conn = pool.get_postgresql_connection()
        mysql_conn = pool.get_mysql_connection()
        
        self.assertEqual(pg_conn, mock_pg_conn)
        self.assertEqual(mysql_conn, mock_mysql_conn)


class TestMySQLNotAvailable(unittest.TestCase):
    """Test suite for handling when MySQL is not installed."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
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
    
    @unittest.skipIf(MYSQL_AVAILABLE, "MySQL connector is installed")
    def test_initialize_mysql_without_connector(self) -> None:
        """Test MySQL initialization fails gracefully without mysql-connector-python."""
        pool = DatabaseConnectionPool()
        
        with self.assertRaises(DatabaseConnectionError) as context:
            pool.initialize_mysql(
                host='localhost',
                port=3306,
                database='testdb',
                username='testuser',
                password='testpass'
            )
        
        self.assertIn("MySQL support not available", str(context.exception))
        self.assertIn("mysql-connector-python", str(context.exception))


if __name__ == '__main__':
    unittest.main()