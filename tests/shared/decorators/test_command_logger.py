"""
Unit tests for command_logger module.

This test suite ensures comprehensive coverage of the command logging
decorator and SQLite logging functionality.
"""

import json
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, Mock, patch

from tuning_fork.config import Config
from tuning_fork.shared.decorators.command_logger import (
    CommandLogger,
    CommandLoggerError,
    with_command_logging,
)


class TestCommandLogger(unittest.TestCase):
    """Test suite for CommandLogger class."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        # Create temporary directory for test databases
        self.temp_dir = tempfile.mkdtemp()
        self.log_file = str(Path(self.temp_dir) / "test_commands.db")
    
    def tearDown(self) -> None:
        """Clean up test fixtures."""
        # Clean up temporary files
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_init_creates_database(self) -> None:
        """Test that initialization creates database with proper schema."""
        logger = CommandLogger(self.log_file, auto_create=True)
        
        # Verify database file exists
        self.assertTrue(Path(self.log_file).exists())
        
        # Verify schema
        with sqlite3.connect(self.log_file) as conn:
            cursor = conn.cursor()
            
            # Check table exists
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='command_log'"
            )
            self.assertIsNotNone(cursor.fetchone())
            
            # Check indexes exist
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            )
            indexes = [row[0] for row in cursor.fetchall()]
            self.assertIn('idx_timestamp', indexes)
            self.assertIn('idx_function_name', indexes)
            self.assertIn('idx_success', indexes)
    
    def test_init_without_auto_create(self) -> None:
        """Test initialization without auto-creating database."""
        logger = CommandLogger(self.log_file, auto_create=False)
        
        # Database should not be created yet
        self.assertFalse(Path(self.log_file).exists())
    
    def test_log_command_success(self) -> None:
        """Test logging a successful command."""
        logger = CommandLogger(self.log_file, auto_create=True)
        
        log_id = logger.log_command(
            function_name="test_function",
            command_text="SELECT * FROM users",
            execution_time=0.123,
            success=True,
            command_type="SELECT",
            row_count=10
        )
        
        self.assertIsInstance(log_id, int)
        self.assertGreater(log_id, 0)
        
        # Verify log entry
        with sqlite3.connect(self.log_file) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM command_log WHERE id = ?", (log_id,))
            row = cursor.fetchone()
            
            self.assertIsNotNone(row)
            self.assertEqual(row[2], "test_function")  # function_name
            self.assertEqual(row[3], "SELECT")  # command_type
            self.assertEqual(row[4], "SELECT * FROM users")  # command_text
            self.assertAlmostEqual(row[5], 0.123, places=6)  # execution_time
            self.assertEqual(row[6], 10)  # row_count
            self.assertEqual(row[7], 1)  # success (stored as 1/0)
    
    def test_log_command_failure(self) -> None:
        """Test logging a failed command."""
        logger = CommandLogger(self.log_file, auto_create=True)
        
        log_id = logger.log_command(
            function_name="test_function",
            command_text="SELECT * FROM nonexistent",
            execution_time=0.050,
            success=False,
            error_message="Table does not exist"
        )
        
        # Verify error is logged
        with sqlite3.connect(self.log_file) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT success, error_message FROM command_log WHERE id = ?", (log_id,))
            row = cursor.fetchone()
            
            self.assertEqual(row[0], 0)  # success = False
            self.assertEqual(row[1], "Table does not exist")
    
    def test_log_command_with_metadata(self) -> None:
        """Test logging a command with metadata."""
        logger = CommandLogger(self.log_file, auto_create=True)
        
        metadata = {
            'user_id': 123,
            'session_id': 'abc-def',
            'extra_info': 'test data'
        }
        
        log_id = logger.log_command(
            function_name="test_function",
            command_text="INSERT INTO users VALUES (?)",
            execution_time=0.075,
            success=True,
            metadata=metadata
        )
        
        # Verify metadata is stored as JSON
        with sqlite3.connect(self.log_file) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT metadata FROM command_log WHERE id = ?", (log_id,))
            stored_metadata = cursor.fetchone()[0]
            
            self.assertIsNotNone(stored_metadata)
            parsed_metadata = json.loads(stored_metadata)
            self.assertEqual(parsed_metadata, metadata)
    
    def test_log_command_with_result_summary(self) -> None:
        """Test logging a command with result summary."""
        logger = CommandLogger(self.log_file, auto_create=True)
        
        log_id = logger.log_command(
            function_name="test_function",
            command_text="SELECT * FROM users LIMIT 5",
            execution_time=0.045,
            success=True,
            result_summary="5 rows returned, sample: (1, 'John', 'john@example.com')"
        )
        
        # Verify result summary
        with sqlite3.connect(self.log_file) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT result_summary FROM command_log WHERE id = ?", (log_id,))
            summary = cursor.fetchone()[0]
            
            self.assertIn("5 rows", summary)
            self.assertIn("John", summary)
    
    def test_log_command_with_all_fields(self) -> None:
        """Test logging a command with all optional fields populated."""
        logger = CommandLogger(self.log_file, auto_create=True)
        
        log_id = logger.log_command(
            function_name="complex_function",
            command_text="UPDATE users SET status = 'active'",
            execution_time=0.234,
            success=True,
            command_type="UPDATE",
            row_count=42,
            error_message=None,
            result_summary="42 rows updated",
            metadata={'batch_id': 'batch_123'}
        )
        
        # Verify all fields
        with sqlite3.connect(self.log_file) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM command_log WHERE id = ?", (log_id,))
            row = cursor.fetchone()
            
            self.assertEqual(row[2], "complex_function")
            self.assertEqual(row[3], "UPDATE")
            self.assertEqual(row[4], "UPDATE users SET status = 'active'")
            self.assertAlmostEqual(row[5], 0.234, places=6)
            self.assertEqual(row[6], 42)
            self.assertEqual(row[7], 1)
            self.assertIsNone(row[8])  # error_message
            self.assertEqual(row[9], "42 rows updated")
            self.assertIsNotNone(row[10])  # metadata
    
    @patch('tuning_fork.shared.decorators.command_logger.sqlite3.connect')
    def test_log_command_database_error(self, mock_connect: Mock) -> None:
        """Test handling of database errors during logging."""
        mock_connect.side_effect = sqlite3.Error("Database is locked")
        
        logger = CommandLogger(self.log_file, auto_create=False)
        
        with self.assertRaises(CommandLoggerError) as context:
            logger.log_command(
                function_name="test",
                command_text="SELECT 1",
                execution_time=0.1,
                success=True
            )
        
        self.assertIn("Failed to log command", str(context.exception))


class TestWithCommandLogging(unittest.TestCase):
    """Test suite for with_command_logging decorator."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        # Create temporary directory for test databases
        self.temp_dir = tempfile.mkdtemp()
        self.log_file = str(Path(self.temp_dir) / "test_commands.db")
        
        # Create mock config
        self.mock_config = MagicMock(spec=Config)
        self.mock_config.config = {
            'logging': {
                'filename': self.log_file,
                'auto_create': True,
                'auto_backup': False
            }
        }
    
    def tearDown(self) -> None:
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_decorator_basic_usage(self) -> None:
        """Test basic decorator usage."""
        @with_command_logging(self.mock_config)
        def test_function() -> str:
            return "success"
        
        result = test_function()
        
        self.assertEqual(result, "success")
        
        # Verify log entry was created
        with sqlite3.connect(self.log_file) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM command_log")
            count = cursor.fetchone()[0]
            self.assertEqual(count, 1)
    
    def test_decorator_with_cursor(self) -> None:
        """Test decorator with cursor parameter."""
        @with_command_logging(self.mock_config)
        def test_function(cursor=None) -> str:
            # Simulate cursor with query attribute
            return "success"
        
        mock_cursor = MagicMock()
        mock_cursor.query = b"SELECT * FROM users"
        mock_cursor.rowcount = 5
        
        result = test_function(cursor=mock_cursor)
        
        self.assertEqual(result, "success")
        
        # Verify command text was captured
        with sqlite3.connect(self.log_file) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT command_text, row_count FROM command_log")
            row = cursor.fetchone()
            self.assertEqual(row[0], "SELECT * FROM users")
            self.assertEqual(row[1], 5)
    
    def test_decorator_with_string_query(self) -> None:
        """Test decorator with string query attribute."""
        @with_command_logging(self.mock_config)
        def test_function(cursor=None) -> str:
            return "success"
        
        mock_cursor = MagicMock()
        mock_cursor.query = "INSERT INTO users VALUES (1, 'test')"
        mock_cursor.rowcount = 1
        
        result = test_function(cursor=mock_cursor)
        
        # Verify command text was captured as string
        with sqlite3.connect(self.log_file) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT command_text FROM command_log")
            command_text = cursor.fetchone()[0]
            self.assertEqual(command_text, "INSERT INTO users VALUES (1, 'test')")
    
    def test_decorator_with_log_results(self) -> None:
        """Test decorator with result logging enabled."""
        @with_command_logging(self.mock_config, log_results=True)
        def test_function() -> list[tuple]:
            return [(1, 'John'), (2, 'Jane'), (3, 'Bob')]
        
        result = test_function()
        
        # Verify result summary was captured
        with sqlite3.connect(self.log_file) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT result_summary FROM command_log")
            summary = cursor.fetchone()[0]
            self.assertIn("3 rows returned", summary)
            self.assertIn("(1, 'John')", summary)
    
    def test_decorator_with_max_result_length(self) -> None:
        """Test decorator respects max_result_length."""
        @with_command_logging(self.mock_config, log_results=True, max_result_length=20)
        def test_function() -> list[tuple]:
            return [('a' * 100, 'b' * 100)]
        
        result = test_function()
        
        # Verify result was truncated
        with sqlite3.connect(self.log_file) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT result_summary FROM command_log")
            summary = cursor.fetchone()[0]
            self.assertIn("...", summary)
    
    def test_decorator_with_exception(self) -> None:
        """Test decorator logs failures correctly."""
        @with_command_logging(self.mock_config)
        def test_function() -> None:
            raise ValueError("Test error")
        
        with self.assertRaises(ValueError):
            test_function()
        
        # Verify error was logged
        with sqlite3.connect(self.log_file) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT success, error_message FROM command_log")
            row = cursor.fetchone()
            self.assertEqual(row[0], 0)  # success = False
            self.assertIn("ValueError", row[1])
            self.assertIn("Test error", row[1])
    
    def test_decorator_extracts_command_type(self) -> None:
        """Test decorator extracts command type from query."""
        @with_command_logging(self.mock_config, extract_command_type=True)
        def test_function(cursor=None) -> str:
            return "success"
        
        mock_cursor = MagicMock()
        mock_cursor.query = "UPDATE users SET name = 'test'"
        mock_cursor.rowcount = 1
        
        test_function(cursor=mock_cursor)
        
        # Verify command type was extracted
        with sqlite3.connect(self.log_file) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT command_type FROM command_log")
            cmd_type = cursor.fetchone()[0]
            self.assertEqual(cmd_type, "UPDATE")
    
    def test_decorator_without_command_type_extraction(self) -> None:
        """Test decorator with command type extraction disabled."""
        @with_command_logging(self.mock_config, extract_command_type=False)
        def test_function(cursor=None) -> str:
            return "success"
        
        mock_cursor = MagicMock()
        mock_cursor.query = "DELETE FROM users"
        mock_cursor.rowcount = 5
        
        test_function(cursor=mock_cursor)
        
        # Verify command type was not extracted
        with sqlite3.connect(self.log_file) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT command_type FROM command_log")
            cmd_type = cursor.fetchone()[0]
            self.assertIsNone(cmd_type)
    
    def test_decorator_with_function_args(self) -> None:
        """Test decorator with function arguments."""
        @with_command_logging(self.mock_config)
        def test_function(x: int, y: str, cursor=None) -> str:
            return f"{x}-{y}"
        
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1  # Set actual integer value
        result = test_function(42, "test", cursor=mock_cursor)
        
        self.assertEqual(result, "42-test")
        
        # Verify metadata captured args info
        with sqlite3.connect(self.log_file) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT metadata FROM command_log")
            metadata_json = cursor.fetchone()[0]
            metadata = json.loads(metadata_json)
            self.assertEqual(metadata['args_count'], 2)
            self.assertIn('cursor', metadata['kwargs_keys'])
    
    def test_decorator_preserves_function_metadata(self) -> None:
        """Test that decorator preserves function metadata."""
        @with_command_logging(self.mock_config)
        def documented_function() -> str:
            """This is a test function."""
            return "test"
        
        self.assertEqual(documented_function.__name__, "documented_function")
        self.assertEqual(documented_function.__doc__, "This is a test function.")
    
    def test_decorator_with_non_list_result(self) -> None:
        """Test decorator with non-list result."""
        @with_command_logging(self.mock_config, log_results=True)
        def test_function() -> int:
            return 42
        
        result = test_function()
        
        # Verify scalar result was captured
        with sqlite3.connect(self.log_file) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT result_summary FROM command_log")
            summary = cursor.fetchone()[0]
            self.assertEqual(summary, "42")
    
    def test_decorator_with_cursor_no_query(self) -> None:
        """Test decorator with cursor that has no query attribute."""
        @with_command_logging(self.mock_config)
        def test_function(cursor=None) -> str:
            return "success"
        
        mock_cursor = MagicMock(spec=[])  # No query attribute
        result = test_function(cursor=mock_cursor)
        
        # Should succeed without error
        self.assertEqual(result, "success")
    
    def test_decorator_with_negative_rowcount(self) -> None:
        """Test decorator with negative rowcount (no rows affected)."""
        @with_command_logging(self.mock_config)
        def test_function(cursor=None) -> str:
            return "success"
        
        mock_cursor = MagicMock()
        mock_cursor.query = "SELECT 1"
        mock_cursor.rowcount = -1
        
        test_function(cursor=mock_cursor)
        
        # Verify rowcount was not logged when negative
        with sqlite3.connect(self.log_file) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT row_count FROM command_log")
            row_count = cursor.fetchone()[0]
            self.assertIsNone(row_count)
    
    @patch('tuning_fork.shared.decorators.command_logger.CommandLogger.log_command')
    def test_decorator_logging_failure_doesnt_break_function(self, mock_log: Mock) -> None:
        """Test that logging failures don't break the decorated function."""
        mock_log.side_effect = CommandLoggerError("Logging failed")
        
        @with_command_logging(self.mock_config)
        def test_function() -> str:
            return "success"
        
        # Function should succeed despite logging failure
        result = test_function()
        self.assertEqual(result, "success")
    
    def test_decorator_with_empty_result_list(self) -> None:
        """Test decorator with empty result list."""
        @with_command_logging(self.mock_config, log_results=True)
        def test_function() -> list:
            return []
        
        result = test_function()
        
        # Verify empty result was logged
        with sqlite3.connect(self.log_file) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT result_summary FROM command_log")
            summary = cursor.fetchone()[0]
            self.assertEqual(summary, "0 rows returned")


if __name__ == '__main__':
    unittest.main()