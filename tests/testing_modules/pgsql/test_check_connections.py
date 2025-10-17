"""
Test suite for PostgreSQL connections checker module.

This module provides comprehensive tests for connection analysis,
blocking query detection, idle transaction monitoring, and
connection management actions.
"""

import json
import unittest
from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

from tuning_fork.config import Config
from tuning_fork.testing_modules.pgsql.check_connections import (
    CONNECTION_THRESHOLDS,
    ConnectionAction,
    ConnectionCheckResult,
    apply_actions,
    check_connections,
    report_connections,
    rollback_actions,
)


class TestConnectionCheckResult(unittest.TestCase):
    """Test suite for ConnectionCheckResult dataclass."""
    
    def test_create_result(self) -> None:
        """Test creating a connection check result."""
        result = ConnectionCheckResult(
            check_type='blocking_query',
            pid=12345,
            database='testdb',
            username='testuser',
            application_name='test_app',
            state='active',
            query='SELECT * FROM users',
            duration_seconds=120.5,
            wait_event_type='Lock',
            wait_event='relation',
            blocked_by=[12346],
            blocking=[],
            status='WARNING',
            message='Test message',
            recommendation='Test recommendation'
        )
        
        self.assertEqual(result.check_type, 'blocking_query')
        self.assertEqual(result.pid, 12345)
        self.assertEqual(result.status, 'WARNING')
        self.assertEqual(len(result.blocked_by), 1)
    
    def test_result_to_dict(self) -> None:
        """Test converting result to dictionary."""
        result = ConnectionCheckResult(
            check_type='idle_in_transaction',
            pid=12345,
            database='testdb',
            username='testuser',
            application_name=None,
            state='idle in transaction',
            query='BEGIN',
            duration_seconds=600.0,
            wait_event_type=None,
            wait_event=None,
            blocked_by=[],
            blocking=[],
            status='CRITICAL',
            message='Long idle transaction',
            recommendation='Close transaction'
        )
        
        result_dict = result.to_dict()
        
        self.assertIsInstance(result_dict, dict)
        self.assertEqual(result_dict['check_type'], 'idle_in_transaction')
        self.assertEqual(result_dict['pid'], 12345)
        self.assertEqual(result_dict['duration_seconds'], 600.0)
    
    def test_result_to_json(self) -> None:
        """Test converting result to JSON."""
        result = ConnectionCheckResult(
            check_type='long_running_query',
            pid=12345,
            database='testdb',
            username='testuser',
            application_name='test_app',
            state='active',
            query='SELECT * FROM large_table',
            duration_seconds=3600.0,
            wait_event_type='IO',
            wait_event='DataFileRead',
            blocked_by=[],
            blocking=[],
            status='CRITICAL',
            message='Long running query',
            recommendation='Review query'
        )
        
        json_str = result.to_json()
        parsed = json.loads(json_str)
        
        self.assertEqual(parsed['check_type'], 'long_running_query')
        self.assertEqual(parsed['duration_seconds'], 3600.0)


class TestConnectionAction(unittest.TestCase):
    """Test suite for ConnectionAction dataclass."""
    
    def test_create_action(self) -> None:
        """Test creating a connection action."""
        action = ConnectionAction(
            action_type='terminate',
            pid=12345,
            setting_name=None,
            old_value='running',
            new_value='terminated',
            timestamp='2025-01-01T00:00:00',
            action_id=1,
            success=True
        )
        
        self.assertEqual(action.action_type, 'terminate')
        self.assertEqual(action.pid, 12345)
        self.assertTrue(action.success)
        self.assertFalse(action.rollback_status)
    
    def test_action_to_dict(self) -> None:
        """Test converting action to dictionary."""
        action = ConnectionAction(
            action_type='cancel',
            pid=12346,
            setting_name=None,
            old_value='running',
            new_value='canceled',
            timestamp='2025-01-01T00:00:00',
            action_id=2,
            success=True
        )
        
        action_dict = action.to_dict()
        
        self.assertIsInstance(action_dict, dict)
        self.assertEqual(action_dict['action_type'], 'cancel')
        self.assertEqual(action_dict['pid'], 12346)


class TestCheckConnections(unittest.TestCase):
    """Test suite for check_connections function."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        self.mock_config = MagicMock(spec=Config)
        self.mock_cursor = MagicMock()
        self.mock_connection = MagicMock()
    
    @patch('tuning_fork.testing_modules.pgsql.check_connections.get_db_connection')
    def test_check_connections_basic(self, mock_get_db: Mock) -> None:
        """Test basic connection check."""
        # Mock max_connections
        self.mock_cursor.fetchone.side_effect = [
            ('100',),  # max_connections
            (50,),     # active connections count
        ]
        self.mock_cursor.fetchall.return_value = []  # No blocking queries
        
        mock_get_db.return_value.__enter__.return_value = (
            self.mock_connection, self.mock_cursor
        )
        
        results = check_connections(self.mock_config)
        
        self.assertIsInstance(results, list)
        # Should have at least connection pool check
        self.assertGreater(len(results), 0)
    
    @patch('tuning_fork.testing_modules.pgsql.check_connections.get_db_connection')
    def test_check_connections_high_usage(self, mock_get_db: Mock) -> None:
        """Test connection check with high pool usage."""
        # Mock 95% connection usage
        self.mock_cursor.fetchone.side_effect = [
            ('100',),  # max_connections
            (95,),     # active connections count (critical)
        ]
        self.mock_cursor.fetchall.return_value = []
        
        mock_get_db.return_value.__enter__.return_value = (
            self.mock_connection, self.mock_cursor
        )
        
        results = check_connections(self.mock_config)
        
        # Find connection pool result
        pool_results = [r for r in results if r.check_type == 'connection_pool']
        self.assertEqual(len(pool_results), 1)
        self.assertEqual(pool_results[0].status, 'CRITICAL')
    
    @patch('tuning_fork.testing_modules.pgsql.check_connections.get_db_connection')
    def test_check_connections_with_blocking(self, mock_get_db: Mock) -> None:
        """Test connection check with blocking queries."""
        self.mock_cursor.fetchone.side_effect = [
            ('100',),  # max_connections
            (50,),     # active connections count
        ]
        
        # Mock blocking query data
        blocking_data = [
            (
                12345,  # blocked_pid
                'testuser',  # blocked_user
                'testdb',  # blocked_db
                'test_app',  # blocked_app
                'active',  # blocked_state
                'SELECT * FROM users',  # blocked_query
                120.5,  # blocked_duration
                'Lock',  # wait_event_type
                'relation',  # wait_event
                12346,  # blocking_pid
                'otheruser',  # blocking_user
                'UPDATE users SET status = 1',  # blocking_query
                300.0  # blocking_duration
            )
        ]
        
        self.mock_cursor.fetchall.side_effect = [
            blocking_data,  # blocking queries
            [],  # idle transactions
            [],  # long running queries
            []   # wait events
        ]
        
        mock_get_db.return_value.__enter__.return_value = (
            self.mock_connection, self.mock_cursor
        )
        
        results = check_connections(self.mock_config)
        
        blocking_results = [r for r in results if r.check_type == 'blocking_query']
        self.assertEqual(len(blocking_results), 1)
        self.assertEqual(blocking_results[0].pid, 12345)
        self.assertEqual(blocking_results[0].blocked_by, [12346])
        self.assertEqual(blocking_results[0].status, 'WARNING')
    
    @patch('tuning_fork.testing_modules.pgsql.check_connections.get_db_connection')
    def test_check_connections_idle_transactions(self, mock_get_db: Mock) -> None:
        """Test checking for idle in transaction connections."""
        self.mock_cursor.fetchone.side_effect = [
            ('100',),  # max_connections
            (50,),     # active connections count
        ]
        
        # Mock idle transaction data
        idle_data = [
            (
                12347,  # pid
                'testuser',  # username
                'testdb',  # database
                'test_app',  # app_name
                'idle in transaction',  # state
                'BEGIN',  # query
                1800.0,  # duration (30 minutes - critical)
                None,  # wait_event_type
                None  # wait_event
            )
        ]
        
        self.mock_cursor.fetchall.side_effect = [
            [],  # blocking queries
            idle_data,  # idle transactions
            [],  # long running queries
            []   # wait events
        ]
        
        mock_get_db.return_value.__enter__.return_value = (
            self.mock_connection, self.mock_cursor
        )
        
        results = check_connections(self.mock_config)
        
        idle_results = [r for r in results if r.check_type == 'idle_in_transaction']
        self.assertEqual(len(idle_results), 1)
        self.assertEqual(idle_results[0].status, 'CRITICAL')
        self.assertEqual(idle_results[0].duration_seconds, 1800.0)
    
    @patch('tuning_fork.testing_modules.pgsql.check_connections.get_db_connection')
    def test_check_connections_long_running_queries(self, mock_get_db: Mock) -> None:
        """Test checking for long-running queries."""
        self.mock_cursor.fetchone.side_effect = [
            ('100',),  # max_connections
            (50,),     # active connections count
        ]
        
        # Mock long-running query data
        long_query_data = [
            (
                12348,  # pid
                'testuser',  # username
                'testdb',  # database
                'test_app',  # app_name
                'active',  # state
                'SELECT * FROM huge_table',  # query
                3600.0,  # duration (1 hour - critical)
                'IO',  # wait_event_type
                'DataFileRead'  # wait_event
            )
        ]
        
        self.mock_cursor.fetchall.side_effect = [
            [],  # blocking queries
            [],  # idle transactions
            long_query_data,  # long running queries
            []   # wait events
        ]
        
        mock_get_db.return_value.__enter__.return_value = (
            self.mock_connection, self.mock_cursor
        )
        
        results = check_connections(self.mock_config)
        
        long_results = [r for r in results if r.check_type == 'long_running_query']
        self.assertEqual(len(long_results), 1)
        self.assertEqual(long_results[0].status, 'CRITICAL')
        self.assertEqual(long_results[0].wait_event_type, 'IO')
    
    @patch('tuning_fork.testing_modules.pgsql.check_connections.get_db_connection')
    def test_check_connections_wait_events(self, mock_get_db: Mock) -> None:
        """Test analyzing wait events."""
        self.mock_cursor.fetchone.side_effect = [
            ('100',),  # max_connections
            (50,),     # active connections count
        ]
        
        # Mock wait event data
        wait_data = [
            ('Lock', 'relation', 10, [12349, 12350, 12351, 12352, 12353,
                                       12354, 12355, 12356, 12357, 12358])
        ]
        
        self.mock_cursor.fetchall.side_effect = [
            [],  # blocking queries
            [],  # idle transactions
            [],  # long running queries
            wait_data  # wait events (10 connections - critical)
        ]
        
        mock_get_db.return_value.__enter__.return_value = (
            self.mock_connection, self.mock_cursor
        )
        
        results = check_connections(self.mock_config)
        
        wait_results = [r for r in results if r.check_type == 'wait_event']
        self.assertEqual(len(wait_results), 1)
        self.assertEqual(wait_results[0].status, 'CRITICAL')
        self.assertIn('Lock', wait_results[0].message)
    
    @patch('tuning_fork.testing_modules.pgsql.check_connections.get_db_connection')
    def test_check_connections_custom_thresholds(self, mock_get_db: Mock) -> None:
        """Test using custom thresholds."""
        custom_thresholds = {
            'max_connections_usage_warning': 0.50,
            'idle_in_transaction_warning': 60
        }
        
        self.mock_cursor.fetchone.side_effect = [
            ('100',),  # max_connections
            (55,),     # active connections count (would trigger custom threshold)
        ]
        self.mock_cursor.fetchall.return_value = []
        
        mock_get_db.return_value.__enter__.return_value = (
            self.mock_connection, self.mock_cursor
        )
        
        results = check_connections(self.mock_config, thresholds=custom_thresholds)
        
        pool_results = [r for r in results if r.check_type == 'connection_pool']
        self.assertEqual(len(pool_results), 1)
        self.assertEqual(pool_results[0].status, 'WARNING')
    
    @patch('tuning_fork.testing_modules.pgsql.check_connections.get_db_connection')
    def test_check_connections_database_error(self, mock_get_db: Mock) -> None:
        """Test handling database errors."""
        mock_get_db.return_value.__enter__.side_effect = Exception("Connection failed")
        
        with self.assertRaises(RuntimeError) as context:
            check_connections(self.mock_config)
        
        self.assertIn("Connection analysis failed", str(context.exception))


class TestReportConnections(unittest.TestCase):
    """Test suite for report_connections function."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        self.sample_results = [
            ConnectionCheckResult(
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
                status='OK',
                message='Connection pool healthy',
                recommendation='No action required'
            ),
            ConnectionCheckResult(
                check_type='blocking_query',
                pid=12345,
                database='testdb',
                username='testuser',
                application_name='test_app',
                state='active',
                query='SELECT * FROM users WHERE id = 1',
                duration_seconds=120.5,
                wait_event_type='Lock',
                wait_event='relation',
                blocked_by=[12346],
                blocking=[],
                status='WARNING',
                message='PID 12345 blocked by PID 12346',
                recommendation='Review blocking query'
            )
        ]
    
    def test_report_text_format(self) -> None:
        """Test generating text format report."""
        report = report_connections(self.sample_results, format='text')
        
        self.assertIsInstance(report, str)
        self.assertIn('PostgreSQL Connection Analysis Report', report)
        self.assertIn('[WARNING]', report)
        self.assertNotIn('[OK]', report)  # OK results excluded by default
    
    def test_report_text_format_include_ok(self) -> None:
        """Test generating text report including OK results."""
        report = report_connections(
            self.sample_results,
            format='text',
            include_ok=True
        )
        
        self.assertIn('[OK]', report)
        self.assertIn('[WARNING]', report)
    
    def test_report_json_format(self) -> None:
        """Test generating JSON format report."""
        report = report_connections(self.sample_results, format='json')
        
        parsed = json.loads(report)
        self.assertIn('generated', parsed)
        self.assertIn('total_findings', parsed)
        self.assertIn('results', parsed)
        self.assertEqual(parsed['total_findings'], 1)  # Only WARNING
    
    def test_report_json_format_include_ok(self) -> None:
        """Test generating JSON report including OK results."""
        report = report_connections(
            self.sample_results,
            format='json',
            include_ok=True
        )
        
        parsed = json.loads(report)
        self.assertEqual(parsed['total_findings'], 2)
    
    def test_report_html_format(self) -> None:
        """Test generating HTML format report."""
        report = report_connections(self.sample_results, format='html')
        
        self.assertIsInstance(report, str)
        self.assertIn('<!DOCTYPE html>', report)
        self.assertIn('PostgreSQL Connection Analysis', report)
        self.assertIn('WARNING', report)
    
    def test_report_invalid_format(self) -> None:
        """Test error handling for invalid format."""
        with self.assertRaises(ValueError) as context:
            report_connections(self.sample_results, format='xml')
        
        self.assertIn('Unsupported format', str(context.exception))
    
    def test_report_empty_results(self) -> None:
        """Test reporting with no findings."""
        report = report_connections([], format='text')
        
        self.assertIn('No connection issues found', report)


class TestApplyActions(unittest.TestCase):
    """Test suite for apply_actions function."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        self.mock_config = MagicMock(spec=Config)
        self.mock_cursor = MagicMock()
        self.mock_connection = MagicMock()
    
    @patch('tuning_fork.testing_modules.pgsql.check_connections.get_db_connection')
    def test_apply_terminate_action(self, mock_get_db: Mock) -> None:
        """Test terminating a connection."""
        self.mock_cursor.fetchone.return_value = (True,)
        mock_get_db.return_value.__enter__.return_value = (
            self.mock_connection, self.mock_cursor
        )
        
        actions = {'terminate_pids': [12345]}
        results = apply_actions(self.mock_config, actions)
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].action_type, 'terminate')
        self.assertEqual(results[0].pid, 12345)
        self.assertTrue(results[0].success)
    
    @patch('tuning_fork.testing_modules.pgsql.check_connections.get_db_connection')
    def test_apply_terminate_multiple(self, mock_get_db: Mock) -> None:
        """Test terminating multiple connections."""
        self.mock_cursor.fetchone.return_value = (True,)
        mock_get_db.return_value.__enter__.return_value = (
            self.mock_connection, self.mock_cursor
        )
        
        actions = {'terminate_pids': [12345, 12346, 12347]}
        results = apply_actions(self.mock_config, actions)
        
        self.assertEqual(len(results), 3)
        self.assertTrue(all(r.action_type == 'terminate' for r in results))
    
    @patch('tuning_fork.testing_modules.pgsql.check_connections.get_db_connection')
    def test_apply_cancel_action(self, mock_get_db: Mock) -> None:
        """Test canceling a query."""
        self.mock_cursor.fetchone.return_value = (True,)
        mock_get_db.return_value.__enter__.return_value = (
            self.mock_connection, self.mock_cursor
        )
        
        actions = {'cancel_pids': [12345]}
        results = apply_actions(self.mock_config, actions)
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].action_type, 'cancel')
        self.assertEqual(results[0].pid, 12345)
        self.assertTrue(results[0].success)
    
    @patch('tuning_fork.testing_modules.pgsql.check_connections.get_db_connection')
    def test_apply_adjust_settings(self, mock_get_db: Mock) -> None:
        """Test adjusting connection settings."""
        self.mock_cursor.fetchone.return_value = ('100',)  # Current max_connections
        mock_get_db.return_value.__enter__.return_value = (
            self.mock_connection, self.mock_cursor
        )
        
        actions = {'adjust_settings': {'max_connections': '200'}}
        results = apply_actions(self.mock_config, actions)
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].action_type, 'adjust_limit')
        self.assertEqual(results[0].setting_name, 'max_connections')
        self.assertEqual(results[0].old_value, '100')
        self.assertEqual(results[0].new_value, '200')
        self.assertTrue(results[0].success)
    
    @patch('tuning_fork.testing_modules.pgsql.check_connections.get_db_connection')
    def test_apply_mixed_actions(self, mock_get_db: Mock) -> None:
        """Test applying multiple action types."""
        self.mock_cursor.fetchone.side_effect = [
            (True,),  # terminate success
            (True,),  # cancel success
            ('100',)  # current max_connections
        ]
        mock_get_db.return_value.__enter__.return_value = (
            self.mock_connection, self.mock_cursor
        )
        
        actions = {
            'terminate_pids': [12345],
            'cancel_pids': [12346],
            'adjust_settings': {'max_connections': '200'}
        }
        results = apply_actions(self.mock_config, actions)
        
        self.assertEqual(len(results), 3)
        action_types = {r.action_type for r in results}
        self.assertEqual(action_types, {'terminate', 'cancel', 'adjust_limit'})
    
    @patch('tuning_fork.testing_modules.pgsql.check_connections.get_db_connection')
    def test_apply_action_failure(self, mock_get_db: Mock) -> None:
        """Test handling action failures."""
        self.mock_cursor.fetchone.side_effect = Exception("Permission denied")
        mock_get_db.return_value.__enter__.return_value = (
            self.mock_connection, self.mock_cursor
        )
        
        actions = {'terminate_pids': [12345]}
        results = apply_actions(self.mock_config, actions)
        
        self.assertEqual(len(results), 1)
        self.assertFalse(results[0].success)
    
    @patch('tuning_fork.testing_modules.pgsql.check_connections.get_db_connection')
    def test_apply_empty_actions(self, mock_get_db: Mock) -> None:
        """Test applying empty actions."""
        mock_get_db.return_value.__enter__.return_value = (
            self.mock_connection, self.mock_cursor
        )
        
        actions = {}
        results = apply_actions(self.mock_config, actions)
        
        self.assertEqual(len(results), 0)
    
    @patch('tuning_fork.testing_modules.pgsql.check_connections.get_db_connection')
    def test_apply_actions_database_error(self, mock_get_db: Mock) -> None:
        """Test handling database connection errors."""
        mock_get_db.return_value.__enter__.side_effect = Exception("Connection failed")
        
        with self.assertRaises(RuntimeError) as context:
            apply_actions(self.mock_config, {'terminate_pids': [12345]})
        
        self.assertIn("Action application failed", str(context.exception))


class TestRollbackActions(unittest.TestCase):
    """Test suite for rollback_actions function."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        self.mock_config = MagicMock(spec=Config)
        self.mock_cursor = MagicMock()
        self.mock_connection = MagicMock()
    
    @patch('tuning_fork.testing_modules.pgsql.check_connections.get_db_connection')
    def test_rollback_adjust_setting(self, mock_get_db: Mock) -> None:
        """Test rolling back a setting adjustment."""
        mock_get_db.return_value.__enter__.return_value = (
            self.mock_connection, self.mock_cursor
        )
        
        original_action = ConnectionAction(
            action_type='adjust_limit',
            pid=None,
            setting_name='max_connections',
            old_value='100',
            new_value='200',
            timestamp='2025-01-01T00:00:00',
            action_id=1,
            success=True
        )
        
        results = rollback_actions(self.mock_config, [original_action])
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].action_type, 'rollback_adjust_limit')
        self.assertEqual(results[0].old_value, '200')
        self.assertEqual(results[0].new_value, '100')
        self.assertTrue(results[0].success)
    
    @patch('tuning_fork.testing_modules.pgsql.check_connections.get_db_connection')
    def test_rollback_multiple_settings(self, mock_get_db: Mock) -> None:
        """Test rolling back multiple setting adjustments."""
        mock_get_db.return_value.__enter__.return_value = (
            self.mock_connection, self.mock_cursor
        )
        
        actions = [
            ConnectionAction(
                action_type='adjust_limit',
                pid=None,
                setting_name='max_connections',
                old_value='100',
                new_value='200',
                timestamp='2025-01-01T00:00:00',
                action_id=1,
                success=True
            ),
            ConnectionAction(
                action_type='adjust_limit',
                pid=None,
                setting_name='shared_buffers',
                old_value='128MB',
                new_value='256MB',
                timestamp='2025-01-01T00:00:01',
                action_id=2,
                success=True
            )
        ]
        
        results = rollback_actions(self.mock_config, actions)
        
        self.assertEqual(len(results), 2)
        self.assertTrue(all(r.action_type == 'rollback_adjust_limit' for r in results))
    
    @patch('tuning_fork.testing_modules.pgsql.check_connections.get_db_connection')
    def test_rollback_terminate_not_allowed(self, mock_get_db: Mock) -> None:
        """Test that terminate actions cannot be rolled back."""
        mock_get_db.return_value.__enter__.return_value = (
            self.mock_connection, self.mock_cursor
        )
        
        terminate_action = ConnectionAction(
            action_type='terminate',
            pid=12345,
            setting_name=None,
            old_value='running',
            new_value='terminated',
            timestamp='2025-01-01T00:00:00',
            action_id=1,
            success=True
        )
        
        results = rollback_actions(self.mock_config, [terminate_action])
        
        # No rollback should be created for terminate actions
        self.assertEqual(len(results), 0)
    
    @patch('tuning_fork.testing_modules.pgsql.check_connections.get_db_connection')
    def test_rollback_cancel_not_allowed(self, mock_get_db: Mock) -> None:
        """Test that cancel actions cannot be rolled back."""
        mock_get_db.return_value.__enter__.return_value = (
            self.mock_connection, self.mock_cursor
        )
        
        cancel_action = ConnectionAction(
            action_type='cancel',
            pid=12345,
            setting_name=None,
            old_value='running',
            new_value='canceled',
            timestamp='2025-01-01T00:00:00',
            action_id=1,
            success=True
        )
        
        results = rollback_actions(self.mock_config, [cancel_action])
        
        # No rollback should be created for cancel actions
        self.assertEqual(len(results), 0)
    
    @patch('tuning_fork.testing_modules.pgsql.check_connections.get_db_connection')
    def test_rollback_already_rolled_back(self, mock_get_db: Mock) -> None:
        """Test skipping already rolled back actions."""
        mock_get_db.return_value.__enter__.return_value = (
            self.mock_connection, self.mock_cursor
        )
        
        already_rolled_back = ConnectionAction(
            action_type='adjust_limit',
            pid=None,
            setting_name='max_connections',
            old_value='100',
            new_value='200',
            timestamp='2025-01-01T00:00:00',
            action_id=1,
            success=True,
            rollback_status=True
        )
        
        results = rollback_actions(self.mock_config, [already_rolled_back])
        
        # Should skip already rolled back actions
        self.assertEqual(len(results), 0)
    
    @patch('tuning_fork.testing_modules.pgsql.check_connections.get_db_connection')
    def test_rollback_failed_action(self, mock_get_db: Mock) -> None:
        """Test not rolling back failed actions."""
        mock_get_db.return_value.__enter__.return_value = (
            self.mock_connection, self.mock_cursor
        )
        
        failed_action = ConnectionAction(
            action_type='adjust_limit',
            pid=None,
            setting_name='max_connections',
            old_value='100',
            new_value='200',
            timestamp='2025-01-01T00:00:00',
            action_id=1,
            success=False  # Action failed
        )
        
        results = rollback_actions(self.mock_config, [failed_action])
        
        # Should not rollback failed actions
        self.assertEqual(len(results), 0)
    
    @patch('tuning_fork.testing_modules.pgsql.check_connections.get_db_connection')
    def test_rollback_failure(self, mock_get_db: Mock) -> None:
        """Test handling rollback failures."""
        self.mock_cursor.execute.side_effect = Exception("Permission denied")
        mock_get_db.return_value.__enter__.return_value = (
            self.mock_connection, self.mock_cursor
        )
        
        action = ConnectionAction(
            action_type='adjust_limit',
            pid=None,
            setting_name='max_connections',
            old_value='100',
            new_value='200',
            timestamp='2025-01-01T00:00:00',
            action_id=1,
            success=True
        )
        
        results = rollback_actions(self.mock_config, [action])
        
        self.assertEqual(len(results), 1)
        self.assertFalse(results[0].success)
    
    @patch('tuning_fork.testing_modules.pgsql.check_connections.get_db_connection')
    def test_rollback_database_error(self, mock_get_db: Mock) -> None:
        """Test handling database connection errors during rollback."""
        mock_get_db.return_value.__enter__.side_effect = Exception("Connection failed")
        
        action = ConnectionAction(
            action_type='adjust_limit',
            pid=None,
            setting_name='max_connections',
            old_value='100',
            new_value='200',
            timestamp='2025-01-01T00:00:00',
            action_id=1,
            success=True
        )
        
        with self.assertRaises(RuntimeError) as context:
            rollback_actions(self.mock_config, [action])
        
        self.assertIn("Rollback failed", str(context.exception))
    
    @patch('tuning_fork.testing_modules.pgsql.check_connections.get_db_connection')
    def test_rollback_mixed_actions(self, mock_get_db: Mock) -> None:
        """Test rolling back mixed action types."""
        mock_get_db.return_value.__enter__.return_value = (
            self.mock_connection, self.mock_cursor
        )
        
        actions = [
            ConnectionAction(
                action_type='adjust_limit',
                pid=None,
                setting_name='max_connections',
                old_value='100',
                new_value='200',
                timestamp='2025-01-01T00:00:00',
                action_id=1,
                success=True
            ),
            ConnectionAction(
                action_type='terminate',
                pid=12345,
                setting_name=None,
                old_value='running',
                new_value='terminated',
                timestamp='2025-01-01T00:00:01',
                action_id=2,
                success=True
            ),
            ConnectionAction(
                action_type='cancel',
                pid=12346,
                setting_name=None,
                old_value='running',
                new_value='canceled',
                timestamp='2025-01-01T00:00:02',
                action_id=3,
                success=True
            )
        ]
        
        results = rollback_actions(self.mock_config, actions)
        
        # Only the adjust_limit action should be rolled back
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].action_type, 'rollback_adjust_limit')


class TestConnectionThresholds(unittest.TestCase):
    """Test suite for connection threshold constants."""
    
    def test_thresholds_exist(self) -> None:
        """Test that all required thresholds are defined."""
        required_thresholds = [
            'max_connections_usage_warning',
            'max_connections_usage_critical',
            'idle_in_transaction_warning',
            'idle_in_transaction_critical',
            'long_running_query_warning',
            'long_running_query_critical',
            'blocking_query_warning',
            'blocking_query_critical'
        ]
        
        for threshold in required_thresholds:
            self.assertIn(threshold, CONNECTION_THRESHOLDS)
    
    def test_threshold_values_reasonable(self) -> None:
        """Test that threshold values are reasonable."""
        # Warning should be less than critical
        self.assertLess(
            CONNECTION_THRESHOLDS['max_connections_usage_warning'],
            CONNECTION_THRESHOLDS['max_connections_usage_critical']
        )
        self.assertLess(
            CONNECTION_THRESHOLDS['idle_in_transaction_warning'],
            CONNECTION_THRESHOLDS['idle_in_transaction_critical']
        )
        self.assertLess(
            CONNECTION_THRESHOLDS['long_running_query_warning'],
            CONNECTION_THRESHOLDS['long_running_query_critical']
        )
        self.assertLess(
            CONNECTION_THRESHOLDS['blocking_query_warning'],
            CONNECTION_THRESHOLDS['blocking_query_critical']
        )
        
        # Connection usage ratios should be between 0 and 1
        self.assertGreater(CONNECTION_THRESHOLDS['max_connections_usage_warning'], 0)
        self.assertLess(CONNECTION_THRESHOLDS['max_connections_usage_warning'], 1)
        self.assertGreater(CONNECTION_THRESHOLDS['max_connections_usage_critical'], 0)
        self.assertLess(CONNECTION_THRESHOLDS['max_connections_usage_critical'], 1)


class TestIntegrationScenarios(unittest.TestCase):
    """Integration tests for realistic connection scenarios."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        self.mock_config = MagicMock(spec=Config)
        self.mock_cursor = MagicMock()
        self.mock_connection = MagicMock()
    
    @patch('tuning_fork.testing_modules.pgsql.check_connections.get_db_connection')
    def test_full_workflow_check_report_apply_rollback(
        self,
        mock_get_db: Mock
    ) -> None:
        """Test complete workflow: check -> report -> apply -> rollback."""
        # Setup mocks for check_connections
        self.mock_cursor.fetchone.side_effect = [
            ('100',),  # max_connections
            (95,),     # active connections (critical)
        ]
        self.mock_cursor.fetchall.return_value = []
        
        mock_get_db.return_value.__enter__.return_value = (
            self.mock_connection, self.mock_cursor
        )
        
        # Step 1: Check connections
        check_results = check_connections(self.mock_config)
        self.assertGreater(len(check_results), 0)
        
        # Step 2: Generate report
        report = report_connections(check_results, format='text')
        self.assertIn('CRITICAL', report)
        
        # Step 3: Apply actions (increase max_connections)
        self.mock_cursor.fetchone.side_effect = [('100',)]
        actions = {'adjust_settings': {'max_connections': '200'}}
        apply_results = apply_actions(self.mock_config, actions)
        self.assertEqual(len(apply_results), 1)
        self.assertTrue(apply_results[0].success)
        
        # Step 4: Rollback actions
        rollback_results = rollback_actions(self.mock_config, apply_results)
        self.assertEqual(len(rollback_results), 1)
        self.assertTrue(rollback_results[0].success)
    
    @patch('tuning_fork.testing_modules.pgsql.check_connections.get_db_connection')
    def test_blocking_chain_detection(self, mock_get_db: Mock) -> None:
        """Test detecting a chain of blocking queries."""
        self.mock_cursor.fetchone.side_effect = [
            ('100',),  # max_connections
            (50,),     # active connections
        ]
        
        # Mock a blocking chain: PID 3 blocked by PID 2, PID 2 blocked by PID 1
        blocking_data = [
            (
                12347, 'user1', 'db1', 'app1', 'active',
                'SELECT * FROM table1', 180.0, 'Lock', 'relation',
                12346, 'user2', 'UPDATE table1', 200.0
            ),
            (
                12346, 'user2', 'db1', 'app1', 'active',
                'UPDATE table1', 200.0, 'Lock', 'relation',
                12345, 'user3', 'DELETE FROM table1', 300.0
            )
        ]
        
        self.mock_cursor.fetchall.side_effect = [
            blocking_data,  # blocking queries
            [],  # idle transactions
            [],  # long running queries
            []   # wait events
        ]
        
        mock_get_db.return_value.__enter__.return_value = (
            self.mock_connection, self.mock_cursor
        )
        
        results = check_connections(self.mock_config)
        
        blocking_results = [r for r in results if r.check_type == 'blocking_query']
        self.assertEqual(len(blocking_results), 2)
        
        # Verify chain is detected
        pids = {r.pid for r in blocking_results}
        self.assertEqual(pids, {12347, 12346})


if __name__ == '__main__':
    unittest.main()