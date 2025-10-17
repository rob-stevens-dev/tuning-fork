"""
Unit tests for module_runner module.

This test suite ensures comprehensive coverage of the module runner
and orchestration functionality for all testing modules.
"""

import unittest
from io import StringIO
from typing import Any
from unittest.mock import MagicMock, Mock, patch

from tuning_fork.config import Config
from tuning_fork.module_runner import ModuleRunner, ModuleRunnerError
from tuning_fork.testing_modules.pgsql import CheckResult
from tuning_fork.testing_modules.pgsql.check_connections import (
    ConnectionCheckResult,
)


class TestModuleRunnerBasics(unittest.TestCase):
    """Test suite for basic ModuleRunner functionality."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        self.mock_config = MagicMock(spec=Config)
        self.mock_config.config = {
            'testing_modules': {
                'pgsql': {
                    'enabled': True,
                    'modules': ['check_settings']
                },
                'mysql_mariadb': {
                    'enabled': False,
                    'modules': []
                }
            }
        }
        
        # Mock config methods
        def mock_get(key: str, default: Any = None) -> Any:
            keys = key.split('.')
            value = self.mock_config.config
            for k in keys:
                if isinstance(value, dict):
                    value = value.get(k, default)
                else:
                    return default
            return value
        
        def mock_has(key: str) -> bool:
            return mock_get(key) is not None
        
        self.mock_config.get.side_effect = mock_get
        self.mock_config.has.side_effect = mock_has
    
    def test_init_with_valid_config(self) -> None:
        """Test initialization with valid configuration."""
        runner = ModuleRunner(self.mock_config)
        
        self.assertIsNotNone(runner)
        self.assertEqual(runner.config, self.mock_config)
    
    def test_init_without_testing_modules_config(self) -> None:
        """Test initialization without testing_modules configuration."""
        self.mock_config.config = {}
        
        with self.assertRaises(ModuleRunnerError) as context:
            ModuleRunner(self.mock_config)
        
        self.assertIn("No testing_modules configuration", str(context.exception))
    
    def test_get_enabled_modules_pgsql(self) -> None:
        """Test getting enabled modules for PostgreSQL."""
        runner = ModuleRunner(self.mock_config)
        
        modules = runner.get_enabled_modules('pgsql')
        
        self.assertEqual(len(modules), 1)
        self.assertIn('check_settings', modules)
    
    def test_get_enabled_modules_disabled_database(self) -> None:
        """Test getting modules for disabled database type."""
        runner = ModuleRunner(self.mock_config)
        
        modules = runner.get_enabled_modules('mysql_mariadb')
        
        self.assertEqual(len(modules), 0)
    
    def test_get_enabled_modules_nonexistent_database(self) -> None:
        """Test getting modules for non-existent database type."""
        runner = ModuleRunner(self.mock_config)
        
        modules = runner.get_enabled_modules('oracle')
        
        self.assertEqual(len(modules), 0)
    
    def test_get_enabled_modules_multiple(self) -> None:
        """Test getting multiple enabled modules."""
        self.mock_config.config['testing_modules']['pgsql']['modules'] = [
            'check_settings',
            'check_connections',
            'check_performance'
        ]
        
        runner = ModuleRunner(self.mock_config)
        modules = runner.get_enabled_modules('pgsql')
        
        self.assertEqual(len(modules), 3)
        self.assertIn('check_settings', modules)
        self.assertIn('check_connections', modules)
        self.assertIn('check_performance', modules)


class TestModuleRunnerCheckSettings(unittest.TestCase):
    """Test suite for ModuleRunner with check_settings module."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        self.mock_config = MagicMock(spec=Config)
        self.mock_config.config = {
            'testing_modules': {
                'pgsql': {
                    'enabled': True,
                    'modules': ['check_settings'],
                    'check_settings': {
                        'workload_type': 'OLTP',
                        'report_format': 'text'
                    }
                }
            }
        }
        
        def mock_get(key: str, default: Any = None) -> Any:
            keys = key.split('.')
            value = self.mock_config.config
            for k in keys:
                if isinstance(value, dict):
                    value = value.get(k, default)
                else:
                    return default
            return value
        
        def mock_has(key: str) -> bool:
            return mock_get(key) is not None
        
        self.mock_config.get.side_effect = mock_get
        self.mock_config.has.side_effect = mock_has
    
    @patch('tuning_fork.testing_modules.pgsql.report_settings')
    @patch('tuning_fork.testing_modules.pgsql.check_settings')
    def test_run_pgsql_module_check_settings(
        self,
        mock_check: Mock,
        mock_report: Mock
    ) -> None:
        """Test running check_settings module."""
        mock_results = [
            CheckResult(
                setting_name='max_connections',
                current_value=100,
                recommended_value=100,
                status='OK',
                message='Optimal',
                impact='Connections'
            )
        ]
        mock_check.return_value = mock_results
        mock_report.return_value = "Test report"
        
        runner = ModuleRunner(self.mock_config)
        result = runner.run_pgsql_module('check_settings')
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['module'], 'check_settings')
        self.assertEqual(result['results'], mock_results)
        self.assertEqual(result['report'], "Test report")
        self.assertEqual(result['workload_type'], 'OLTP')
        
        mock_check.assert_called_once_with(self.mock_config, workload_type='OLTP')
        mock_report.assert_called_once()
    
    def test_run_pgsql_module_unknown_module(self) -> None:
        """Test running unknown PostgreSQL module."""
        runner = ModuleRunner(self.mock_config)
        
        result = runner.run_pgsql_module('unknown_module')
        
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['module'], 'unknown_module')
        self.assertIn('Unknown PostgreSQL module', result['error'])
    
    @patch('tuning_fork.testing_modules.pgsql.check_settings')
    def test_run_pgsql_module_with_exception(self, mock_check: Mock) -> None:
        """Test handling of exceptions during module execution."""
        mock_check.side_effect = Exception("Database connection failed")
        
        runner = ModuleRunner(self.mock_config)
        result = runner.run_pgsql_module('check_settings')
        
        self.assertEqual(result['status'], 'error')
        self.assertIn('Database connection failed', result['error'])
    
    @patch('tuning_fork.testing_modules.pgsql.report_settings')
    @patch('tuning_fork.testing_modules.pgsql.check_settings')
    def test_run_pgsql_module_with_custom_report_format(
        self,
        mock_check: Mock,
        mock_report: Mock
    ) -> None:
        """Test running module with custom report format from config."""
        self.mock_config.config['testing_modules']['pgsql']['check_settings'] = {
            'report_format': 'json'
        }
        
        mock_check.return_value = []
        mock_report.return_value = '[]'
        
        runner = ModuleRunner(self.mock_config)
        result = runner.run_pgsql_module('check_settings')
        
        mock_report.assert_called_once()
        call_args = mock_report.call_args
        self.assertEqual(call_args[1]['format'], 'json')


class TestModuleRunnerCheckConnections(unittest.TestCase):
    """Test suite for ModuleRunner with check_connections module."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        self.mock_config = MagicMock(spec=Config)
        self.mock_config.config = {
            'testing_modules': {
                'pgsql': {
                    'enabled': True,
                    'modules': ['check_connections'],
                    'check_connections': {
                        'report_format': 'text',
                        'include_ok': False
                    }
                }
            }
        }
        
        def mock_get(key: str, default: Any = None) -> Any:
            keys = key.split('.')
            value = self.mock_config.config
            for k in keys:
                if isinstance(value, dict):
                    value = value.get(k, default)
                else:
                    return default
            return value
        
        def mock_has(key: str) -> bool:
            return mock_get(key) is not None
        
        self.mock_config.get.side_effect = mock_get
        self.mock_config.has.side_effect = mock_has
    
    @patch('tuning_fork.testing_modules.pgsql.report_connections')
    @patch('tuning_fork.testing_modules.pgsql.check_connections')
    def test_run_check_connections_module(
        self,
        mock_check: Mock,
        mock_report: Mock
    ) -> None:
        """Test running check_connections module."""
        mock_results = [
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
                status='WARNING',
                message='Connection pool at 85% capacity',
                recommendation='Consider increasing max_connections'
            )
        ]
        mock_check.return_value = mock_results
        mock_report.return_value = "Test report"
        
        runner = ModuleRunner(self.mock_config)
        result = runner.run_pgsql_module('check_connections')
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['module'], 'check_connections')
        self.assertEqual(result['results'], mock_results)
        self.assertEqual(result['report'], "Test report")
        self.assertEqual(result['total_findings'], 1)
        self.assertEqual(result['warning_count'], 1)
        self.assertEqual(result['critical_count'], 0)
        
        mock_check.assert_called_once_with(self.mock_config, thresholds=None)
        mock_report.assert_called_once_with(
            mock_results,
            format='text',
            include_ok=False
        )
    
    @patch('tuning_fork.testing_modules.pgsql.report_connections')
    @patch('tuning_fork.testing_modules.pgsql.check_connections')
    def test_run_check_connections_with_custom_thresholds(
        self,
        mock_check: Mock,
        mock_report: Mock
    ) -> None:
        """Test running check_connections with custom thresholds."""
        custom_thresholds = {
            'max_connections_usage_warning': 0.70,
            'idle_in_transaction_warning': 180
        }
        
        self.mock_config.config['testing_modules']['pgsql']['check_connections']['thresholds'] = custom_thresholds
        
        mock_check.return_value = []
        mock_report.return_value = "No issues found"
        
        runner = ModuleRunner(self.mock_config)
        result = runner.run_pgsql_module('check_connections')
        
        self.assertEqual(result['status'], 'success')
        mock_check.assert_called_once_with(
            self.mock_config,
            thresholds=custom_thresholds
        )
    
    @patch('tuning_fork.testing_modules.pgsql.report_connections')
    @patch('tuning_fork.testing_modules.pgsql.check_connections')
    def test_run_check_connections_json_format(
        self,
        mock_check: Mock,
        mock_report: Mock
    ) -> None:
        """Test running check_connections with JSON format."""
        self.mock_config.config['testing_modules']['pgsql']['check_connections']['report_format'] = 'json'
        
        mock_check.return_value = []
        mock_report.return_value = '{"results": []}'
        
        runner = ModuleRunner(self.mock_config)
        result = runner.run_pgsql_module('check_connections')
        
        self.assertEqual(result['status'], 'success')
        mock_report.assert_called_once_with(
            [],
            format='json',
            include_ok=False
        )
    
    @patch('tuning_fork.testing_modules.pgsql.report_connections')
    @patch('tuning_fork.testing_modules.pgsql.check_connections')
    def test_run_check_connections_include_ok(
        self,
        mock_check: Mock,
        mock_report: Mock
    ) -> None:
        """Test running check_connections with include_ok=True."""
        self.mock_config.config['testing_modules']['pgsql']['check_connections']['include_ok'] = True
        
        mock_check.return_value = []
        mock_report.return_value = "All checks passed"
        
        runner = ModuleRunner(self.mock_config)
        result = runner.run_pgsql_module('check_connections')
        
        self.assertEqual(result['status'], 'success')
        mock_report.assert_called_once_with(
            [],
            format='text',
            include_ok=True
        )
    
    @patch('tuning_fork.testing_modules.pgsql.check_connections')
    def test_run_check_connections_with_exception(self, mock_check: Mock) -> None:
        """Test handling of exceptions during check_connections execution."""
        mock_check.side_effect = Exception("Database connection failed")
        
        runner = ModuleRunner(self.mock_config)
        result = runner.run_pgsql_module('check_connections')
        
        self.assertEqual(result['status'], 'error')
        self.assertIn('Database connection failed', result['error'])
    
    @patch('tuning_fork.testing_modules.pgsql.report_connections')
    @patch('tuning_fork.testing_modules.pgsql.check_connections')
    def test_run_check_connections_with_multiple_findings(
        self,
        mock_check: Mock,
        mock_report: Mock
    ) -> None:
        """Test running check_connections with multiple findings."""
        mock_results = [
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
                status='CRITICAL',
                message='Connection pool at 98% capacity',
                recommendation='Increase max_connections immediately'
            ),
            ConnectionCheckResult(
                check_type='blocking_query',
                pid=12345,
                database='testdb',
                username='testuser',
                application_name='test_app',
                state='active',
                query='SELECT * FROM users',
                duration_seconds=180.0,
                wait_event_type='Lock',
                wait_event='relation',
                blocked_by=[12346],
                blocking=[],
                status='WARNING',
                message='PID 12345 blocked by PID 12346',
                recommendation='Review blocking query'
            ),
            ConnectionCheckResult(
                check_type='idle_in_transaction',
                pid=12347,
                database='testdb',
                username='testuser',
                application_name='test_app',
                state='idle in transaction',
                query='BEGIN',
                duration_seconds=1800.0,
                wait_event_type=None,
                wait_event=None,
                blocked_by=[],
                blocking=[],
                status='CRITICAL',
                message='Connection idle in transaction for 1800s',
                recommendation='Close transaction'
            )
        ]
        
        mock_check.return_value = mock_results
        mock_report.return_value = "Multiple issues found"
        
        runner = ModuleRunner(self.mock_config)
        result = runner.run_pgsql_module('check_connections')
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['total_findings'], 3)
        self.assertEqual(result['critical_count'], 2)
        self.assertEqual(result['warning_count'], 1)
    
    @patch('tuning_fork.testing_modules.pgsql.report_connections')
    @patch('tuning_fork.testing_modules.pgsql.check_connections')
    def test_default_configuration_values(
        self,
        mock_check: Mock,
        mock_report: Mock
    ) -> None:
        """Test that default configuration values are used when not specified."""
        self.mock_config.config['testing_modules']['pgsql']['check_connections'] = {}
        
        mock_check.return_value = []
        mock_report.return_value = "Default config used"
        
        runner = ModuleRunner(self.mock_config)
        result = runner.run_pgsql_module('check_connections')
        
        self.assertEqual(result['status'], 'success')
        
        mock_check.assert_called_once_with(self.mock_config, thresholds=None)
        mock_report.assert_called_once_with(
            [],
            format='text',
            include_ok=False
        )


class TestModuleRunnerMultipleModules(unittest.TestCase):
    """Test suite for running multiple modules together."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        self.mock_config = MagicMock(spec=Config)
        self.mock_config.config = {
            'testing_modules': {
                'pgsql': {
                    'enabled': True,
                    'modules': ['check_settings', 'check_connections'],
                    'check_settings': {
                        'workload_type': 'OLTP',
                        'report_format': 'text'
                    },
                    'check_connections': {
                        'report_format': 'text',
                        'include_ok': False
                    }
                },
                'mysql_mariadb': {
                    'enabled': False,
                    'modules': []
                }
            }
        }
        
        def mock_get(key: str, default: Any = None) -> Any:
            keys = key.split('.')
            value = self.mock_config.config
            for k in keys:
                if isinstance(value, dict):
                    value = value.get(k, default)
                else:
                    return default
            return value
        
        def mock_has(key: str) -> bool:
            return mock_get(key) is not None
        
        self.mock_config.get.side_effect = mock_get
        self.mock_config.has.side_effect = mock_has
    
    @patch('tuning_fork.testing_modules.pgsql.report_connections')
    @patch('tuning_fork.testing_modules.pgsql.report_settings')
    @patch('tuning_fork.testing_modules.pgsql.check_connections')
    @patch('tuning_fork.testing_modules.pgsql.check_settings')
    def test_run_modules_pgsql(
        self,
        mock_check_settings: Mock,
        mock_check_connections: Mock,
        mock_report_settings: Mock,
        mock_report_connections: Mock
    ) -> None:
        """Test running all modules for PostgreSQL."""
        mock_check_settings.return_value = []
        mock_check_connections.return_value = []
        mock_report_settings.return_value = "Settings Report"
        mock_report_connections.return_value = "Connections Report"
        
        runner = ModuleRunner(self.mock_config)
        results = runner.run_modules('pgsql')
        
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['module'], 'check_settings')
        self.assertEqual(results[1]['module'], 'check_connections')
        self.assertTrue(all(r['status'] == 'success' for r in results))
    
    def test_run_modules_no_enabled_modules(self) -> None:
        """Test running modules when none are enabled."""
        runner = ModuleRunner(self.mock_config)
        results = runner.run_modules('mysql_mariadb')
        
        self.assertEqual(len(results), 0)
    
    def test_run_modules_unsupported_database_type(self) -> None:
        """Test running modules for unsupported database type."""
        self.mock_config.config['testing_modules']['oracle'] = {
            'enabled': True,
            'modules': ['check_settings']
        }
        
        runner = ModuleRunner(self.mock_config)
        results = runner.run_modules('oracle')
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['status'], 'error')
        self.assertIn('Unsupported database type', results[0]['error'])
    
    @patch('tuning_fork.testing_modules.pgsql.report_settings')
    @patch('tuning_fork.testing_modules.pgsql.check_settings')
    def test_run_modules_multiple_same_module(
        self,
        mock_check: Mock,
        mock_report: Mock
    ) -> None:
        """Test running the same module multiple times."""
        self.mock_config.config['testing_modules']['pgsql']['modules'] = [
            'check_settings',
            'check_settings'
        ]
        
        mock_check.return_value = []
        mock_report.return_value = "Report"
        
        runner = ModuleRunner(self.mock_config)
        results = runner.run_modules('pgsql')
        
        self.assertEqual(len(results), 2)
        self.assertTrue(all(r['status'] == 'success' for r in results))
    
    @patch('tuning_fork.testing_modules.pgsql.report_connections')
    @patch('tuning_fork.testing_modules.pgsql.report_settings')
    @patch('tuning_fork.testing_modules.pgsql.check_connections')
    @patch('tuning_fork.testing_modules.pgsql.check_settings')
    def test_run_modules_with_partial_failure(
        self,
        mock_check_settings: Mock,
        mock_check_connections: Mock,
        mock_report_settings: Mock,
        mock_report_connections: Mock
    ) -> None:
        """Test running modules when some fail."""
        mock_check_settings.return_value = []
        mock_check_connections.side_effect = Exception("Connection check failed")
        mock_report_settings.return_value = "Settings OK"
        
        runner = ModuleRunner(self.mock_config)
        results = runner.run_modules('pgsql')
        
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['status'], 'success')
        self.assertEqual(results[1]['status'], 'error')
        self.assertIn('Connection check failed', results[1]['error'])
    
    @patch('tuning_fork.testing_modules.pgsql.report_connections')
    @patch('tuning_fork.testing_modules.pgsql.report_settings')
    @patch('tuning_fork.testing_modules.pgsql.check_connections')
    @patch('tuning_fork.testing_modules.pgsql.check_settings')
    def test_run_all_single_database(
        self,
        mock_check_settings: Mock,
        mock_check_connections: Mock,
        mock_report_settings: Mock,
        mock_report_connections: Mock
    ) -> None:
        """Test running all modules for all database types."""
        mock_check_settings.return_value = []
        mock_check_connections.return_value = []
        mock_report_settings.return_value = "Settings Report"
        mock_report_connections.return_value = "Connections Report"
        
        runner = ModuleRunner(self.mock_config)
        all_results = runner.run_all()
        
        self.assertIn('pgsql', all_results)
        self.assertEqual(len(all_results['pgsql']), 2)
        self.assertTrue(all(r['status'] == 'success' for r in all_results['pgsql']))
    
    def test_run_modules_exception_during_module_call(self) -> None:
        """Test handling exception raised during module execution call."""
        self.mock_config.config['testing_modules']['pgsql']['modules'] = ['check_settings']
        
        runner = ModuleRunner(self.mock_config)
        
        # Mock run_pgsql_module to raise an exception
        with patch.object(runner, 'run_pgsql_module', side_effect=Exception("Unexpected error")):
            results = runner.run_modules('pgsql')
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['status'], 'error')
        self.assertIn('Unexpected error', results[0]['error'])


class TestModuleRunnerMySQL(unittest.TestCase):
    """Test suite for ModuleRunner MySQL/MariaDB support."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        self.mock_config = MagicMock(spec=Config)
        self.mock_config.config = {
            'testing_modules': {
                'mysql_mariadb': {
                    'enabled': True,
                    'modules': ['check_settings']
                }
            }
        }
        
        def mock_get(key: str, default: Any = None) -> Any:
            keys = key.split('.')
            value = self.mock_config.config
            for k in keys:
                if isinstance(value, dict):
                    value = value.get(k, default)
                else:
                    return default
            return value
        
        def mock_has(key: str) -> bool:
            return mock_get(key) is not None
        
        self.mock_config.get.side_effect = mock_get
        self.mock_config.has.side_effect = mock_has
    
    def test_run_mysql_module_not_implemented(self) -> None:
        """Test running MySQL module (not yet implemented)."""
        runner = ModuleRunner(self.mock_config)
        result = runner.run_mysql_module('check_settings')
        
        self.assertEqual(result['status'], 'error')
        self.assertIn('not yet implemented', result['error'])
    
    def test_run_modules_mysql(self) -> None:
        """Test running modules for MySQL returns not implemented."""
        runner = ModuleRunner(self.mock_config)
        results = runner.run_modules('mysql_mariadb')
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['status'], 'error')
        self.assertIn('not yet implemented', results[0]['error'])


class TestModuleRunnerReporting(unittest.TestCase):
    """Test suite for ModuleRunner reporting functionality."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        self.mock_config = MagicMock(spec=Config)
        self.mock_config.config = {
            'testing_modules': {
                'pgsql': {
                    'enabled': True,
                    'modules': ['check_settings']
                }
            }
        }
        
        def mock_get(key: str, default: Any = None) -> Any:
            keys = key.split('.')
            value = self.mock_config.config
            for k in keys:
                if isinstance(value, dict):
                    value = value.get(k, default)
                else:
                    return default
            return value
        
        def mock_has(key: str) -> bool:
            return mock_get(key) is not None
        
        self.mock_config.get.side_effect = mock_get
        self.mock_config.has.side_effect = mock_has
    
    def test_print_summary_success(self) -> None:
        """Test printing summary with successful results."""
        runner = ModuleRunner(self.mock_config)
        
        all_results = {
            'pgsql': [
                {
                    'module': 'check_settings',
                    'status': 'success',
                    'report': 'Summary: 0 critical, 0 warnings, 10 ok'
                }
            ]
        }
        
        import sys
        old_stdout = sys.stdout
        sys.stdout = StringIO()
        
        try:
            runner.print_summary(all_results)
            output = sys.stdout.getvalue()
        finally:
            sys.stdout = old_stdout
        
        self.assertIn('✓', output)
        self.assertIn('SUCCESS', output)
    
    def test_print_summary_error(self) -> None:
        """Test printing summary with error results."""
        runner = ModuleRunner(self.mock_config)
        
        all_results = {
            'pgsql': [
                {
                    'module': 'check_settings',
                    'status': 'error',
                    'error': 'Connection failed'
                }
            ]
        }
        
        import sys
        old_stdout = sys.stdout
        sys.stdout = StringIO()
        
        try:
            runner.print_summary(all_results)
            output = sys.stdout.getvalue()
        finally:
            sys.stdout = old_stdout
        
        self.assertIn('✗', output)
        self.assertIn('ERROR', output)
        self.assertIn('Connection failed', output)
    
    def test_print_summary_empty_results(self) -> None:
        """Test printing summary with no results."""
        runner = ModuleRunner(self.mock_config)
        
        import sys
        old_stdout = sys.stdout
        sys.stdout = StringIO()
        
        try:
            runner.print_summary({})
            output = sys.stdout.getvalue()
        finally:
            sys.stdout = old_stdout
        
        self.assertIn('No modules were executed', output)
    
    def test_print_summary_multiple_databases(self) -> None:
        """Test printing summary for multiple database types."""
        runner = ModuleRunner(self.mock_config)
        
        all_results = {
            'pgsql': [
                {
                    'module': 'check_settings',
                    'status': 'success',
                    'report': 'Summary: 1 critical, 0 warnings, 0 ok'
                }
            ],
            'mysql_mariadb': [
                {
                    'module': 'check_settings',
                    'status': 'success',
                    'report': 'Summary: 0 critical, 1 warnings, 0 ok'
                }
            ]
        }
        
        import sys
        old_stdout = sys.stdout
        sys.stdout = StringIO()
        
        try:
            runner.print_summary(all_results)
            output = sys.stdout.getvalue()
        finally:
            sys.stdout = old_stdout
        
        self.assertIn('PGSQL', output)
        self.assertIn('MYSQL_MARIADB', output)
        self.assertIn('1 critical', output)
        self.assertIn('1 warnings', output)


if __name__ == '__main__':
    unittest.main()