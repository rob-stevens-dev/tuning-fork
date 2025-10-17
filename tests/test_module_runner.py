"""
Unit tests for module_runner module.

This test suite ensures comprehensive coverage of the module runner
and orchestration functionality.
"""

import unittest
from io import StringIO
from typing import Any
from unittest.mock import MagicMock, Mock, patch

from tuning_fork.config import Config
from tuning_fork.module_runner import ModuleRunner, ModuleRunnerError
from tuning_fork.testing_modules.pgsql import CheckResult


class TestModuleRunner(unittest.TestCase):
    """Test suite for ModuleRunner class."""
    
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
    
    @patch('tuning_fork.testing_modules.pgsql.report_settings')
    @patch('tuning_fork.testing_modules.pgsql.check_settings')
    def test_run_pgsql_module_check_settings(
        self,
        mock_check: Mock,
        mock_report: Mock
    ) -> None:
        """Test running check_settings module."""
        # Mock check results
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
        
        # Verify report_settings was called with 'json' format
        mock_report.assert_called_once()
        call_args = mock_report.call_args
        self.assertEqual(call_args[1]['format'], 'json')
    
    @patch('tuning_fork.testing_modules.pgsql.report_settings')
    @patch('tuning_fork.testing_modules.pgsql.check_settings')
    def test_run_modules_pgsql(
        self,
        mock_check: Mock,
        mock_report: Mock
    ) -> None:
        """Test running all modules for PostgreSQL."""
        mock_check.return_value = []
        mock_report.return_value = "Report"
        
        runner = ModuleRunner(self.mock_config)
        results = runner.run_modules('pgsql')
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['status'], 'success')
        self.assertEqual(results[0]['module'], 'check_settings')
    
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
    def test_run_modules_multiple_modules(
        self,
        mock_check: Mock,
        mock_report: Mock
    ) -> None:
        """Test running multiple modules."""
        self.mock_config.config['testing_modules']['pgsql']['modules'] = [
            'check_settings',
            'check_settings'  # Run twice for testing
        ]
        
        mock_check.return_value = []
        mock_report.return_value = "Report"
        
        runner = ModuleRunner(self.mock_config)
        results = runner.run_modules('pgsql')
        
        self.assertEqual(len(results), 2)
        self.assertTrue(all(r['status'] == 'success' for r in results))
    
    @patch('tuning_fork.testing_modules.pgsql.report_settings')
    @patch('tuning_fork.testing_modules.pgsql.check_settings')
    def test_run_modules_with_partial_failure(
        self,
        mock_check: Mock,
        mock_report: Mock
    ) -> None:
        """Test running modules when some fail."""
        self.mock_config.config['testing_modules']['pgsql']['modules'] = [
            'check_settings',
            'unknown_module'
        ]
        
        mock_check.return_value = []
        mock_report.return_value = "Report"
        
        runner = ModuleRunner(self.mock_config)
        results = runner.run_modules('pgsql')
        
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['status'], 'success')
        self.assertEqual(results[1]['status'], 'error')
    
    @patch('tuning_fork.testing_modules.pgsql.report_settings')
    @patch('tuning_fork.testing_modules.pgsql.check_settings')
    def test_run_all_single_database(
        self,
        mock_check: Mock,
        mock_report: Mock
    ) -> None:
        """Test running all modules for all database types."""
        mock_check.return_value = []
        mock_report.return_value = "Report"
        
        runner = ModuleRunner(self.mock_config)
        all_results = runner.run_all()
        
        self.assertIn('pgsql', all_results)
        self.assertEqual(len(all_results['pgsql']), 1)
        self.assertNotIn('mysql_mariadb', all_results)  # Disabled, so not included
    
    @patch('tuning_fork.testing_modules.pgsql.report_settings')
    @patch('tuning_fork.testing_modules.pgsql.check_settings')
    def test_run_all_multiple_databases(
        self,
        mock_check: Mock,
        mock_report: Mock
    ) -> None:
        """Test running all modules for multiple database types."""
        self.mock_config.config['testing_modules']['mysql_mariadb'] = {
            'enabled': True,
            'modules': []
        }
        
        mock_check.return_value = []
        mock_report.return_value = "Report"
        
        runner = ModuleRunner(self.mock_config)
        all_results = runner.run_all()
        
        self.assertIn('pgsql', all_results)
        # mysql_mariadb enabled but no modules, so no results
    
    @patch('tuning_fork.testing_modules.pgsql.report_settings')
    @patch('tuning_fork.testing_modules.pgsql.check_settings')
    def test_print_summary_with_results(
        self,
        mock_check: Mock,
        mock_report: Mock
    ) -> None:
        """Test printing summary with successful results."""
        mock_check.return_value = []
        mock_report.return_value = "Summary: 0 critical, 0 warnings, 1 ok"
        
        runner = ModuleRunner(self.mock_config)
        all_results = runner.run_all()
        
        # Capture stdout
        import sys
        old_stdout = sys.stdout
        sys.stdout = StringIO()
        
        try:
            runner.print_summary(all_results)
            output = sys.stdout.getvalue()
        finally:
            sys.stdout = old_stdout
        
        self.assertIn('TUNING FORK', output)
        self.assertIn('PGSQL', output)
        self.assertIn('check_settings', output)
        self.assertIn('SUCCESS', output)
        self.assertIn('0 critical, 0 warnings, 1 ok', output)
    
    def test_print_summary_with_errors(self) -> None:
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
        
        # Capture stdout
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
        
        # Capture stdout
        import sys
        old_stdout = sys.stdout
        sys.stdout = StringIO()
        
        try:
            runner.print_summary({})
            output = sys.stdout.getvalue()
        finally:
            sys.stdout = old_stdout
        
        self.assertIn('No modules were executed', output)
    
    @patch('tuning_fork.testing_modules.pgsql.report_settings')
    @patch('tuning_fork.testing_modules.pgsql.check_settings')
    def test_print_summary_multiple_databases(
        self,
        mock_check: Mock,
        mock_report: Mock
    ) -> None:
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
        
        # Capture stdout
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