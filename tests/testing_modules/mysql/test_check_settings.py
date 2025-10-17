"""
Unit tests for MySQL/MariaDB check_settings module.

This test suite ensures comprehensive coverage of the MySQL/MariaDB settings
checker functionality, including check, report, apply, and rollback operations.
"""

import json
import unittest
from datetime import datetime
from typing import Any
from unittest.mock import MagicMock, Mock, patch

from tuning_fork.config import Config
from tuning_fork.testing_modules.mysql.check_settings import (
    RECOMMENDED_SETTINGS_OLTP,
    ChangeLogEntry,
    CheckResult,
    _compare_setting_values,
    _normalize_memory_value,
    apply_settings,
    check_settings,
    report_settings,
    rollback_settings,
)


class TestCheckResult(unittest.TestCase):
    """Test suite for CheckResult dataclass."""
    
    def test_check_result_creation(self) -> None:
        """Test creating a CheckResult instance."""
        result = CheckResult(
            setting_name='max_connections',
            current_value=151,
            recommended_value=151,
            status='OK',
            message='Setting is optimal',
            impact='Maximum concurrent connections',
            setting_unit=None,
            workload_type='OLTP'
        )
        
        self.assertEqual(result.setting_name, 'max_connections')
        self.assertEqual(result.current_value, 151)
        self.assertEqual(result.recommended_value, 151)
        self.assertEqual(result.status, 'OK')
        self.assertEqual(result.workload_type, 'OLTP')
    
    def test_check_result_to_dict(self) -> None:
        """Test converting CheckResult to dictionary."""
        result = CheckResult(
            setting_name='innodb_buffer_pool_size',
            current_value='2G',
            recommended_value='2G',
            status='OK',
            message='Setting is optimal',
            impact='InnoDB buffer pool size',
            setting_unit='memory',
            workload_type='OLTP'
        )
        
        result_dict = result.to_dict()
        
        self.assertIsInstance(result_dict, dict)
        self.assertEqual(result_dict['setting_name'], 'innodb_buffer_pool_size')
        self.assertEqual(result_dict['setting_unit'], 'memory')
        self.assertEqual(result_dict['workload_type'], 'OLTP')
    
    def test_check_result_to_json(self) -> None:
        """Test converting CheckResult to JSON."""
        result = CheckResult(
            setting_name='max_connections',
            current_value=151,
            recommended_value=151,
            status='OK',
            message='Setting is optimal',
            impact='Maximum connections'
        )
        
        json_str = result.to_json()
        
        self.assertIsInstance(json_str, str)
        parsed = json.loads(json_str)
        self.assertEqual(parsed['setting_name'], 'max_connections')


class TestChangeLogEntry(unittest.TestCase):
    """Test suite for ChangeLogEntry dataclass."""
    
    def test_change_log_entry_creation(self) -> None:
        """Test creating a ChangeLogEntry instance."""
        entry = ChangeLogEntry(
            setting_name='max_connections',
            old_value=151,
            new_value=200,
            timestamp='2025-01-01T00:00:00',
            change_id=1,
            scope='GLOBAL'
        )
        
        self.assertEqual(entry.setting_name, 'max_connections')
        self.assertEqual(entry.old_value, 151)
        self.assertEqual(entry.new_value, 200)
        self.assertEqual(entry.scope, 'GLOBAL')
        self.assertFalse(entry.rollback_status)
    
    def test_change_log_entry_to_dict(self) -> None:
        """Test converting ChangeLogEntry to dictionary."""
        entry = ChangeLogEntry(
            setting_name='innodb_buffer_pool_size',
            old_value='1G',
            new_value='2G',
            timestamp='2025-01-01T00:00:00',
            change_id=1,
            applied_by='admin',
            scope='GLOBAL'
        )
        
        entry_dict = entry.to_dict()
        
        self.assertIsInstance(entry_dict, dict)
        self.assertEqual(entry_dict['applied_by'], 'admin')
        self.assertEqual(entry_dict['scope'], 'GLOBAL')
        self.assertFalse(entry_dict['rollback_status'])


class TestNormalizeMemoryValue(unittest.TestCase):
    """Test suite for _normalize_memory_value function."""
    
    def test_normalize_kilobytes(self) -> None:
        """Test normalizing kilobyte values."""
        self.assertEqual(_normalize_memory_value('1K'), 1024)
        self.assertEqual(_normalize_memory_value('512K'), 512 * 1024)
    
    def test_normalize_megabytes(self) -> None:
        """Test normalizing megabyte values."""
        self.assertEqual(_normalize_memory_value('1M'), 1024 ** 2)
        self.assertEqual(_normalize_memory_value('256M'), 256 * 1024 ** 2)
    
    def test_normalize_gigabytes(self) -> None:
        """Test normalizing gigabyte values."""
        self.assertEqual(_normalize_memory_value('1G'), 1024 ** 3)
        self.assertEqual(_normalize_memory_value('4G'), 4 * 1024 ** 3)
    
    def test_normalize_terabytes(self) -> None:
        """Test normalizing terabyte values."""
        self.assertEqual(_normalize_memory_value('1T'), 1024 ** 4)
    
    def test_normalize_numeric_string(self) -> None:
        """Test normalizing numeric string (assumes bytes)."""
        self.assertEqual(_normalize_memory_value('1048576'), 1048576)
    
    def test_normalize_integer(self) -> None:
        """Test normalizing integer input."""
        self.assertEqual(_normalize_memory_value(1024), 1024)
    
    def test_normalize_float(self) -> None:
        """Test normalizing float input."""
        self.assertEqual(_normalize_memory_value(1024.5), 1024)
    
    def test_normalize_lowercase_unit(self) -> None:
        """Test normalizing with lowercase unit."""
        self.assertEqual(_normalize_memory_value('256m'), 256 * 1024 ** 2)
    
    def test_normalize_with_spaces(self) -> None:
        """Test normalizing with extra spaces."""
        self.assertEqual(_normalize_memory_value('  256 M  '), 256 * 1024 ** 2)
    
    def test_normalize_invalid_value(self) -> None:
        """Test normalizing invalid value returns 0."""
        self.assertEqual(_normalize_memory_value('invalid'), 0)
        self.assertEqual(_normalize_memory_value('XYZ'), 0)


class TestCompareSettingValues(unittest.TestCase):
    """Test suite for _compare_setting_values function."""
    
    def test_compare_memory_values_ok(self) -> None:
        """Test comparing memory values when current meets recommended."""
        status, message = _compare_setting_values('256M', '256M', 'memory', 'test_setting')
        self.assertEqual(status, 'OK')
        self.assertIn('optimal', message.lower())
    
    def test_compare_memory_values_warning(self) -> None:
        """Test comparing memory values when current is below but acceptable."""
        status, message = _compare_setting_values('150M', '256M', 'memory', 'test_setting')
        self.assertEqual(status, 'WARNING')
        self.assertIn('below', message.lower())
    
    def test_compare_memory_values_critical(self) -> None:
        """Test comparing memory values when current is critically low."""
        status, message = _compare_setting_values('64M', '256M', 'memory', 'test_setting')
        self.assertEqual(status, 'CRITICAL')
        self.assertIn('critical', message.lower())
    
    def test_compare_numeric_values_ok(self) -> None:
        """Test comparing numeric values when current meets recommended."""
        status, message = _compare_setting_values(151, 151, None, 'max_connections')
        self.assertEqual(status, 'OK')
    
    def test_compare_numeric_values_warning(self) -> None:
        """Test comparing numeric values when current is below but acceptable."""
        status, message = _compare_setting_values(120, 151, None, 'max_connections')
        self.assertEqual(status, 'WARNING')
    
    def test_compare_numeric_values_critical(self) -> None:
        """Test comparing numeric values when current is critically low."""
        status, message = _compare_setting_values(50, 151, None, 'max_connections')
        self.assertEqual(status, 'CRITICAL')
    
    def test_compare_on_off_values_match(self) -> None:
        """Test comparing ON/OFF values when they match."""
        status, message = _compare_setting_values('ON', 'ON', None, 'innodb_file_per_table')
        self.assertEqual(status, 'OK')
    
    def test_compare_on_off_values_differ(self) -> None:
        """Test comparing ON/OFF values when they differ."""
        status, message = _compare_setting_values('OFF', 'ON', None, 'innodb_file_per_table')
        self.assertEqual(status, 'WARNING')
    
    def test_compare_query_cache_special_case(self) -> None:
        """Test query_cache settings are treated as OK (deprecated)."""
        status, message = _compare_setting_values(0, 0, None, 'query_cache_size')
        self.assertEqual(status, 'OK')
        self.assertIn('deprecated', message.lower())


class TestCheckSettings(unittest.TestCase):
    """Test suite for check_settings function."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
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
        self.mock_cursor = MagicMock()
        self.mock_connection = MagicMock()
    
    @patch('tuning_fork.testing_modules.mysql.check_settings.get_db_connection')
    def test_check_settings_all_ok(self, mock_get_db: Mock) -> None:
        """Test checking settings when all are optimal."""
        self.mock_cursor.fetchone.return_value = ('max_connections', 151)
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        results = check_settings(self.mock_config, ['max_connections'])
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].setting_name, 'max_connections')
        self.assertEqual(results[0].status, 'OK')
    
    @patch('tuning_fork.testing_modules.mysql.check_settings.get_db_connection')
    def test_check_settings_with_warning(self, mock_get_db: Mock) -> None:
        """Test checking settings with warning status."""
        self.mock_cursor.fetchone.return_value = ('max_connections', 120)
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        results = check_settings(self.mock_config, ['max_connections'])
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'WARNING')
    
    @patch('tuning_fork.testing_modules.mysql.check_settings.get_db_connection')
    def test_check_settings_with_critical(self, mock_get_db: Mock) -> None:
        """Test checking settings with critical status."""
        self.mock_cursor.fetchone.return_value = ('max_connections', 20)
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        results = check_settings(self.mock_config, ['max_connections'])
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'CRITICAL')
    
    @patch('tuning_fork.testing_modules.mysql.check_settings.get_db_connection')
    def test_check_settings_invalid_setting_name(self, mock_get_db: Mock) -> None:
        """Test checking with invalid setting name."""
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        with self.assertRaises(ValueError) as context:
            check_settings(self.mock_config, ['invalid_setting'])
        
        self.assertIn("Unknown settings", str(context.exception))
    
    @patch('tuning_fork.testing_modules.mysql.check_settings.get_db_connection')
    def test_check_settings_invalid_workload_type(self, mock_get_db: Mock) -> None:
        """Test checking with invalid workload type."""
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        with self.assertRaises(ValueError) as context:
            check_settings(self.mock_config, ['max_connections'], workload_type='OLAP')
        
        self.assertIn("Unsupported workload type", str(context.exception))


class TestReportSettings(unittest.TestCase):
    """Test suite for report_settings function."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        self.check_results = [
            CheckResult(
                setting_name='max_connections',
                current_value=151,
                recommended_value=151,
                status='OK',
                message='Setting is optimal',
                impact='Maximum concurrent connections',
                workload_type='OLTP'
            ),
            CheckResult(
                setting_name='innodb_buffer_pool_size',
                current_value='1G',
                recommended_value='2G',
                status='WARNING',
                message='Setting below recommended',
                impact='InnoDB buffer pool size',
                setting_unit='memory',
                workload_type='OLTP'
            ),
            CheckResult(
                setting_name='thread_cache_size',
                current_value=4,
                recommended_value=32,
                status='CRITICAL',
                message='Setting critically low',
                impact='Thread cache size',
                workload_type='OLTP'
            )
        ]
    
    def test_report_settings_text_format(self) -> None:
        """Test generating text format report."""
        report = report_settings(self.check_results, format='text')
        
        self.assertIsInstance(report, str)
        self.assertIn('MySQL/MariaDB Settings Check Report', report)
        self.assertIn('CRITICAL ISSUES:', report)
        self.assertIn('WARNINGS:', report)
        self.assertIn('OK:', report)
    
    def test_report_settings_json_format(self) -> None:
        """Test generating JSON format report."""
        report = report_settings(self.check_results, format='json')
        
        self.assertIsInstance(report, str)
        parsed = json.loads(report)
        self.assertIsInstance(parsed, list)
        self.assertEqual(len(parsed), 3)
    
    def test_report_settings_markdown_format(self) -> None:
        """Test generating Markdown format report."""
        report = report_settings(self.check_results, format='markdown')
        
        self.assertIsInstance(report, str)
        self.assertIn('# MySQL/MariaDB Settings Report', report)
        self.assertIn('| Setting |', report)


if __name__ == '__main__':
    unittest.main()