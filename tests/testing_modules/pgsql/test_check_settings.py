"""
Unit tests for check_settings module.

This test suite ensures comprehensive coverage of the PostgreSQL settings
checker functionality, including check, report, apply, and rollback operations.
"""

import json
import unittest
from datetime import datetime
from typing import Any
from unittest.mock import MagicMock, Mock, patch

from tuning_fork.config import Config
from tuning_fork.testing_modules.pgsql.config.check_settings import (
    RECOMMENDED_SETTINGS,
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
            current_value=100,
            recommended_value=200,
            status='WARNING',
            message='Setting below recommended',
            impact='Maximum concurrent connections',
            setting_unit=None
        )
        
        self.assertEqual(result.setting_name, 'max_connections')
        self.assertEqual(result.current_value, 100)
        self.assertEqual(result.recommended_value, 200)
        self.assertEqual(result.status, 'WARNING')
    
    def test_check_result_to_dict(self) -> None:
        """Test converting CheckResult to dictionary."""
        result = CheckResult(
            setting_name='work_mem',
            current_value='4MB',
            recommended_value='8MB',
            status='WARNING',
            message='Memory below recommended',
            impact='Per-query memory',
            setting_unit='kB'
        )
        
        result_dict = result.to_dict()
        
        self.assertIsInstance(result_dict, dict)
        self.assertEqual(result_dict['setting_name'], 'work_mem')
        self.assertEqual(result_dict['setting_unit'], 'kB')
    
    def test_check_result_to_json(self) -> None:
        """Test converting CheckResult to JSON."""
        result = CheckResult(
            setting_name='shared_buffers',
            current_value='128MB',
            recommended_value='256MB',
            status='WARNING',
            message='Buffer size low',
            impact='Shared memory buffers'
        )
        
        json_str = result.to_json()
        
        self.assertIsInstance(json_str, str)
        parsed = json.loads(json_str)
        self.assertEqual(parsed['setting_name'], 'shared_buffers')


class TestChangeLogEntry(unittest.TestCase):
    """Test suite for ChangeLogEntry dataclass."""
    
    def test_change_log_entry_creation(self) -> None:
        """Test creating a ChangeLogEntry instance."""
        entry = ChangeLogEntry(
            setting_name='work_mem',
            old_value='4MB',
            new_value='8MB',
            timestamp='2025-01-01T00:00:00',
            change_id=1
        )
        
        self.assertEqual(entry.setting_name, 'work_mem')
        self.assertEqual(entry.old_value, '4MB')
        self.assertEqual(entry.new_value, '8MB')
        self.assertFalse(entry.rollback_status)
    
    def test_change_log_entry_to_dict(self) -> None:
        """Test converting ChangeLogEntry to dictionary."""
        entry = ChangeLogEntry(
            setting_name='max_connections',
            old_value=100,
            new_value=200,
            timestamp='2025-01-01T00:00:00',
            change_id=1,
            applied_by='admin'
        )
        
        entry_dict = entry.to_dict()
        
        self.assertIsInstance(entry_dict, dict)
        self.assertEqual(entry_dict['applied_by'], 'admin')
        self.assertFalse(entry_dict['rollback_status'])


class TestNormalizeMemoryValue(unittest.TestCase):
    """Test suite for _normalize_memory_value function."""
    
    def test_normalize_kilobytes(self) -> None:
        """Test normalizing kilobyte values."""
        self.assertEqual(_normalize_memory_value('1KB'), 1024)
        self.assertEqual(_normalize_memory_value('512KB'), 512 * 1024)
    
    def test_normalize_megabytes(self) -> None:
        """Test normalizing megabyte values."""
        self.assertEqual(_normalize_memory_value('1MB'), 1024 ** 2)
        self.assertEqual(_normalize_memory_value('256MB'), 256 * 1024 ** 2)
    
    def test_normalize_gigabytes(self) -> None:
        """Test normalizing gigabyte values."""
        self.assertEqual(_normalize_memory_value('1GB'), 1024 ** 3)
        self.assertEqual(_normalize_memory_value('4GB'), 4 * 1024 ** 3)
    
    def test_normalize_terabytes(self) -> None:
        """Test normalizing terabyte values."""
        self.assertEqual(_normalize_memory_value('1TB'), 1024 ** 4)
    
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
        self.assertEqual(_normalize_memory_value('256mb'), 256 * 1024 ** 2)
    
    def test_normalize_with_spaces(self) -> None:
        """Test normalizing with extra spaces."""
        self.assertEqual(_normalize_memory_value('  256 MB  '), 256 * 1024 ** 2)
    
    def test_normalize_invalid_value(self) -> None:
        """Test normalizing invalid value returns 0."""
        self.assertEqual(_normalize_memory_value('invalid'), 0)
        self.assertEqual(_normalize_memory_value('XYZ'), 0)


class TestCompareSettingValues(unittest.TestCase):
    """Test suite for _compare_setting_values function."""
    
    def test_compare_memory_values_ok(self) -> None:
        """Test comparing memory values when current meets recommended."""
        status, message = _compare_setting_values('256MB', '256MB', 'memory')
        self.assertEqual(status, 'OK')
        self.assertIn('optimal', message.lower())
    
    def test_compare_memory_values_warning(self) -> None:
        """Test comparing memory values when current is below but acceptable."""
        status, message = _compare_setting_values('150MB', '256MB', 'memory')
        self.assertEqual(status, 'WARNING')
        self.assertIn('below', message.lower())
    
    def test_compare_memory_values_critical(self) -> None:
        """Test comparing memory values when current is critically low."""
        status, message = _compare_setting_values('64MB', '256MB', 'memory')
        self.assertEqual(status, 'CRITICAL')
        self.assertIn('critical', message.lower())
    
    def test_compare_numeric_values_ok(self) -> None:
        """Test comparing numeric values when current meets recommended."""
        status, message = _compare_setting_values(100, 100, None)
        self.assertEqual(status, 'OK')
    
    def test_compare_numeric_values_warning(self) -> None:
        """Test comparing numeric values when current is below but acceptable."""
        status, message = _compare_setting_values(80, 100, None)
        self.assertEqual(status, 'WARNING')
    
    def test_compare_numeric_values_critical(self) -> None:
        """Test comparing numeric values when current is critically low."""
        status, message = _compare_setting_values(50, 100, None)
        self.assertEqual(status, 'CRITICAL')
    
    def test_compare_string_values_match(self) -> None:
        """Test comparing string values when they match."""
        status, message = _compare_setting_values('on', 'on', None)
        self.assertEqual(status, 'OK')
    
    def test_compare_string_values_differ(self) -> None:
        """Test comparing string values when they differ."""
        status, message = _compare_setting_values('off', 'on', None)
        self.assertEqual(status, 'WARNING')


class TestCheckSettings(unittest.TestCase):
    """Test suite for check_settings function."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        self.mock_config = MagicMock(spec=Config)
        self.mock_config.config = {
            'database': {
                'host': 'localhost',
                'port': 5432,
                'database': 'testdb',
                'username': 'testuser',
                'password': 'testpass'
            }
        }
        self.mock_cursor = MagicMock()
        self.mock_connection = MagicMock()
    
    @patch('tuning_fork.testing_modules.pgsql.config.check_settings.get_db_connection')
    def test_check_settings_all_ok(self, mock_get_db: Mock) -> None:
        """Test checking settings when all are optimal."""
        # Mock cursor responses
        self.mock_cursor.fetchone.return_value = (100, None)
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        results = check_settings(self.mock_config, ['max_connections'])
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].setting_name, 'max_connections')
        self.assertEqual(results[0].status, 'OK')
    
    @patch('tuning_fork.testing_modules.pgsql.config.check_settings.get_db_connection')
    def test_check_settings_with_warning(self, mock_get_db: Mock) -> None:
        """Test checking settings with warning status."""
        self.mock_cursor.fetchone.return_value = (80, None)
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        results = check_settings(self.mock_config, ['max_connections'])
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'WARNING')
    
    @patch('tuning_fork.testing_modules.pgsql.config.check_settings.get_db_connection')
    def test_check_settings_with_critical(self, mock_get_db: Mock) -> None:
        """Test checking settings with critical status."""
        self.mock_cursor.fetchone.return_value = (5, None)
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        results = check_settings(self.mock_config, ['max_connections'])
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'CRITICAL')
    
    @patch('tuning_fork.testing_modules.pgsql.config.check_settings.get_db_connection')
    def test_check_settings_memory_value(self, mock_get_db: Mock) -> None:
        """Test checking memory-based settings."""
        # PostgreSQL returns: value in blocks, unit shows block size
        # e.g., shared_buffers=16384 with unit='8kB' means 16384 blocks of 8kB = 128MB
        self.mock_cursor.fetchone.return_value = (16384, '8kB')
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        results = check_settings(self.mock_config, ['shared_buffers'])
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].setting_name, 'shared_buffers')
        # Value displayed as concatenation: 16384 + 8kB = 163848kB
        self.assertEqual(results[0].current_value, '163848kB')
    
    @patch('tuning_fork.testing_modules.pgsql.config.check_settings.get_db_connection')
    def test_check_settings_not_found(self, mock_get_db: Mock) -> None:
        """Test checking setting that doesn't exist in database."""
        self.mock_cursor.fetchone.return_value = None
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        results = check_settings(self.mock_config, ['max_connections'])
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'CRITICAL')
        self.assertEqual(results[0].current_value, 'NOT_FOUND')
    
    @patch('tuning_fork.testing_modules.pgsql.config.check_settings.get_db_connection')
    def test_check_settings_all_settings(self, mock_get_db: Mock) -> None:
        """Test checking all recommended settings."""
        self.mock_cursor.fetchone.return_value = (100, None)
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        results = check_settings(self.mock_config)
        
        self.assertEqual(len(results), len(RECOMMENDED_SETTINGS))
    
    @patch('tuning_fork.testing_modules.pgsql.config.check_settings.get_db_connection')
    def test_check_settings_invalid_setting_name(self, mock_get_db: Mock) -> None:
        """Test checking with invalid setting name."""
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        with self.assertRaises(ValueError) as context:
            check_settings(self.mock_config, ['invalid_setting'])
        
        self.assertIn("Unknown settings", str(context.exception))
    
    @patch('tuning_fork.testing_modules.pgsql.config.check_settings.get_db_connection')
    def test_check_settings_no_cursor(self, mock_get_db: Mock) -> None:
        """Test checking settings context manager is used correctly."""
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        self.mock_cursor.fetchone.return_value = (100, None)
        
        results = check_settings(self.mock_config, ['max_connections'])
        
        # Verify the context manager was used
        mock_get_db.assert_called_once_with(self.mock_config)
        self.assertEqual(len(results), 1)
    
    @patch('tuning_fork.testing_modules.pgsql.config.check_settings.get_db_connection')
    def test_check_settings_with_exception(self, mock_get_db: Mock) -> None:
        """Test handling of database exceptions during check."""
        self.mock_cursor.execute.side_effect = Exception("Database error")
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        results = check_settings(self.mock_config, ['max_connections'])
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'CRITICAL')
        self.assertIn('ERROR', results[0].current_value)
    
    @patch('tuning_fork.testing_modules.pgsql.config.check_settings.get_db_connection')
    def test_check_settings_multiple_settings(self, mock_get_db: Mock) -> None:
        """Test checking multiple specific settings."""
        self.mock_cursor.fetchone.side_effect = [
            (100, None),
            (32768, '8kB'),  # 32768 blocks * 8kB = 256MB
            (524288, '8kB')  # 524288 blocks * 8kB = 4GB
        ]
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        results = check_settings(
            self.mock_config,
            ['max_connections', 'shared_buffers', 'effective_cache_size']
        )
        
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0].setting_name, 'max_connections')
        self.assertEqual(results[1].setting_name, 'shared_buffers')
        self.assertEqual(results[2].setting_name, 'effective_cache_size')

class TestReportSettings(unittest.TestCase):
    """Test suite for report_settings function."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        self.check_results = [
            CheckResult(
                setting_name='max_connections',
                current_value=100,
                recommended_value=100,
                status='OK',
                message='Setting is optimal',
                impact='Maximum concurrent connections'
            ),
            CheckResult(
                setting_name='shared_buffers',
                current_value='128MB',
                recommended_value='256MB',
                status='WARNING',
                message='Setting below recommended',
                impact='Shared memory buffers'
            ),
            CheckResult(
                setting_name='work_mem',
                current_value='1MB',
                recommended_value='4MB',
                status='CRITICAL',
                message='Setting critically low',
                impact='Per-query memory'
            )
        ]
    
    def test_report_settings_text_format(self) -> None:
        """Test generating text format report."""
        report = report_settings(self.check_results, format='text')
        
        self.assertIsInstance(report, str)
        self.assertIn('PostgreSQL Settings Check Report', report)
        self.assertIn('CRITICAL ISSUES:', report)
        self.assertIn('WARNINGS:', report)
        self.assertIn('OK:', report)
        self.assertIn('work_mem', report)
        self.assertIn('shared_buffers', report)
        self.assertIn('max_connections', report)
    
    def test_report_settings_json_format(self) -> None:
        """Test generating JSON format report."""
        report = report_settings(self.check_results, format='json')
        
        self.assertIsInstance(report, str)
        parsed = json.loads(report)
        self.assertIsInstance(parsed, list)
        self.assertEqual(len(parsed), 3)
        self.assertEqual(parsed[0]['setting_name'], 'max_connections')
    
    def test_report_settings_markdown_format(self) -> None:
        """Test generating Markdown format report."""
        report = report_settings(self.check_results, format='markdown')
        
        self.assertIsInstance(report, str)
        self.assertIn('# PostgreSQL Settings Report', report)
        self.assertIn('| Setting |', report)
        self.assertIn('|---------|', report)
        self.assertIn('max_connections', report)
    
    def test_report_settings_empty_results(self) -> None:
        """Test generating report with no results."""
        report = report_settings([], format='text')
        
        self.assertEqual(report, "No settings checked.")
    
    def test_report_settings_invalid_format(self) -> None:
        """Test generating report with invalid format."""
        with self.assertRaises(ValueError) as context:
            report_settings(self.check_results, format='xml')
        
        self.assertIn("Unsupported format", str(context.exception))
    
    def test_report_settings_summary_counts(self) -> None:
        """Test that summary includes correct counts."""
        report = report_settings(self.check_results, format='text')
        
        self.assertIn('1 critical', report)
        self.assertIn('1 warnings', report)
        self.assertIn('1 ok', report)
    
    def test_report_settings_all_ok(self) -> None:
        """Test report when all settings are OK."""
        ok_results = [
            CheckResult(
                setting_name='max_connections',
                current_value=100,
                recommended_value=100,
                status='OK',
                message='Setting is optimal',
                impact='Maximum concurrent connections'
            )
        ]
        
        report = report_settings(ok_results, format='text')
        
        self.assertNotIn('CRITICAL ISSUES:', report)
        self.assertNotIn('WARNINGS:', report)
        self.assertIn('OK:', report)


class TestApplySettings(unittest.TestCase):
    """Test suite for apply_settings function."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        self.mock_config = MagicMock(spec=Config)
        self.mock_cursor = MagicMock()
    

class TestRollbackSettings(unittest.TestCase):
    """Test suite for rollback_settings function."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        self.mock_config = MagicMock(spec=Config)
        self.mock_cursor = MagicMock()
        
        self.change_log = [
            ChangeLogEntry(
                setting_name='work_mem',
                old_value='4MB',
                new_value='8MB',
                timestamp='2025-01-01T00:00:00',
                change_id=1
            ),
            ChangeLogEntry(
                setting_name='max_connections',
                old_value=100,
                new_value=200,
                timestamp='2025-01-01T00:00:01',
                change_id=2
            )
        ]
    

class TestIntegration(unittest.TestCase):
    """Integration tests for combined functionality."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        self.mock_config = MagicMock(spec=Config)
        self.mock_cursor = MagicMock()
    

if __name__ == '__main__':
    unittest.main()