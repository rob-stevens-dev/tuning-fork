"""
Unit tests for PostgreSQL check_settings module.

This test suite ensures comprehensive coverage of the PostgreSQL settings
checker functionality, including check, report, apply, and rollback operations
with support for multiple workload types (OLTP, OLAP, Mixed).
"""

import json
import unittest
from datetime import datetime
from typing import Any
from unittest.mock import MagicMock, Mock, patch

from tuning_fork.config import Config
from tuning_fork.testing_modules.pgsql.check_settings import (
    RECOMMENDED_SETTINGS_OLTP,
    RECOMMENDED_SETTINGS_OLAP,
    RECOMMENDED_SETTINGS_MIXED,
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
            setting_unit=None,
            workload_type='OLTP'
        )
        
        self.assertEqual(result.setting_name, 'max_connections')
        self.assertEqual(result.current_value, 100)
        self.assertEqual(result.recommended_value, 200)
        self.assertEqual(result.status, 'WARNING')
        self.assertEqual(result.workload_type, 'OLTP')
    
    def test_check_result_to_dict(self) -> None:
        """Test converting CheckResult to dictionary."""
        result = CheckResult(
            setting_name='work_mem',
            current_value='4MB',
            recommended_value='8MB',
            status='WARNING',
            message='Memory below recommended',
            impact='Per-query memory',
            setting_unit='kB',
            workload_type='OLAP'
        )
        
        result_dict = result.to_dict()
        
        self.assertIsInstance(result_dict, dict)
        self.assertEqual(result_dict['setting_name'], 'work_mem')
        self.assertEqual(result_dict['setting_unit'], 'kB')
        self.assertEqual(result_dict['workload_type'], 'OLAP')
    
    def test_check_result_to_json(self) -> None:
        """Test converting CheckResult to JSON."""
        result = CheckResult(
            setting_name='shared_buffers',
            current_value='128MB',
            recommended_value='256MB',
            status='WARNING',
            message='Buffer size low',
            impact='Shared memory buffers',
            workload_type='Mixed'
        )
        
        json_str = result.to_json()
        
        self.assertIsInstance(json_str, str)
        parsed = json.loads(json_str)
        self.assertEqual(parsed['setting_name'], 'shared_buffers')
        self.assertEqual(parsed['workload_type'], 'Mixed')
        
class TestChangeLogEntry(unittest.TestCase):
    """Test suite for ChangeLogEntry dataclass."""
    
    def test_change_log_entry_creation(self) -> None:
        """Test creating a ChangeLogEntry instance."""
        entry = ChangeLogEntry(
            setting_name='work_mem',
            old_value='4MB',
            new_value='8MB',
            timestamp='2025-10-17T12:00:00',
            change_id=1,
            scope='SYSTEM'
        )
        
        self.assertEqual(entry.setting_name, 'work_mem')
        self.assertEqual(entry.old_value, '4MB')
        self.assertEqual(entry.new_value, '8MB')
        self.assertEqual(entry.scope, 'SYSTEM')
        self.assertFalse(entry.rollback_status)
    
    def test_change_log_entry_to_dict(self) -> None:
        """Test converting ChangeLogEntry to dictionary."""
        entry = ChangeLogEntry(
            setting_name='max_connections',
            old_value=100,
            new_value=200,
            timestamp='2025-10-17T12:00:00',
            change_id=2,
            scope='SESSION'
        )
        
        entry_dict = entry.to_dict()
        
        self.assertIsInstance(entry_dict, dict)
        self.assertEqual(entry_dict['setting_name'], 'max_connections')
        self.assertEqual(entry_dict['scope'], 'SESSION')
    
    def test_change_log_entry_default_values(self) -> None:
        """Test ChangeLogEntry with default values."""
        entry = ChangeLogEntry(
            setting_name='shared_buffers',
            old_value='128MB',
            new_value='256MB',
            timestamp='2025-10-17T12:00:00',
            change_id=1
        )
        
        self.assertEqual(entry.applied_by, 'tuning_fork')
        self.assertFalse(entry.rollback_status)
        self.assertEqual(entry.scope, 'SYSTEM')
        
        
class TestNormalizeMemoryValue(unittest.TestCase):
    """Test suite for _normalize_memory_value function."""
    
    def test_normalize_memory_kb(self) -> None:
        """Test normalizing kilobyte values."""
        self.assertEqual(_normalize_memory_value('1024kB'), 1024 * 1024)
        self.assertEqual(_normalize_memory_value('512KB'), 512 * 1024)
    
    def test_normalize_memory_mb(self) -> None:
        """Test normalizing megabyte values."""
        self.assertEqual(_normalize_memory_value('256MB'), 256 * 1024 * 1024)
        self.assertEqual(_normalize_memory_value('1mb'), 1 * 1024 * 1024)
    
    def test_normalize_memory_gb(self) -> None:
        """Test normalizing gigabyte values."""
        self.assertEqual(_normalize_memory_value('4GB'), 4 * 1024 * 1024 * 1024)
        self.assertEqual(_normalize_memory_value('2gb'), 2 * 1024 * 1024 * 1024)
    
    def test_normalize_memory_tb(self) -> None:
        """Test normalizing terabyte values."""
        self.assertEqual(_normalize_memory_value('1TB'), 1 * 1024 * 1024 * 1024 * 1024)
    
    def test_normalize_memory_numeric_only(self) -> None:
        """Test normalizing numeric-only values (assumed bytes)."""
        self.assertEqual(_normalize_memory_value('1048576'), 1048576)
        self.assertEqual(_normalize_memory_value(1048576), 1048576)
    
    def test_normalize_memory_float_values(self) -> None:
        """Test normalizing float memory values."""
        self.assertEqual(_normalize_memory_value('1.5GB'), int(1.5 * 1024 * 1024 * 1024))
        self.assertEqual(_normalize_memory_value('0.5MB'), int(0.5 * 1024 * 1024))
    
    def test_normalize_memory_invalid_format(self) -> None:
        """Test handling invalid memory format."""
        with self.assertRaises(ValueError):
            _normalize_memory_value('invalid')
        
        with self.assertRaises(ValueError):
            _normalize_memory_value('256XB')
            
class TestCompareSettingValues(unittest.TestCase):
    """Test suite for _compare_setting_values function."""
    
    def test_compare_memory_values_ok(self) -> None:
        """Test comparing memory values that are OK."""
        status, message = _compare_setting_values('256MB', '256MB', 'memory')
        self.assertEqual(status, 'OK')
        self.assertIn('optimal', message.lower())
    
    def test_compare_memory_values_warning(self) -> None:
        """Test comparing memory values that trigger warning."""
        status, message = _compare_setting_values('150MB', '256MB', 'memory')
        self.assertEqual(status, 'WARNING')
        self.assertIn('below recommended', message.lower())
    
    def test_compare_memory_values_critical(self) -> None:
        """Test comparing memory values that are critical."""
        status, message = _compare_setting_values('64MB', '256MB', 'memory')
        self.assertEqual(status, 'CRITICAL')
        self.assertIn('critically low', message.lower())
    
    def test_compare_numeric_values_ok(self) -> None:
        """Test comparing numeric values that are OK."""
        status, message = _compare_setting_values(200, 100, None)
        self.assertEqual(status, 'OK')
    
    def test_compare_numeric_values_warning(self) -> None:
        """Test comparing numeric values that trigger warning."""
        status, message = _compare_setting_values(80, 100, None)
        self.assertEqual(status, 'WARNING')
    
    def test_compare_numeric_values_critical(self) -> None:
        """Test comparing numeric values that are critical."""
        status, message = _compare_setting_values(50, 100, None)
        self.assertEqual(status, 'CRITICAL')
    
    def test_compare_string_values_match(self) -> None:
        """Test comparing string values that match."""
        status, message = _compare_setting_values('on', 'on', None)
        self.assertEqual(status, 'OK')
    
    def test_compare_string_values_differ(self) -> None:
        """Test comparing string values that differ."""
        status, message = _compare_setting_values('off', 'on', None)
        self.assertEqual(status, 'WARNING')
        
        
class TestCheckSettings(unittest.TestCase):
    """Test suite for check_settings function."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        self.mock_config = MagicMock(spec=Config)
        self.mock_connection = MagicMock()
        self.mock_cursor = MagicMock()
    
    @patch('tuning_fork.testing_modules.pgsql.check_settings.get_db_connection')
    def test_check_settings_oltp_workload(self, mock_get_db: Mock) -> None:
        """Test checking settings with OLTP workload."""
        self.mock_cursor.fetchone.return_value = (100, None)
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        results = check_settings(self.mock_config, ['max_connections'], workload_type='OLTP')
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].workload_type, 'OLTP')
        self.assertEqual(results[0].recommended_value, 100)
    
    @patch('tuning_fork.testing_modules.pgsql.check_settings.get_db_connection')
    def test_check_settings_olap_workload(self, mock_get_db: Mock) -> None:
        """Test checking settings with OLAP workload."""
        self.mock_cursor.fetchone.return_value = (50, None)
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        results = check_settings(self.mock_config, ['max_connections'], workload_type='OLAP')
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].workload_type, 'OLAP')
        self.assertEqual(results[0].recommended_value, 50)
    
    @patch('tuning_fork.testing_modules.pgsql.check_settings.get_db_connection')
    def test_check_settings_mixed_workload(self, mock_get_db: Mock) -> None:
        """Test checking settings with Mixed workload."""
        self.mock_cursor.fetchone.return_value = (75, None)
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        results = check_settings(self.mock_config, ['max_connections'], workload_type='Mixed')
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].workload_type, 'Mixed')
        self.assertEqual(results[0].recommended_value, 75)
    
    @patch('tuning_fork.testing_modules.pgsql.check_settings.get_db_connection')
    def test_check_settings_invalid_workload(self, mock_get_db: Mock) -> None:
        """Test checking with invalid workload type."""
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        with self.assertRaises(ValueError) as context:
            check_settings(self.mock_config, ['max_connections'], workload_type='INVALID')
        
        self.assertIn("Unsupported workload type", str(context.exception))
    
    @patch('tuning_fork.testing_modules.pgsql.check_settings.get_db_connection')
    def test_check_settings_with_ok_status(self, mock_get_db: Mock) -> None:
        """Test checking settings with OK status."""
        self.mock_cursor.fetchone.return_value = (100, None)
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        results = check_settings(self.mock_config, ['max_connections'])
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'OK')
        
    @patch('tuning_fork.testing_modules.pgsql.check_settings.get_db_connection')
    def test_check_settings_with_warning(self, mock_get_db: Mock) -> None:
        """Test checking settings with warning status."""
        self.mock_cursor.fetchone.return_value = (80, None)
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        results = check_settings(self.mock_config, ['max_connections'])
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'WARNING')
    
    @patch('tuning_fork.testing_modules.pgsql.check_settings.get_db_connection')
    def test_check_settings_with_critical(self, mock_get_db: Mock) -> None:
        """Test checking settings with critical status."""
        self.mock_cursor.fetchone.return_value = (5, None)
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        results = check_settings(self.mock_config, ['max_connections'])
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'CRITICAL')
    
    @patch('tuning_fork.testing_modules.pgsql.check_settings.get_db_connection')
    def test_check_settings_memory_value(self, mock_get_db: Mock) -> None:
        """Test checking memory-based settings."""
        self.mock_cursor.fetchone.return_value = (32768, '8kB')
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        results = check_settings(self.mock_config, ['shared_buffers'])
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].setting_name, 'shared_buffers')
        self.assertEqual(results[0].current_value, '327688kB')
    
    @patch('tuning_fork.testing_modules.pgsql.check_settings.get_db_connection')
    def test_check_settings_not_found(self, mock_get_db: Mock) -> None:
        """Test checking setting that doesn't exist in database."""
        self.mock_cursor.fetchone.return_value = None
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        results = check_settings(self.mock_config, ['max_connections'])
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'CRITICAL')
        self.assertEqual(results[0].current_value, 'NOT_FOUND')
    
    @patch('tuning_fork.testing_modules.pgsql.check_settings.get_db_connection')
    def test_check_settings_all_settings(self, mock_get_db: Mock) -> None:
        """Test checking all recommended settings."""
        self.mock_cursor.fetchone.return_value = (100, None)
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        results = check_settings(self.mock_config)
        
        self.assertEqual(len(results), len(RECOMMENDED_SETTINGS_OLTP))
    
    @patch('tuning_fork.testing_modules.pgsql.check_settings.get_db_connection')
    def test_check_settings_invalid_setting_name(self, mock_get_db: Mock) -> None:
        """Test checking with invalid setting name."""
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        with self.assertRaises(ValueError) as context:
            check_settings(self.mock_config, ['invalid_setting'])
        
        self.assertIn("Unknown settings", str(context.exception))
    
    @patch('tuning_fork.testing_modules.pgsql.check_settings.get_db_connection')
    def test_check_settings_with_exception(self, mock_get_db: Mock) -> None:
        """Test handling of database exceptions during check."""
        self.mock_cursor.execute.side_effect = Exception("Database error")
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        results = check_settings(self.mock_config, ['max_connections'])
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'CRITICAL')
        self.assertEqual(results[0].current_value, 'ERROR')
        
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
                impact='Maximum concurrent connections',
                workload_type='OLTP'
            ),
            CheckResult(
                setting_name='shared_buffers',
                current_value='128MB',
                recommended_value='256MB',
                status='WARNING',
                message='Setting below recommended',
                impact='Shared memory buffers',
                setting_unit='8kB',
                workload_type='OLTP'
            ),
            CheckResult(
                setting_name='work_mem',
                current_value='1MB',
                recommended_value='4MB',
                status='CRITICAL',
                message='Setting critically low',
                impact='Per-query memory',
                workload_type='OLTP'
            )
        ]
    
    def test_report_settings_text_format(self) -> None:
        """Test generating text format report."""
        report = report_settings(self.check_results, format='text')
        
        self.assertIsInstance(report, str)
        self.assertIn('PostgreSQL Settings Check Report', report)
        self.assertIn('Workload Type: OLTP', report)
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
        self.assertEqual(parsed[0]['setting_name'], 'max_connections')
        self.assertEqual(parsed[0]['workload_type'], 'OLTP')
    
    def test_report_settings_markdown_format(self) -> None:
        """Test generating Markdown format report."""
        report = report_settings(self.check_results, format='markdown')
        
        self.assertIsInstance(report, str)
        self.assertIn('# PostgreSQL Settings Report', report)
        self.assertIn('**Workload Type:** OLTP', report)
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


class TestApplySettings(unittest.TestCase):
    """Test suite for apply_settings function."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        self.mock_config = MagicMock(spec=Config)
        self.mock_cursor = MagicMock()
        self.mock_connection = MagicMock()
    
    @patch('tuning_fork.testing_modules.pgsql.check_settings.get_db_connection')
    def test_apply_settings_with_persist(self, mock_get_db: Mock) -> None:
        """Test applying settings with ALTER SYSTEM SET (persist=True)."""
        self.mock_cursor.fetchone.return_value = ('4MB', 'user')
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        changes = apply_settings(
            self.mock_config,
            {'work_mem': '8MB'},
            persist=True
        )
        
        self.assertEqual(len(changes), 1)
        self.assertEqual(changes[0].setting_name, 'work_mem')
        self.assertEqual(changes[0].old_value, '4MB')
        self.assertEqual(changes[0].new_value, '8MB')
        self.assertEqual(changes[0].scope, 'SYSTEM')
    
    @patch('tuning_fork.testing_modules.pgsql.check_settings.get_db_connection')
    def test_apply_settings_without_persist(self, mock_get_db: Mock) -> None:
        """Test applying settings with SET (persist=False)."""
        self.mock_cursor.fetchone.return_value = ('4MB', 'user')
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        changes = apply_settings(
            self.mock_config,
            {'work_mem': '8MB'},
            persist=False
        )
        
        self.assertEqual(len(changes), 1)
        self.assertEqual(changes[0].scope, 'SESSION')
    
    @patch('tuning_fork.testing_modules.pgsql.check_settings.get_db_connection')
    def test_apply_settings_invalid_setting(self, mock_get_db: Mock) -> None:
        """Test applying invalid setting name."""
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        with self.assertRaises(ValueError) as context:
            apply_settings(self.mock_config, {'invalid_setting': '100'})
        
        self.assertIn("Unknown settings", str(context.exception))
    
    @patch('tuning_fork.testing_modules.pgsql.check_settings.get_db_connection')
    def test_apply_settings_invalid_workload(self, mock_get_db: Mock) -> None:
        """Test applying settings with invalid workload type."""
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        with self.assertRaises(ValueError) as context:
            apply_settings(
                self.mock_config,
                {'work_mem': '8MB'},
                workload_type='INVALID'
            )
        
        self.assertIn("Unsupported workload type", str(context.exception))
        
class TestRollbackSettings(unittest.TestCase):
    """Test suite for rollback_settings function."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        self.mock_config = MagicMock(spec=Config)
        self.mock_cursor = MagicMock()
        self.mock_connection = MagicMock()
        
        self.change_log = [
            ChangeLogEntry(
                setting_name='work_mem',
                old_value='4MB',
                new_value='8MB',
                timestamp='2025-10-17T12:00:00',
                change_id=1,
                scope='SYSTEM'
            ),
            ChangeLogEntry(
                setting_name='maintenance_work_mem',
                old_value='64MB',
                new_value='128MB',
                timestamp='2025-10-17T12:00:00',
                change_id=2,
                scope='SESSION'
            )
        ]
    
    @patch('tuning_fork.testing_modules.pgsql.check_settings.get_db_connection')
    def test_rollback_settings_with_persist(self, mock_get_db: Mock) -> None:
        """Test rolling back settings with persist=True."""
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        rolled_back = rollback_settings(self.mock_config, self.change_log, persist=True)
        
        self.assertEqual(len(rolled_back), 2)
        self.assertIn('work_mem', rolled_back)
        self.assertIn('maintenance_work_mem', rolled_back)
        
        for entry in self.change_log:
            self.assertTrue(entry.rollback_status)
    
    @patch('tuning_fork.testing_modules.pgsql.check_settings.get_db_connection')
    def test_rollback_settings_empty_log(self, mock_get_db: Mock) -> None:
        """Test rolling back with empty change log."""
        mock_get_db.return_value.__enter__.return_value = (self.mock_connection, self.mock_cursor)
        
        with self.assertRaises(ValueError) as context:
            rollback_settings(self.mock_config, [])
        
        self.assertIn("No change log entries", str(context.exception))


class TestWorkloadTypeSettings(unittest.TestCase):
    """Test suite for workload-specific settings."""
    
    def test_oltp_settings_exist(self) -> None:
        """Test that OLTP settings are defined."""
        self.assertIsNotNone(RECOMMENDED_SETTINGS_OLTP)
        self.assertIn('max_connections', RECOMMENDED_SETTINGS_OLTP)
        self.assertIn('shared_buffers', RECOMMENDED_SETTINGS_OLTP)
        self.assertIn('work_mem', RECOMMENDED_SETTINGS_OLTP)
    
    def test_olap_settings_exist(self) -> None:
        """Test that OLAP settings are defined."""
        self.assertIsNotNone(RECOMMENDED_SETTINGS_OLAP)
        self.assertIn('max_connections', RECOMMENDED_SETTINGS_OLAP)
        self.assertIn('shared_buffers', RECOMMENDED_SETTINGS_OLAP)
        self.assertIn('work_mem', RECOMMENDED_SETTINGS_OLAP)
    
    def test_mixed_settings_exist(self) -> None:
        """Test that Mixed settings are defined."""
        self.assertIsNotNone(RECOMMENDED_SETTINGS_MIXED)
        self.assertIn('max_connections', RECOMMENDED_SETTINGS_MIXED)
        self.assertIn('shared_buffers', RECOMMENDED_SETTINGS_MIXED)
        self.assertIn('work_mem', RECOMMENDED_SETTINGS_MIXED)
    
    def test_oltp_vs_olap_differences(self) -> None:
        """Test that OLTP and OLAP have different recommendations."""
        oltp_max_conn = RECOMMENDED_SETTINGS_OLTP['max_connections']['value']
        olap_max_conn = RECOMMENDED_SETTINGS_OLAP['max_connections']['value']
        self.assertLess(olap_max_conn, oltp_max_conn)
        
        oltp_work_mem = _normalize_memory_value(RECOMMENDED_SETTINGS_OLTP['work_mem']['value'])
        olap_work_mem = _normalize_memory_value(RECOMMENDED_SETTINGS_OLAP['work_mem']['value'])
        self.assertGreater(olap_work_mem, oltp_work_mem)
    
    def test_all_settings_have_required_fields(self) -> None:
        """Test that all settings have required configuration fields."""
        required_fields = ['value', 'unit', 'impact', 'critical_threshold', 'context', 'dynamic']
        
        for workload_settings in [RECOMMENDED_SETTINGS_OLTP, RECOMMENDED_SETTINGS_OLAP, RECOMMENDED_SETTINGS_MIXED]:
            for setting_name, config in workload_settings.items():
                for field in required_fields:
                    self.assertIn(
                        field,
                        config,
                        f"Setting '{setting_name}' missing required field '{field}'"
                    )
    
    def test_dynamic_flag_consistency(self) -> None:
        """Test that dynamic flag matches context."""
        for workload_settings in [RECOMMENDED_SETTINGS_OLTP, RECOMMENDED_SETTINGS_OLAP, RECOMMENDED_SETTINGS_MIXED]:
            for setting_name, config in workload_settings.items():
                context = config['context']
                dynamic = config['dynamic']
                
                if context == 'postmaster':
                    self.assertFalse(
                        dynamic,
                        f"Setting '{setting_name}' has context='postmaster' but dynamic=True"
                    )


if __name__ == '__main__':
    unittest.main()