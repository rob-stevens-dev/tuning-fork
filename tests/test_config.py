"""
Unit tests for config module.

This test suite ensures comprehensive coverage of configuration loading
and access functionality.
"""

import tempfile
import unittest
from pathlib import Path
from typing import Any

from tuning_fork.config import Config, ConfigError


class TestConfig(unittest.TestCase):
    """Test suite for Config class."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        self.test_config_data = {
            'database': {
                'type': 'postgresql',
                'host': '192.168.0.131',
                'port': 55432,
                'username': 'postgres',
                'password': 'postgres',
                'database': 'testdb'
            },
            'logging': {
                'filename': 'logging/{session_id}.db',
                'level': 'DEBUG',
                'auto_create': True,
                'auto_backup': True
            },
            'simple_value': 'test',
            'numeric_value': 42
        }
    
    def _create_temp_config(self, content: str) -> str:
        """
        Create a temporary configuration file.
        
        Args:
            content: YAML content to write to file.
        
        Returns:
            Path to temporary config file.
        """
        temp_file = tempfile.NamedTemporaryFile(
            mode='w',
            suffix='.yaml',
            delete=False
        )
        temp_file.write(content)
        temp_file.close()
        return temp_file.name
    
    def test_load_valid_config(self) -> None:
        """Test loading a valid configuration file."""
        config_content = """
database:
  type: postgresql
  host: 192.168.0.131
  port: 55432
  username: postgres
  password: postgres
  database: testdb

logging:
  filename: logging/{session_id}.db
  level: DEBUG
  auto_create: true
  auto_backup: true
"""
        config_file = self._create_temp_config(config_content)
        
        try:
            config = Config(config_file)
            
            self.assertIsNotNone(config.config)
            self.assertIn('database', config.config)
            self.assertIn('logging', config.config)
        finally:
            Path(config_file).unlink()
    
    def test_load_nonexistent_file(self) -> None:
        """Test loading a non-existent configuration file."""
        with self.assertRaises(ConfigError) as context:
            Config('nonexistent_file.yaml')
        
        self.assertIn("Configuration file not found", str(context.exception))
    
    def test_load_empty_config(self) -> None:
        """Test loading an empty configuration file."""
        config_file = self._create_temp_config("")
        
        try:
            with self.assertRaises(ConfigError) as context:
                Config(config_file)
            
            self.assertIn("Configuration file is empty", str(context.exception))
        finally:
            Path(config_file).unlink()
    
    def test_load_invalid_yaml(self) -> None:
        """Test loading invalid YAML content."""
        config_content = """
database:
  host: localhost
  port: [invalid yaml structure
"""
        config_file = self._create_temp_config(config_content)
        
        try:
            with self.assertRaises(ConfigError) as context:
                Config(config_file)
            
            self.assertIn("Failed to parse YAML", str(context.exception))
        finally:
            Path(config_file).unlink()
    
    def test_load_non_dict_config(self) -> None:
        """Test loading configuration that isn't a dictionary."""
        config_content = "- item1\n- item2\n- item3"
        config_file = self._create_temp_config(config_content)
        
        try:
            with self.assertRaises(ConfigError) as context:
                Config(config_file)
            
            self.assertIn("Configuration must be a dictionary", str(context.exception))
        finally:
            Path(config_file).unlink()
    
    def test_get_simple_value(self) -> None:
        """Test getting a simple top-level value."""
        config_content = "simple_value: test\nnumeric_value: 42"
        config_file = self._create_temp_config(config_content)
        
        try:
            config = Config(config_file)
            
            self.assertEqual(config.get('simple_value'), 'test')
            self.assertEqual(config.get('numeric_value'), 42)
        finally:
            Path(config_file).unlink()
    
    def test_get_nested_value(self) -> None:
        """Test getting nested configuration values using dot notation."""
        config_content = """
database:
  host: localhost
  port: 5432
  credentials:
    username: admin
    password: secret
"""
        config_file = self._create_temp_config(config_content)
        
        try:
            config = Config(config_file)
            
            self.assertEqual(config.get('database.host'), 'localhost')
            self.assertEqual(config.get('database.port'), 5432)
            self.assertEqual(config.get('database.credentials.username'), 'admin')
            self.assertEqual(config.get('database.credentials.password'), 'secret')
        finally:
            Path(config_file).unlink()
    
    def test_get_with_default(self) -> None:
        """Test getting configuration value with default."""
        config_content = "existing_key: value"
        config_file = self._create_temp_config(config_content)
        
        try:
            config = Config(config_file)
            
            self.assertEqual(config.get('existing_key', 'default'), 'value')
            self.assertEqual(config.get('missing_key', 'default'), 'default')
            self.assertEqual(config.get('missing.nested.key', 123), 123)
        finally:
            Path(config_file).unlink()
    
    def test_get_nonexistent_key(self) -> None:
        """Test getting a non-existent key returns None."""
        config_content = "key: value"
        config_file = self._create_temp_config(config_content)
        
        try:
            config = Config(config_file)
            
            self.assertIsNone(config.get('nonexistent_key'))
            self.assertIsNone(config.get('deeply.nested.nonexistent.key'))
        finally:
            Path(config_file).unlink()
    
    def test_require_existing_key(self) -> None:
        """Test requiring an existing configuration key."""
        config_content = """
database:
  host: localhost
  port: 5432
"""
        config_file = self._create_temp_config(config_content)
        
        try:
            config = Config(config_file)
            
            self.assertEqual(config.require('database.host'), 'localhost')
            self.assertEqual(config.require('database.port'), 5432)
        finally:
            Path(config_file).unlink()
    
    def test_require_missing_key(self) -> None:
        """Test requiring a missing configuration key raises error."""
        config_content = "key: value"
        config_file = self._create_temp_config(config_content)
        
        try:
            config = Config(config_file)
            
            with self.assertRaises(ConfigError) as context:
                config.require('missing_key')
            
            self.assertIn("Required configuration key not found", str(context.exception))
        finally:
            Path(config_file).unlink()
    
    def test_has_existing_key(self) -> None:
        """Test checking existence of configuration keys."""
        config_content = """
database:
  host: localhost
  port: 5432
"""
        config_file = self._create_temp_config(config_content)
        
        try:
            config = Config(config_file)
            
            self.assertTrue(config.has('database'))
            self.assertTrue(config.has('database.host'))
            self.assertTrue(config.has('database.port'))
            self.assertFalse(config.has('missing_key'))
            self.assertFalse(config.has('database.missing'))
        finally:
            Path(config_file).unlink()
    
    def test_get_dict_value(self) -> None:
        """Test getting an entire dictionary section."""
        config_content = """
database:
  host: localhost
  port: 5432
"""
        config_file = self._create_temp_config(config_content)
        
        try:
            config = Config(config_file)
            
            db_config = config.get('database')
            self.assertIsInstance(db_config, dict)
            self.assertEqual(db_config['host'], 'localhost')
            self.assertEqual(db_config['port'], 5432)
        finally:
            Path(config_file).unlink()
    
    def test_get_with_boolean_values(self) -> None:
        """Test getting boolean configuration values."""
        config_content = """
features:
  enabled: true
  debug: false
"""
        config_file = self._create_temp_config(config_content)
        
        try:
            config = Config(config_file)
            
            self.assertTrue(config.get('features.enabled'))
            self.assertFalse(config.get('features.debug'))
        finally:
            Path(config_file).unlink()
    
    def test_get_with_list_values(self) -> None:
        """Test getting list configuration values."""
        config_content = """
servers:
  - host1
  - host2
  - host3
"""
        config_file = self._create_temp_config(config_content)
        
        try:
            config = Config(config_file)
            
            servers = config.get('servers')
            self.assertIsInstance(servers, list)
            self.assertEqual(len(servers), 3)
            self.assertIn('host1', servers)
        finally:
            Path(config_file).unlink()


if __name__ == '__main__':
    unittest.main()