"""
Configuration management module.

This module handles loading and accessing application configuration from YAML files.
"""

import yaml
from os import path
from typing import Any, Optional


class ConfigError(Exception):
    """Raised when configuration loading or access fails."""
    pass


class Config:
    """
    Configuration manager for loading and accessing application settings.
    
    Loads configuration from YAML files and provides access to configuration
    values with validation and error handling.
    
    Attributes:
        config: Dictionary containing loaded configuration data.
    
    Example:
        >>> config = Config('config.yaml')
        >>> db_host = config.get('database.host')
        >>> db_port = config.get('database.port', default=5432)
    """
    
    def __init__(self, config_file: str) -> None:
        """
        Initialize configuration manager.
        
        Args:
            config_file: Path to YAML configuration file.
        
        Raises:
            ConfigError: If configuration file doesn't exist or can't be loaded.
        """
        self.config: dict[str, Any] = {}
        
        if not path.exists(config_file):
            raise ConfigError(f"Configuration file not found: {config_file}")
        
        self.load_config(config_file)
    
    def load_config(self, config_file_path: str) -> None:
        """
        Load configuration from a YAML file.
        
        Args:
            config_file_path: Path to the YAML configuration file.
        
        Raises:
            ConfigError: If file can't be read or parsed.
        """
        try:
            with open(config_file_path, 'r', encoding='utf-8') as file:
                loaded_config = yaml.safe_load(file)
                
                if loaded_config is None:
                    raise ConfigError(f"Configuration file is empty: {config_file_path}")
                
                if not isinstance(loaded_config, dict):
                    raise ConfigError(
                        f"Configuration must be a dictionary, got {type(loaded_config)}"
                    )
                
                self.config = loaded_config
                
        except yaml.YAMLError as exc:
            raise ConfigError(f"Failed to parse YAML configuration: {exc}") from exc
        except IOError as exc:
            raise ConfigError(f"Failed to read configuration file: {exc}") from exc
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value using dot notation.
        
        Args:
            key: Configuration key in dot notation (e.g., 'database.host').
            default: Default value to return if key doesn't exist.
        
        Returns:
            Configuration value or default if key doesn't exist.
        
        Example:
            >>> config.get('database.host')
            '192.168.0.131'
            >>> config.get('database.timeout', default=30)
            30
        """
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default
        
        return value
    
    def require(self, key: str) -> Any:
        """
        Get a required configuration value.
        
        Args:
            key: Configuration key in dot notation.
        
        Returns:
            Configuration value.
        
        Raises:
            ConfigError: If the required key doesn't exist.
        
        Example:
            >>> config.require('database.host')
            '192.168.0.131'
            >>> config.require('missing.key')
            ConfigError: Required configuration key not found: missing.key
        """
        value = self.get(key)
        if value is None:
            raise ConfigError(f"Required configuration key not found: {key}")
        return value
    
    def has(self, key: str) -> bool:
        """
        Check if a configuration key exists.
        
        Args:
            key: Configuration key in dot notation.
        
        Returns:
            True if key exists, False otherwise.
        
        Example:
            >>> config.has('database.host')
            True
            >>> config.has('missing.key')
            False
        """
        return self.get(key) is not None