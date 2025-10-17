"""
Testing module runner and orchestrator.

This module provides functionality to execute testing modules based on
configuration file settings, managing the workflow of check, report,
apply, and rollback operations.
"""

import logging
from typing import Any, Optional

from tuning_fork.config import Config

# Module-level logger
logger = logging.getLogger(__name__)


class ModuleRunnerError(Exception):
    """Raised when module execution fails."""
    pass


class ModuleRunner:
    """
    Orchestrator for running testing modules.
    
    This class manages the execution of testing modules as specified in the
    configuration file, handling module discovery, execution order, and
    error handling.
    
    Attributes:
        config: Configuration object containing module settings.
    """
    
    def __init__(self, config: Config) -> None:
        """
        Initialize module runner.
        
        Args:
            config: Configuration object with testing_modules settings.
        
        Raises:
            ModuleRunnerError: If configuration is invalid.
        """
        self.config = config
        
        # Validate configuration
        if not config.has('testing_modules'):
            raise ModuleRunnerError("No testing_modules configuration found")
    
    def get_enabled_modules(self, db_type: str) -> list[str]:
        """
        Get list of enabled modules for a database type.
        
        Args:
            db_type: Database type (e.g., 'pgsql', 'mysql_mariadb').
        
        Returns:
            List of enabled module names.
        
        Example:
            >>> runner = ModuleRunner(config)
            >>> modules = runner.get_enabled_modules('pgsql')
            >>> print(modules)
            ['check_settings', 'check_connections']
        """
        modules_config = self.config.get(f'testing_modules.{db_type}', {})
        
        if not modules_config.get('enabled', False):
            logger.info(f"Database type '{db_type}' is not enabled")
            return []
        
        modules = modules_config.get('modules', [])
        logger.info(f"Found {len(modules)} enabled modules for '{db_type}'")
        
        return modules
    
    def run_pgsql_module(self, module_name: str) -> dict[str, Any]:
        """
        Run a PostgreSQL testing module.
        
        Args:
            module_name: Name of the module to run (e.g., 'check_settings', 'check_connections').
        
        Returns:
            Dictionary containing module execution results.
        
        Raises:
            ModuleRunnerError: If module is unknown or execution fails.
        
        Example:
            >>> runner = ModuleRunner(config)
            >>> result = runner.run_pgsql_module('check_settings')
            >>> print(result['status'])
            'success'
        """
        logger.info(f"Running PostgreSQL module: {module_name}")
        
        result: dict[str, Any] = {
            'module': module_name,
            'status': 'unknown',
            'database_type': 'pgsql'
        }
        
        try:
            if module_name == 'check_settings':
                from tuning_fork.testing_modules.pgsql import (
                    check_settings,
                    report_settings
                )
                
                # Get workload type from config
                workload_type = self.config.get(
                    'testing_modules.pgsql.check_settings.workload_type',
                    default='OLTP'
                )
                
                # Get report format from config
                report_format = self.config.get(
                    'testing_modules.pgsql.check_settings.report_format',
                    default='text'
                )
                
                # Run check
                check_results = check_settings(self.config, workload_type=workload_type)
                
                # Generate report
                report = report_settings(check_results, format=report_format)
                
                result.update({
                    'status': 'success',
                    'results': check_results,
                    'report': report,
                    'workload_type': workload_type
                })
                
            elif module_name == 'check_connections':
                from tuning_fork.testing_modules.pgsql import (
                    check_connections,
                    report_connections
                )
                
                # Get custom thresholds from config if provided
                custom_thresholds = self.config.get(
                    'testing_modules.pgsql.check_connections.thresholds',
                    default=None
                )
                
                # Get report format from config
                report_format = self.config.get(
                    'testing_modules.pgsql.check_connections.report_format',
                    default='text'
                )
                
                # Get include_ok flag from config
                include_ok = self.config.get(
                    'testing_modules.pgsql.check_connections.include_ok',
                    default=False
                )
                
                # Run check
                check_results = check_connections(
                    self.config,
                    thresholds=custom_thresholds
                )
                
                # Generate report
                report = report_connections(
                    check_results,
                    format=report_format,
                    include_ok=include_ok
                )
                
                result.update({
                    'status': 'success',
                    'results': check_results,
                    'report': report,
                    'total_findings': len(check_results),
                    'critical_count': len([r for r in check_results if r.status == 'CRITICAL']),
                    'warning_count': len([r for r in check_results if r.status == 'WARNING'])
                })
                
            else:
                result.update({
                    'status': 'error',
                    'error': f"Unknown PostgreSQL module: {module_name}"
                })
                logger.error(f"Unknown module: {module_name}")
            
        except Exception as exc:
            result.update({
                'status': 'error',
                'error': str(exc)
            })
            logger.error(f"Module execution failed: {exc}", exc_info=True)
        
        return result
    
    def run_mysql_module(self, module_name: str) -> dict[str, Any]:
        """
        Run a MySQL/MariaDB testing module.
        
        Args:
            module_name: Name of the module to run (e.g., 'check_settings').
        
        Returns:
            Dictionary containing module execution results.
        
        Raises:
            ModuleRunnerError: If module is unknown or execution fails.
        
        Example:
            >>> runner = ModuleRunner(config)
            >>> result = runner.run_mysql_module('check_settings')
            >>> print(result['status'])
            'success'
        """
        logger.info(f"Running MySQL/MariaDB module: {module_name}")
        
        return {
            'status': 'error',
            'module': module_name,
            'error': f"MySQL/MariaDB module '{module_name}' not yet implemented"
        }
    
    def run_modules(self, db_type: str) -> list[dict[str, Any]]:
        """
        Run all enabled modules for a database type.
        
        Args:
            db_type: Database type (e.g., 'pgsql', 'mysql_mariadb').
        
        Returns:
            List of module execution result dictionaries.
        
        Example:
            >>> runner = ModuleRunner(config)
            >>> results = runner.run_modules('pgsql')
            >>> for result in results:
            ...     print(f"{result['module']}: {result['status']}")
        """
        modules = self.get_enabled_modules(db_type)
        results = []
        
        if not modules:
            logger.warning(f"No modules enabled for database type: {db_type}")
            return results
        
        for module_name in modules:
            try:
                if db_type == 'pgsql':
                    result = self.run_pgsql_module(module_name)
                elif db_type in ('mysql', 'mariadb', 'mysql_mariadb'):
                    result = self.run_mysql_module(module_name)
                else:
                    result = {
                        'status': 'error',
                        'module': module_name,
                        'error': f"Unsupported database type: {db_type}"
                    }
                
                results.append(result)
                
            except Exception as exc:
                logger.error(f"Error running module '{module_name}': {exc}")
                results.append({
                    'status': 'error',
                    'module': module_name,
                    'error': str(exc)
                })
        
        return results
    
    def run_all(self) -> dict[str, list[dict[str, Any]]]:
        """
        Run all enabled modules for all database types.
        
        Returns:
            Dictionary mapping database types to their module results.
        
        Example:
            >>> runner = ModuleRunner(config)
            >>> all_results = runner.run_all()
            >>> for db_type, results in all_results.items():
            ...     print(f"{db_type}: {len(results)} modules executed")
        """
        all_results: dict[str, list[dict[str, Any]]] = {}
        
        # Get all database types from config
        testing_modules = self.config.get('testing_modules', {})
        
        for db_type in testing_modules.keys():
            logger.info(f"Processing database type: {db_type}")
            results = self.run_modules(db_type)
            
            if results:
                all_results[db_type] = results
        
        return all_results
    
    def print_summary(self, all_results: dict[str, list[dict[str, Any]]]) -> None:
        """
        Print a summary of all module execution results.
        
        Args:
            all_results: Dictionary of results from run_all().
        
        Example:
            >>> runner = ModuleRunner(config)
            >>> results = runner.run_all()
            >>> runner.print_summary(results)
        """
        print("=" * 80)
        print("TUNING FORK - MODULE EXECUTION SUMMARY")
        print("=" * 80)
        print()
        
        if not all_results:
            print("No modules were executed.")
            return
        
        for db_type, results in all_results.items():
            print(f"Database Type: {db_type.upper()}")
            print("-" * 80)
            
            for result in results:
                module_name = result['module']
                status = result['status']
                
                status_symbol = "✓" if status == 'success' else "✗"
                print(f"  {status_symbol} {module_name}: {status.upper()}")
                
                if status == 'error':
                    print(f"    Error: {result.get('error', 'Unknown error')}")
                elif status == 'success' and 'report' in result:
                    # Print a condensed version of the report
                    report_lines = result['report'].split('\n')
                    # Find and print summary line
                    for line in report_lines:
                        if 'Summary:' in line:
                            print(f"    {line.strip()}")
                            break
            
            print()
        
        print("=" * 80)