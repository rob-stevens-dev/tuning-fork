#!/usr/bin/env python3
"""
Example CLI for Tuning Fork testing modules.

This script demonstrates how to use the module runner to execute
database testing and tuning operations.
"""

import argparse
import logging
import sys
from pathlib import Path

from tuning_fork.config import Config
from tuning_fork.module_runner import ModuleRunner, ModuleRunnerError
from tuning_fork.testing_modules.pgsql.config.check_settings import (
    apply_settings,
    rollback_settings,
)


def setup_logging(verbose: bool = False) -> None:
    """
    Set up logging configuration.
    
    Args:
        verbose: If True, set logging level to DEBUG.
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def cmd_check(args: argparse.Namespace) -> int:
    """
    Execute check command - runs configured testing modules.
    
    Args:
        args: Parsed command-line arguments.
    
    Returns:
        Exit code (0 for success, 1 for failure).
    """
    try:
        config = Config(args.config)
        runner = ModuleRunner(config)
        
        if args.database:
            # Run modules for specific database type
            print(f"Checking {args.database} configuration...")
            results = runner.run_modules(args.database)
            
            if not results:
                print(f"No modules enabled for {args.database}")
                return 0
            
            # Print results
            for result in results:
                if result['status'] == 'success':
                    print(f"\n{result['report']}")
                else:
                    print(f"\nError in {result['module']}: {result.get('error', 'Unknown error')}")
        else:
            # Run all enabled modules
            print("Checking all enabled database configurations...")
            all_results = runner.run_all()
            runner.print_summary(all_results)
            
            # Print detailed reports if requested
            if args.detailed:
                for db_type, results in all_results.items():
                    print(f"\n{'=' * 80}")
                    print(f"Detailed Report for {db_type.upper()}")
                    print('=' * 80)
                    for result in results:
                        if result['status'] == 'success':
                            print(f"\n{result['report']}")
        
        return 0
    
    except ModuleRunnerError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Unexpected error: {exc}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


def cmd_apply(args: argparse.Namespace) -> int:
    """
    Execute apply command - applies recommended settings.
    
    Args:
        args: Parsed command-line arguments.
    
    Returns:
        Exit code (0 for success, 1 for failure).
    """
    try:
        config = Config(args.config)
        
        # Parse settings from command line
        settings_to_apply = {}
        for setting in args.settings:
            try:
                key, value = setting.split('=', 1)
                settings_to_apply[key.strip()] = value.strip()
            except ValueError:
                print(f"Invalid setting format: {setting}. Use KEY=VALUE", file=sys.stderr)
                return 1
        
        if not settings_to_apply:
            print("No settings specified. Use --setting KEY=VALUE", file=sys.stderr)
            return 1
        
        # Confirm with user unless --yes is specified
        if not args.yes:
            print("The following settings will be applied:")
            for key, value in settings_to_apply.items():
                print(f"  {key} = {value}")
            print("\nWARNING: This will modify your database configuration!")
            print("A database reload or restart may be required for changes to take effect.")
            
            response = input("\nContinue? (yes/no): ")
            if response.lower() not in ('yes', 'y'):
                print("Aborted.")
                return 0
        
        # Apply settings
        print("\nApplying settings...")
        changes = apply_settings(config, settings_to_apply)
        
        print(f"\nSuccessfully applied {len(changes)} settings:")
        for change in changes:
            print(f"  ✓ {change.setting_name}: {change.old_value} → {change.new_value}")
        
        print("\nIMPORTANT: Run 'SELECT pg_reload_conf();' or restart PostgreSQL for changes to take effect.")
        
        # Save change log to file if requested
        if args.save_changes:
            import json
            changes_file = Path(args.save_changes)
            changes_data = [change.to_dict() for change in changes]
            
            with open(changes_file, 'w') as f:
                json.dump(changes_data, f, indent=2, default=str)
            
            print(f"\nChange log saved to: {changes_file}")
            print("Use this file with 'tuning-fork rollback' if needed.")
        
        return 0
    
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


def cmd_rollback(args: argparse.Namespace) -> int:
    """
    Execute rollback command - reverts previously applied settings.
    
    Args:
        args: Parsed command-line arguments.
    
    Returns:
        Exit code (0 for success, 1 for failure).
    """
    try:
        config = Config(args.config)
        
        # Load change log from file
        import json
        from tuning_fork.testing_modules.pgsql.config.check_settings import ChangeLogEntry
        
        changes_file = Path(args.changes_file)
        if not changes_file.exists():
            print(f"Change log file not found: {changes_file}", file=sys.stderr)
            return 1
        
        with open(changes_file) as f:
            changes_data = json.load(f)
        
        # Convert to ChangeLogEntry objects
        changes = [ChangeLogEntry(**change) for change in changes_data]
        
        if not changes:
            print("No changes found in log file.")
            return 0
        
        # Confirm with user unless --yes is specified
        if not args.yes:
            print("The following settings will be rolled back:")
            for change in changes:
                if not change.rollback_status:
                    print(f"  {change.setting_name}: {change.new_value} → {change.old_value}")
            
            print("\nWARNING: This will modify your database configuration!")
            
            response = input("\nContinue? (yes/no): ")
            if response.lower() not in ('yes', 'y'):
                print("Aborted.")
                return 0
        
        # Rollback settings
        print("\nRolling back settings...")
        rolled_back = rollback_settings(config, changes)
        
        print(f"\nSuccessfully rolled back {len(rolled_back)} settings:")
        for setting_name in rolled_back:
            print(f"  ✓ {setting_name}")
        
        print("\nIMPORTANT: Run 'SELECT pg_reload_conf();' or restart PostgreSQL for changes to take effect.")
        
        # Update change log file
        updated_data = [change.to_dict() for change in changes]
        with open(changes_file, 'w') as f:
            json.dump(updated_data, f, indent=2, default=str)
        
        print(f"\nChange log updated: {changes_file}")
        
        return 0
    
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


def main() -> int:
    """
    Main entry point for the CLI.
    
    Returns:
        Exit code.
    """
    parser = argparse.ArgumentParser(
        description='Tuning Fork - Database Testing and Tuning Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check all enabled databases
  %(prog)s check
  
  # Check specific database
  %(prog)s check --database pgsql
  
  # Check with detailed reports
  %(prog)s check --detailed
  
  # Apply settings
  %(prog)s apply --setting work_mem=8MB --setting max_connections=200
  
  # Apply settings and save change log
  %(prog)s apply --setting work_mem=8MB --save-changes changes.json
  
  # Rollback settings
  %(prog)s rollback --changes-file changes.json
        """
    )
    
    parser.add_argument(
        '-c', '--config',
        default='config/config.dev.yml',
        help='Path to configuration file (default: config/config.dev.yml)'
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Check command
    check_parser = subparsers.add_parser(
        'check',
        help='Check database configuration'
    )
    check_parser.add_argument(
        '--database',
        choices=['pgsql', 'mysql_mariadb', 'mssql', 'oracle'],
        help='Check specific database type'
    )
    check_parser.add_argument(
        '--detailed',
        action='store_true',
        help='Show detailed reports'
    )
    
    # Apply command
    apply_parser = subparsers.add_parser(
        'apply',
        help='Apply configuration settings'
    )
    apply_parser.add_argument(
        '--setting',
        dest='settings',
        action='append',
        default=[],
        help='Setting to apply (format: KEY=VALUE). Can be specified multiple times.'
    )
    apply_parser.add_argument(
        '--yes', '-y',
        action='store_true',
        help='Skip confirmation prompt'
    )
    apply_parser.add_argument(
        '--save-changes',
        metavar='FILE',
        help='Save change log to file for rollback'
    )
    
    # Rollback command
    rollback_parser = subparsers.add_parser(
        'rollback',
        help='Rollback previously applied settings'
    )
    rollback_parser.add_argument(
        '--changes-file',
        required=True,
        help='Change log file from apply command'
    )
    rollback_parser.add_argument(
        '--yes', '-y',
        action='store_true',
        help='Skip confirmation prompt'
    )
    
    args = parser.parse_args()
    
    # Set up logging
    setup_logging(args.verbose)
    
    # Execute command
    if args.command == 'check':
        return cmd_check(args)
    elif args.command == 'apply':
        return cmd_apply(args)
    elif args.command == 'rollback':
        return cmd_rollback(args)
    else:
        parser.print_help()
        return 1


if __name__ == '__main__':
    sys.exit(main())