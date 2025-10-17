#!/usr/bin/env python3
"""
MySQL Settings Checker Demo

This script demonstrates the MySQL/MariaDB settings checker module
with before/after examples showing real improvements.

Usage:
    python examples/demo_mysql_checker.py [config_file]
    
Example:
    python examples/demo_mysql_checker.py config/test/mysql80.yml
"""

import argparse
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from tuning_fork.config import Config
from tuning_fork.testing_modules.mysql.check_settings import (
    check_settings,
    report_settings,
    apply_settings,
    rollback_settings
)


def print_section(title: str) -> None:
    """Print a formatted section header."""
    print(f"\n{'='*80}")
    print(f"{title}")
    print('='*80 + "\n")


def demo_check_settings(config: Config) -> None:
    """Demonstrate checking MySQL settings."""
    print_section("Step 1: Check Current MySQL Settings")
    
    print("Analyzing your MySQL/MariaDB configuration...\n")
    
    # Check all settings
    results = check_settings(config)
    
    # Generate and display report
    report = report_settings(results, format='text')
    print(report)
    
    # Summary
    critical = len([r for r in results if r.status == 'CRITICAL'])
    warnings = len([r for r in results if r.status == 'WARNING'])
    ok = len([r for r in results if r.status == 'OK'])
    
    print(f"\n📊 Analysis Complete:")
    print(f"   🔴 {critical} critical issues found")
    print(f"   🟡 {warnings} warnings found")
    print(f"   🟢 {ok} settings optimal")
    
    return results


def demo_safe_apply(config: Config) -> None:
    """Demonstrate safely applying a setting."""
    print_section("Step 2: Apply Safe Setting (thread_cache_size)")
    
    print("We'll apply one safe, dynamic setting as a demonstration.\n")
    
    # Check current value
    print("BEFORE:")
    results = check_settings(config, ['thread_cache_size'])
    current = results[0].current_value
    print(f"  thread_cache_size: {current}")
    
    # Apply setting
    print("\nApplying recommended value (32)...")
    try:
        changes = apply_settings(
            config,
            {'thread_cache_size': 32},
            persist=True
        )
        
        print(f"✓ Successfully applied {len(changes)} setting(s)")
        
        # Verify change
        print("\nAFTER:")
        results = check_settings(config, ['thread_cache_size'])
        new_value = results[0].current_value
        print(f"  thread_cache_size: {new_value}")
        
        print(f"\n💡 Improvement: {current} → {new_value}")
        print("   This change is now active and will persist across restarts (MySQL 8.0+)")
        
        return changes
        
    except Exception as e:
        print(f"✗ Failed to apply setting: {e}")
        return None


def demo_rollback(config: Config, changes) -> None:
    """Demonstrate rolling back changes."""
    if not changes:
        print("\nNo changes to rollback.")
        return
    
    print_section("Step 3: Rollback (Optional Demo)")
    
    print("Demonstrating rollback capability...\n")
    
    # Show what will be rolled back
    print("Changes to rollback:")
    for change in changes:
        print(f"  {change.setting_name}: {change.new_value} → {change.old_value}")
    
    # Ask for confirmation
    response = input("\nRollback the change? (y/N): ")
    
    if response.lower() == 'y':
        try:
            rolled_back = rollback_settings(config, changes)
            print(f"\n✓ Rolled back {len(rolled_back)} setting(s)")
            
            # Verify
            results = check_settings(config, ['thread_cache_size'])
            print(f"  thread_cache_size is now: {results[0].current_value}")
            
        except Exception as e:
            print(f"✗ Rollback failed: {e}")
    else:
        print("\nSkipping rollback. Change remains applied.")


def demo_report_formats(config: Config) -> None:
    """Demonstrate different report formats."""
    print_section("Step 4: Different Report Formats")
    
    results = check_settings(config, ['max_connections', 'innodb_buffer_pool_size'])
    
    print("📝 Text Format (default):")
    print("-" * 80)
    print(report_settings(results, format='text'))
    
    print("\n📝 JSON Format (machine-readable):")
    print("-" * 80)
    print(report_settings(results, format='json'))
    
    print("\n📝 Markdown Format (documentation):")
    print("-" * 80)
    print(report_settings(results, format='markdown'))


def main():
    """Main demo function."""
    parser = argparse.ArgumentParser(
        description='MySQL Settings Checker Demo',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Demo with MySQL 8.0
  python examples/demo_mysql_checker.py config/test/mysql80.yml
  
  # Demo with MariaDB
  python examples/demo_mysql_checker.py config/test/mariadb.yml
        """
    )
    parser.add_argument(
        'config',
        nargs='?',
        default='config/test/mysql80.yml',
        help='Path to config file (default: config/test/mysql80.yml)'
    )
    parser.add_argument(
        '--no-apply',
        action='store_true',
        help='Skip the apply/rollback demo'
    )
    
    args = parser.parse_args()
    
    print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║                     MySQL/MariaDB Settings Checker Demo                      ║
║                                                                              ║
║  This demo shows how to analyze, optimize, and manage MySQL configuration   ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
    """)
    
    try:
        # Load configuration
        print(f"Loading configuration from: {args.config}")
        config = Config(args.config)
        
        # Demo 1: Check settings
        results = demo_check_settings(config)
        
        # Demo 2 & 3: Apply and rollback (if not skipped)
        if not args.no_apply:
            changes = demo_safe_apply(config)
            if changes:
                demo_rollback(config, changes)
        
        # Demo 4: Report formats
        demo_report_formats(config)
        
        # Final summary
        print_section("Demo Complete! 🎉")
        
        print("""
What you learned:
  ✓ How to check MySQL/MariaDB settings against best practices
  ✓ How to identify critical issues, warnings, and optimal settings
  ✓ How to safely apply configuration changes
  ✓ How to rollback changes if needed
  ✓ How to generate reports in multiple formats

Next Steps:
  1. Review your actual production MySQL settings
  2. Apply recommended changes in a test environment first
  3. Monitor performance after changes
  4. Use the change log for audit trails

Documentation:
  - README.md for quick start
  - examples/ for more usage examples
  - src/tuning_fork/testing_modules/mysql/check_settings.py for API docs

Happy tuning! 🚀
        """)
        
        return 0
        
    except FileNotFoundError:
        print(f"\n✗ Config file not found: {args.config}")
        print("\nMake sure you've run the Docker setup script:")
        print("  ./scripts/setup_mysql_docker.sh")
        return 1
        
    except Exception as e:
        print(f"\n✗ Demo failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())