"""
Example usage of the database connection decorator with MySQL/MariaDB.

This file demonstrates various ways to use the @with_database decorator
for automatic MySQL/MariaDB connection management.
"""

from tuning_fork.config import Config
from tuning_fork.shared.decorators.db_connection import with_database
from tuning_fork.shared.decorators.execution_timer import log_execution_time

# Load MySQL configuration
config = Config('config/config.mysql.yml')


# Example 1: Basic MySQL usage with autocommit
@with_database(config, autocommit=True)
def get_all_mysql_users(cursor=None) -> list[tuple]:
    """
    Fetch all users from MySQL.
    
    The cursor is automatically injected by the decorator.
    """
    cursor.execute("SELECT user, host FROM mysql.user")
    return cursor.fetchall()


# Example 2: Using both connection and cursor
@with_database(config, pass_connection=True, pass_cursor=True)
def create_mysql_user(username: str, host: str, password: str, connection=None, cursor=None) -> None:
    """
    Create a new MySQL user.
    
    This function manually commits after creation.
    """
    cursor.execute(
        f"CREATE USER IF NOT EXISTS '{username}'@'{host}' IDENTIFIED BY %s",
        (password,)
    )
    cursor.execute(
        f"GRANT SELECT ON *.* TO '{username}'@'{host}'"
    )
    connection.commit()


# Example 3: Working with a table
@with_database(config, autocommit=True)
def get_users_from_table(cursor=None) -> list[tuple]:
    """
    Get users from a custom users table.
    """
    cursor.execute("SELECT id, username, email FROM users")
    return cursor.fetchall()


# Example 4: Combining decorators (logging + database)
@log_execution_time(log_args=True, log_result=True)
@with_database(config, autocommit=True)
def get_database_size(cursor=None) -> tuple:
    """
    Get MySQL database size with execution time logging.
    
    Decorators are stacked - database connection is managed first,
    then execution time is logged.
    """
    cursor.execute("""
        SELECT 
            table_schema AS database_name,
            ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS size_mb
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
        GROUP BY table_schema
    """)
    return cursor.fetchone()


# Example 5: Error handling with MySQL
@with_database(config, autocommit=True)
def insert_user(username: str, email: str, cursor=None) -> int:
    """
    Insert a user into the users table.
    
    Returns the new user's ID.
    Transaction is automatically rolled back on error.
    """
    cursor.execute(
        "INSERT INTO users (username, email) VALUES (%s, %s)",
        (username, email)
    )
    return cursor.lastrowid


# Example 6: Read-only query (no autocommit needed)
@with_database(config, autocommit=False)
def search_users(search_term: str, cursor=None) -> list[tuple]:
    """
    Search for users by username or email.
    
    Since this is read-only, autocommit is False.
    """
    cursor.execute(
        """
        SELECT id, username, email 
        FROM users 
        WHERE username LIKE %s OR email LIKE %s
        """,
        (f'%{search_term}%', f'%{search_term}%')
    )
    return cursor.fetchall()


# Example 7: Using with context manager directly
from tuning_fork.shared.decorators.db_connection import get_db_connection

def manual_connection_management() -> None:
    """
    Example of using the context manager directly instead of decorator.
    
    Useful when you need more control or when decorator isn't appropriate.
    """
    with get_db_connection(config) as (conn, cursor):
        cursor.execute("SELECT COUNT(*) FROM users")
        count = cursor.fetchone()[0]
        print(f"Total users: {count}")
        
        # Transaction is automatically committed when context exits


# Example 8: Show MySQL-specific features
@with_database(config, autocommit=True)
def show_mysql_variables(cursor=None) -> list[tuple]:
    """
    Get MySQL configuration variables.
    """
    cursor.execute("SHOW VARIABLES LIKE 'max_connections'")
    return cursor.fetchall()


# Example 9: Working with transactions
@with_database(config, pass_connection=True, pass_cursor=True, autocommit=False)
def transfer_credits(from_user: int, to_user: int, amount: float, connection=None, cursor=None) -> bool:
    """
    Transfer credits between users with transaction management.
    
    Returns True if successful, False otherwise.
    Demonstrates manual transaction control.
    """
    try:
        # Deduct from sender
        cursor.execute(
            "UPDATE users SET credits = credits - %s WHERE id = %s AND credits >= %s",
            (amount, from_user, amount)
        )
        
        if cursor.rowcount == 0:
            # Insufficient funds
            connection.rollback()
            return False
        
        # Add to receiver
        cursor.execute(
            "UPDATE users SET credits = credits + %s WHERE id = %s",
            (amount, to_user)
        )
        
        # Commit transaction
        connection.commit()
        return True
        
    except Exception as exc:
        connection.rollback()
        print(f"Transfer failed: {exc}")
        return False


if __name__ == '__main__':
    # Example usage
    try:
        # Check MySQL variables
        print("Checking MySQL configuration...")
        vars = show_mysql_variables()
        for var in vars:
            print(f"  {var[0]}: {var[1]}")
        
        # Get database size
        print("\nDatabase size:")
        size_info = get_database_size()
        if size_info:
            print(f"  {size_info[0]}: {size_info[1]} MB")
        
        # Manual connection management
        print("\nManual connection test:")
        manual_connection_management()
        
        print("\n✓ All MySQL examples completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")