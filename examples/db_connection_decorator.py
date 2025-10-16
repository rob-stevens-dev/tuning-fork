"""
Example usage of the database connection decorator.

This file demonstrates various ways to use the @with_database decorator
for automatic database connection management.
"""

from tuning_fork.config import Config
from tuning_fork.shared.decorators.db_connection import with_database
from tuning_fork.shared.decorators.execution_timer import log_execution_time

# Load configuration
config = Config('config.yaml')


# Example 1: Basic usage with autocommit
@with_database(config, autocommit=True)
def get_all_users(cursor=None) -> list[tuple]:
    """
    Fetch all users from the database.
    
    The cursor is automatically injected by the decorator.
    """
    cursor.execute("SELECT id, name, email FROM users")
    return cursor.fetchall()


# Example 2: Using both connection and cursor
@with_database(config, pass_connection=True, pass_cursor=True)
def create_user(name: str, email: str, connection=None, cursor=None) -> int:
    """
    Create a new user and return their ID.
    
    This function manually commits after insertion.
    """
    cursor.execute(
        "INSERT INTO users (name, email) VALUES (%s, %s) RETURNING id",
        (name, email)
    )
    user_id = cursor.fetchone()[0]
    connection.commit()
    return user_id


# Example 3: Using only connection (managing cursor yourself)
@with_database(config, pass_connection=True, pass_cursor=False)
def complex_transaction(user_data: list[dict], connection=None) -> None:
    """
    Perform a complex transaction with multiple operations.
    
    This demonstrates manual cursor management within a transaction.
    """
    with connection.cursor() as cursor:
        for user in user_data:
            cursor.execute(
                "INSERT INTO users (name, email) VALUES (%s, %s)",
                (user['name'], user['email'])
            )
        
        # Manually commit the transaction
        connection.commit()


# Example 4: Combining decorators (logging + database)
@log_execution_time(log_args=True, log_result=True)
@with_database(config, autocommit=True)
def get_user_by_id(user_id: int, cursor=None) -> tuple | None:
    """
    Get a user by ID with execution time logging.
    
    Decorators are stacked - database connection is managed first,
    then execution time is logged.
    """
    cursor.execute(
        "SELECT id, name, email FROM users WHERE id = %s",
        (user_id,)
    )
    return cursor.fetchone()


# Example 5: Error handling
@with_database(config, autocommit=True)
def update_user_email(user_id: int, new_email: str, cursor=None) -> bool:
    """
    Update a user's email address.
    
    Returns True if successful, False if user not found.
    Transaction is automatically rolled back on error.
    """
    cursor.execute(
        "UPDATE users SET email = %s WHERE id = %s",
        (new_email, user_id)
    )
    return cursor.rowcount > 0


# Example 6: Read-only query (no autocommit needed)
@with_database(config, autocommit=False)
def search_users(search_term: str, cursor=None) -> list[tuple]:
    """
    Search for users by name or email.
    
    Since this is read-only, autocommit is False.
    """
    cursor.execute(
        """
        SELECT id, name, email 
        FROM users 
        WHERE name ILIKE %s OR email ILIKE %s
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


if __name__ == '__main__':
    # Example usage
    try:
        # Fetch all users
        users = get_all_users()
        print(f"Found {len(users)} users")
        
        # Create a new user
        new_user_id = create_user("John Doe", "john@example.com")
        print(f"Created user with ID: {new_user_id}")
        
        # Get user by ID with logging
        user = get_user_by_id(new_user_id)
        print(f"Retrieved user: {user}")
        
        # Update user email
        success = update_user_email(new_user_id, "john.doe@example.com")
        print(f"Email update {'successful' if success else 'failed'}")
        
        # Search users
        results = search_users("john")
        print(f"Search found {len(results)} results")
        
        # Manual connection management
        manual_connection_management()
        
    except Exception as e:
        print(f"Error: {e}")
        
        
        
@log_execution_time()           # Times everything
@with_command_logging(config)   # Logs to SQLite  
@with_database(config)          # Provides cursor
def my_query(cursor=None):
    cursor.execute("SELECT * FROM users")
    return cursor.fetchall()