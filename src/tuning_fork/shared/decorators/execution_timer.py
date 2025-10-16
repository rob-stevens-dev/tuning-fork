"""
Decorator module for logging and execution time tracking.

This module provides decorators for tracking function execution time and logging
function calls with configurable detail levels.
"""

import functools
import logging
import time
from typing import Any, Callable, Optional, TypeVar, cast

# Type variables for generic decorator typing
F = TypeVar('F', bound=Callable[..., Any])


def log_execution_time(
    logger: Optional[logging.Logger] = None,
    level: int = logging.INFO,
    log_args: bool = False,
    log_result: bool = False,
    precision: int = 4
) -> Callable[[F], F]:
    """
    Decorator that logs function execution time and optionally logs arguments and results.
    
    This decorator measures the execution time of a function and logs it using the
    specified logger. It can optionally log function arguments and return values for
    debugging purposes.
    
    Args:
        logger: Logger instance to use. If None, creates a logger based on the
                decorated function's module name.
        level: Logging level to use (default: logging.INFO).
        log_args: Whether to log function arguments (default: False).
        log_result: Whether to log function return value (default: False).
        precision: Number of decimal places for time display (default: 4).
    
    Returns:
        Decorated function with execution time logging.
    
    Example:
        >>> @log_execution_time(log_args=True, precision=2)
        ... def compute_sum(a: int, b: int) -> int:
        ...     return a + b
        >>> result = compute_sum(5, 3)
        INFO - Function 'compute_sum' called with args=(5, 3) kwargs={}
        INFO - Function 'compute_sum' executed in 0.0001s
    
    Notes:
        - Uses time.perf_counter() for high-resolution timing
        - Thread-safe for concurrent execution
        - Preserves function metadata via functools.wraps
    """
    def decorator(func: F) -> F:
        # Create logger from function's module if not provided
        nonlocal logger
        if logger is None:
            logger = logging.getLogger(func.__module__)
        
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            """Wrapper function that performs timing and logging."""
            func_name = func.__qualname__
            
            # Log function call with arguments if requested
            if log_args:
                logger.log(
                    level,
                    f"Function '{func_name}' called with args={args} kwargs={kwargs}"
                )
            
            # Measure execution time
            start_time = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                end_time = time.perf_counter()
                execution_time = end_time - start_time
                
                # Log execution time
                logger.log(
                    level,
                    f"Function '{func_name}' executed in {execution_time:.{precision}f}s"
                )
                
                # Log result if requested
                if log_result:
                    logger.log(
                        level,
                        f"Function '{func_name}' returned: {result!r}"
                    )
                
                return result
                
            except Exception as exc:
                end_time = time.perf_counter()
                execution_time = end_time - start_time
                
                # Log execution time even on failure
                logger.log(
                    logging.ERROR,
                    f"Function '{func_name}' failed after {execution_time:.{precision}f}s "
                    f"with {type(exc).__name__}: {exc}"
                )
                raise
        
        return cast(F, wrapper)
    
    return decorator


def track_performance(
    threshold_seconds: float = 1.0,
    logger: Optional[logging.Logger] = None,
    warn_on_slow: bool = True
) -> Callable[[F], F]:
    """
    Decorator that tracks performance and warns if execution exceeds a threshold.
    
    This decorator is useful for identifying performance bottlenecks by logging
    warnings when function execution time exceeds the specified threshold.
    
    Args:
        threshold_seconds: Time threshold in seconds. Functions exceeding this
                          duration will trigger a warning (default: 1.0).
        logger: Logger instance to use. If None, creates a logger based on the
                decorated function's module name.
        warn_on_slow: Whether to log a warning for slow executions (default: True).
    
    Returns:
        Decorated function with performance tracking.
    
    Example:
        >>> @track_performance(threshold_seconds=0.5)
        ... def slow_function():
        ...     time.sleep(0.6)
        ...     return "done"
        >>> result = slow_function()
        WARNING - Function 'slow_function' exceeded threshold: 0.6001s > 0.5000s
    
    Notes:
        - Always logs execution time at INFO level
        - Logs WARNING level if threshold is exceeded and warn_on_slow is True
        - Uses time.perf_counter() for accurate measurements
    """
    def decorator(func: F) -> F:
        # Create logger from function's module if not provided
        nonlocal logger
        if logger is None:
            logger = logging.getLogger(func.__module__)
        
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            """Wrapper function that performs performance tracking."""
            func_name = func.__qualname__
            
            start_time = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                end_time = time.perf_counter()
                execution_time = end_time - start_time
                
                # Log execution time
                logger.info(
                    f"Function '{func_name}' executed in {execution_time:.4f}s"
                )
                
                # Warn if execution exceeded threshold
                if warn_on_slow and execution_time > threshold_seconds:
                    logger.warning(
                        f"Function '{func_name}' exceeded threshold: "
                        f"{execution_time:.4f}s > {threshold_seconds:.4f}s"
                    )
                
                return result
                
            except Exception as exc:
                end_time = time.perf_counter()
                execution_time = end_time - start_time
                
                logger.error(
                    f"Function '{func_name}' failed after {execution_time:.4f}s "
                    f"with {type(exc).__name__}: {exc}"
                )
                raise
        
        return cast(F, wrapper)
    
    return decorator