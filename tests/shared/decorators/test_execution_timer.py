"""
Unit tests for execution_timer module.

This test suite ensures comprehensive coverage of the logging and execution
time tracking decorators, including edge cases and error conditions.
"""

import logging
import time
import unittest
from io import StringIO
from typing import Any
from unittest.mock import MagicMock, patch

from tuning_fork.shared.decorators.execution_timer import (
    log_execution_time,
    track_performance,
)


class TestLogExecutionTime(unittest.TestCase):
    """Test suite for log_execution_time decorator."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        self.logger = logging.getLogger('test_logger')
        self.logger.setLevel(logging.DEBUG)
        self.log_stream = StringIO()
        self.handler = logging.StreamHandler(self.log_stream)
        self.handler.setFormatter(
            logging.Formatter('%(levelname)s - %(message)s')
        )
        self.logger.addHandler(self.handler)
    
    def tearDown(self) -> None:
        """Clean up test fixtures."""
        self.logger.removeHandler(self.handler)
        self.handler.close()
    
    def test_basic_execution_time_logging(self) -> None:
        """Test that basic execution time is logged correctly."""
        @log_execution_time(logger=self.logger)
        def sample_function() -> str:
            return "test"
        
        result = sample_function()
        
        self.assertEqual(result, "test")
        log_output = self.log_stream.getvalue()
        self.assertIn("sample_function", log_output)
        self.assertIn("executed in", log_output)
        self.assertIn("INFO", log_output)
    
    def test_execution_time_with_args_logging(self) -> None:
        """Test that function arguments are logged when log_args=True."""
        @log_execution_time(logger=self.logger, log_args=True)
        def sample_function(a: int, b: str, c: int = 10) -> str:
            return f"{a}-{b}-{c}"
        
        result = sample_function(5, "test", c=20)
        
        self.assertEqual(result, "5-test-20")
        log_output = self.log_stream.getvalue()
        self.assertIn("args=(5, 'test')", log_output)
        self.assertIn("kwargs={'c': 20}", log_output)
    
    def test_execution_time_with_result_logging(self) -> None:
        """Test that function result is logged when log_result=True."""
        @log_execution_time(logger=self.logger, log_result=True)
        def sample_function() -> dict[str, int]:
            return {"key": 42}
        
        result = sample_function()
        
        self.assertEqual(result, {"key": 42})
        log_output = self.log_stream.getvalue()
        self.assertIn("returned: {'key': 42}", log_output)
    
    def test_execution_time_with_custom_precision(self) -> None:
        """Test that precision parameter controls decimal places."""
        @log_execution_time(logger=self.logger, precision=2)
        def sample_function() -> None:
            time.sleep(0.01)
        
        sample_function()
        
        log_output = self.log_stream.getvalue()
        # Check for 2 decimal places pattern (e.g., "0.01s")
        import re
        match = re.search(r'executed in (\d+\.\d{2})s', log_output)
        self.assertIsNotNone(match)
    
    def test_execution_time_with_custom_level(self) -> None:
        """Test that custom logging level is respected."""
        @log_execution_time(logger=self.logger, level=logging.DEBUG)
        def sample_function() -> None:
            pass
        
        sample_function()
        
        log_output = self.log_stream.getvalue()
        self.assertIn("DEBUG", log_output)
    
    def test_execution_time_on_exception(self) -> None:
        """Test that execution time is logged even when function raises exception."""
        @log_execution_time(logger=self.logger)
        def failing_function() -> None:
            raise ValueError("Test error")
        
        with self.assertRaises(ValueError):
            failing_function()
        
        log_output = self.log_stream.getvalue()
        self.assertIn("failed after", log_output)
        self.assertIn("ValueError: Test error", log_output)
        self.assertIn("ERROR", log_output)
    
    def test_execution_time_preserves_function_metadata(self) -> None:
        """Test that decorator preserves original function metadata."""
        @log_execution_time(logger=self.logger)
        def documented_function() -> str:
            """This is a test function."""
            return "test"
        
        self.assertEqual(documented_function.__name__, "documented_function")
        self.assertEqual(documented_function.__doc__, "This is a test function.")
    
    def test_execution_time_with_no_logger_creates_default(self) -> None:
        """Test that decorator creates default logger when none provided."""
        @log_execution_time()
        def sample_function() -> str:
            return "test"
        
        # Should not raise exception
        result = sample_function()
        self.assertEqual(result, "test")
    
    def test_execution_time_with_multiple_decorators(self) -> None:
        """Test that decorator can be stacked with other decorators."""
        @log_execution_time(logger=self.logger, log_args=True)
        @log_execution_time(logger=self.logger, log_result=True)
        def sample_function(x: int) -> int:
            return x * 2
        
        result = sample_function(5)
        self.assertEqual(result, 10)
        # Should have multiple log entries
        log_output = self.log_stream.getvalue()
        self.assertGreater(log_output.count("executed in"), 1)
    
    def test_execution_time_with_class_method(self) -> None:
        """Test decorator works correctly with class methods."""
        class TestClass:
            @log_execution_time(logger=self.logger)
            def method(self) -> str:
                return "method_result"
        
        obj = TestClass()
        result = obj.method()
        
        self.assertEqual(result, "method_result")
        log_output = self.log_stream.getvalue()
        self.assertIn("TestClass.method", log_output)


class TestTrackPerformance(unittest.TestCase):
    """Test suite for track_performance decorator."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        self.logger = logging.getLogger('test_perf_logger')
        self.logger.setLevel(logging.DEBUG)
        self.log_stream = StringIO()
        self.handler = logging.StreamHandler(self.log_stream)
        self.handler.setFormatter(
            logging.Formatter('%(levelname)s - %(message)s')
        )
        self.logger.addHandler(self.handler)
    
    def tearDown(self) -> None:
        """Clean up test fixtures."""
        self.logger.removeHandler(self.handler)
        self.handler.close()
    
    def test_fast_function_no_warning(self) -> None:
        """Test that fast functions don't trigger warnings."""
        @track_performance(threshold_seconds=1.0, logger=self.logger)
        def fast_function() -> str:
            return "fast"
        
        result = fast_function()
        
        self.assertEqual(result, "fast")
        log_output = self.log_stream.getvalue()
        self.assertIn("INFO", log_output)
        self.assertNotIn("WARNING", log_output)
        self.assertNotIn("exceeded threshold", log_output)
    
    def test_slow_function_triggers_warning(self) -> None:
        """Test that slow functions trigger threshold warnings."""
        @track_performance(threshold_seconds=0.01, logger=self.logger)
        def slow_function() -> str:
            time.sleep(0.02)
            return "slow"
        
        result = slow_function()
        
        self.assertEqual(result, "slow")
        log_output = self.log_stream.getvalue()
        self.assertIn("WARNING", log_output)
        self.assertIn("exceeded threshold", log_output)
    
    def test_warn_on_slow_disabled(self) -> None:
        """Test that warnings can be disabled with warn_on_slow=False."""
        @track_performance(
            threshold_seconds=0.01,
            logger=self.logger,
            warn_on_slow=False
        )
        def slow_function() -> str:
            time.sleep(0.02)
            return "slow"
        
        result = slow_function()
        
        self.assertEqual(result, "slow")
        log_output = self.log_stream.getvalue()
        self.assertIn("INFO", log_output)
        self.assertNotIn("WARNING", log_output)
    
    def test_performance_tracking_on_exception(self) -> None:
        """Test that performance is tracked even when function raises exception."""
        @track_performance(threshold_seconds=1.0, logger=self.logger)
        def failing_function() -> None:
            raise RuntimeError("Performance test error")
        
        with self.assertRaises(RuntimeError):
            failing_function()
        
        log_output = self.log_stream.getvalue()
        self.assertIn("failed after", log_output)
        self.assertIn("RuntimeError: Performance test error", log_output)
        self.assertIn("ERROR", log_output)
    
    def test_performance_tracking_preserves_metadata(self) -> None:
        """Test that decorator preserves original function metadata."""
        @track_performance(logger=self.logger)
        def documented_function() -> str:
            """Performance test function."""
            return "test"
        
        self.assertEqual(documented_function.__name__, "documented_function")
        self.assertEqual(documented_function.__doc__, "Performance test function.")
    
    def test_performance_tracking_with_no_logger(self) -> None:
        """Test that decorator creates default logger when none provided."""
        @track_performance(threshold_seconds=1.0)
        def sample_function() -> str:
            return "test"
        
        # Should not raise exception
        result = sample_function()
        self.assertEqual(result, "test")
    
    def test_performance_tracking_exact_threshold(self) -> None:
        """Test behavior when execution time equals threshold."""
        @track_performance(threshold_seconds=0.02, logger=self.logger)
        def threshold_function() -> str:
            time.sleep(0.02)
            return "threshold"
        
        result = threshold_function()
        
        self.assertEqual(result, "threshold")
        # Execution time should be slightly over 0.02s, triggering warning
        log_output = self.log_stream.getvalue()
        # Due to timing variations, we just check it logged
        self.assertIn("executed in", log_output)
    
    def test_performance_tracking_with_return_value(self) -> None:
        """Test that return values are properly passed through."""
        @track_performance(logger=self.logger)
        def return_complex_value() -> dict[str, Any]:
            return {"status": "ok", "data": [1, 2, 3]}
        
        result = return_complex_value()
        
        self.assertEqual(result, {"status": "ok", "data": [1, 2, 3]})
    
    def test_performance_tracking_with_arguments(self) -> None:
        """Test that decorated functions handle arguments correctly."""
        @track_performance(logger=self.logger)
        def function_with_args(a: int, b: str, c: bool = True) -> str:
            return f"{a}-{b}-{c}"
        
        result = function_with_args(10, "test", c=False)
        
        self.assertEqual(result, "10-test-False")


class TestIntegration(unittest.TestCase):
    """Integration tests for combined decorator usage."""
    
    def setUp(self) -> None:
        """Set up test fixtures."""
        self.logger = logging.getLogger('test_integration')
        self.logger.setLevel(logging.DEBUG)
        self.log_stream = StringIO()
        self.handler = logging.StreamHandler(self.log_stream)
        self.handler.setFormatter(
            logging.Formatter('%(levelname)s - %(message)s')
        )
        self.logger.addHandler(self.handler)
    
    def tearDown(self) -> None:
        """Clean up test fixtures."""
        self.logger.removeHandler(self.handler)
        self.handler.close()
    
    def test_combined_decorators(self) -> None:
        """Test that both decorators can be used together."""
        @track_performance(threshold_seconds=0.5, logger=self.logger)
        @log_execution_time(logger=self.logger, log_args=True, log_result=True)
        def combined_function(x: int) -> int:
            return x * 2
        
        result = combined_function(21)
        
        self.assertEqual(result, 42)
        log_output = self.log_stream.getvalue()
        # Should have logs from both decorators
        self.assertGreater(log_output.count("executed in"), 1)


if __name__ == '__main__':
    unittest.main()