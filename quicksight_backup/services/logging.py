"""
Comprehensive logging service for QuickSight backup operations.
"""

import logging
import logging.handlers
import json
import os
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path
from dataclasses import asdict

from ..models.backup_result import BackupResult, BackupReport, BackupStatus
from ..models.config import BackupConfig


class StructuredFormatter(logging.Formatter):
    """Custom formatter for structured logging with JSON output."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as structured JSON."""
        log_data = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # Add context data if available
        if hasattr(record, 'context') and record.context:
            log_data['context'] = record.context
            
        # Add exception info if available
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)
            
        return json.dumps(log_data, default=str)


class ProgressTracker:
    """Tracks progress of backup operations."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.current_operation: Optional[str] = None
        self.total_items: int = 0
        self.processed_items: int = 0
        self.failed_items: int = 0
        self.start_time: Optional[datetime] = None
        
    def start_operation(self, operation_name: str, total_items: int = 0) -> None:
        """Start tracking a new operation."""
        self.current_operation = operation_name
        self.total_items = total_items
        self.processed_items = 0
        self.failed_items = 0
        self.start_time = datetime.now()
        
        self.logger.info(
            f"Starting operation: {operation_name}",
            extra={'context': {
                'operation': operation_name,
                'total_items': total_items,
                'start_time': self.start_time.isoformat()
            }}
        )
        
    def update_progress(self, processed: int = 1, failed: int = 0) -> None:
        """Update progress counters."""
        self.processed_items += processed
        self.failed_items += failed
        
        if self.total_items > 0:
            progress_percent = (self.processed_items / self.total_items) * 100
            self.logger.info(
                f"Progress: {self.processed_items}/{self.total_items} ({progress_percent:.1f}%)",
                extra={'context': {
                    'operation': self.current_operation,
                    'processed': self.processed_items,
                    'failed': self.failed_items,
                    'total': self.total_items,
                    'progress_percent': progress_percent
                }}
            )
        else:
            self.logger.info(
                f"Processed: {self.processed_items}, Failed: {self.failed_items}",
                extra={'context': {
                    'operation': self.current_operation,
                    'processed': self.processed_items,
                    'failed': self.failed_items
                }}
            )
            
    def complete_operation(self) -> Dict[str, Any]:
        """Complete the current operation and return summary."""
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds() if self.start_time else 0
        
        summary = {
            'operation': self.current_operation,
            'total_items': self.total_items,
            'processed_items': self.processed_items,
            'failed_items': self.failed_items,
            'success_rate': ((self.processed_items - self.failed_items) / max(self.processed_items, 1)) * 100,
            'duration_seconds': duration,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': end_time.isoformat()
        }
        
        self.logger.info(
            f"Completed operation: {self.current_operation}",
            extra={'context': summary}
        )
        
        return summary


class LoggingService:
    """Comprehensive logging service for QuickSight backup operations."""
    
    def __init__(self, config: BackupConfig):
        """
        Initialize the logging service.
        
        Args:
            config: Backup configuration containing logging settings
        """
        self.config = config
        self.logger = self._setup_logger()
        self.progress_tracker = ProgressTracker(self.logger)
        self.operation_history: List[Dict[str, Any]] = []
        
    def _setup_logger(self) -> logging.Logger:
        """Set up the main logger with appropriate handlers and formatters."""
        logger = logging.getLogger('quicksight_backup')
        logger.setLevel(getattr(logging, self.config.logging_level.upper()))
        
        # Clear any existing handlers
        logger.handlers.clear()
        
        # Create log directory if it doesn't exist
        log_path = Path(self.config.logging_file_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        # File handler with structured logging
        file_handler = logging.handlers.RotatingFileHandler(
            self.config.logging_file_path,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5
        )
        file_handler.setFormatter(StructuredFormatter())
        logger.addHandler(file_handler)
        
        # Console handler with simple formatting
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        return logger
        
    def log_info(self, message: str, context: Optional[Dict[str, Any]] = None) -> None:
        """Log an info message with optional context."""
        self.logger.info(message, extra={'context': context or {}})
        
    def log_warning(self, message: str, context: Optional[Dict[str, Any]] = None) -> None:
        """Log a warning message with optional context."""
        self.logger.warning(message, extra={'context': context or {}})
        
    def log_error(self, message: str, error: Optional[Exception] = None, 
                  context: Optional[Dict[str, Any]] = None) -> None:
        """Log an error message with optional exception and context."""
        extra_context = context or {}
        if error:
            extra_context.update({
                'error_type': type(error).__name__,
                'error_message': str(error)
            })
        
        self.logger.error(message, exc_info=error, extra={'context': extra_context})
        
    def log_debug(self, message: str, context: Optional[Dict[str, Any]] = None) -> None:
        """Log a debug message with optional context."""
        self.logger.debug(message, extra={'context': context or {}})
        
    def start_backup_operation(self, operation_name: str, total_items: int = 0) -> None:
        """Start tracking a backup operation."""
        self.progress_tracker.start_operation(operation_name, total_items)
        
    def update_backup_progress(self, processed: int = 1, failed: int = 0) -> None:
        """Update backup operation progress."""
        self.progress_tracker.update_progress(processed, failed)
        
    def complete_backup_operation(self) -> Dict[str, Any]:
        """Complete the current backup operation."""
        summary = self.progress_tracker.complete_operation()
        self.operation_history.append(summary)
        return summary
        
    def generate_backup_report(self, results: List[BackupResult]) -> BackupReport:
        """
        Generate a comprehensive backup report from backup results.
        
        Args:
            results: List of backup results from different operations
            
        Returns:
            BackupReport: Comprehensive backup report
        """
        if not results:
            return BackupReport(
                total_resources=0,
                successful_resources=0,
                failed_resources=0,
                partial_resources=0,
                total_execution_time=0.0,
                start_time=datetime.now(),
                end_time=datetime.now(),
                results=[]
            )
            
        # Calculate report metrics
        total_resources = len(results)
        successful_resources = sum(1 for r in results if r.status == BackupStatus.SUCCESS)
        failed_resources = sum(1 for r in results if r.status == BackupStatus.FAILED)
        partial_resources = sum(1 for r in results if r.status == BackupStatus.PARTIAL)
        total_execution_time = sum(r.execution_time for r in results)
        
        # Get time range
        start_time = min(r.timestamp for r in results)
        end_time = max(r.timestamp for r in results)
        
        report = BackupReport(
            total_resources=total_resources,
            successful_resources=successful_resources,
            failed_resources=failed_resources,
            partial_resources=partial_resources,
            total_execution_time=total_execution_time,
            start_time=start_time,
            end_time=end_time,
            results=results
        )
        
        # Log the report summary
        self.log_info(
            "Backup report generated",
            context={
                'total_resources': total_resources,
                'successful_resources': successful_resources,
                'failed_resources': failed_resources,
                'partial_resources': partial_resources,
                'success_rate': report.success_rate,
                'total_execution_time': total_execution_time,
                'duration': (end_time - start_time).total_seconds()
            }
        )
        
        return report
        
    def save_backup_report(self, report: BackupReport, output_path: Optional[str] = None) -> str:
        """
        Save backup report to a JSON file.
        
        Args:
            report: The backup report to save
            output_path: Optional custom output path
            
        Returns:
            str: Path to the saved report file
        """
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"backup_report_{timestamp}.json"
            
        # Convert report to dictionary for JSON serialization
        report_data = {
            'summary': {
                'total_resources': report.total_resources,
                'successful_resources': report.successful_resources,
                'failed_resources': report.failed_resources,
                'partial_resources': report.partial_resources,
                'success_rate': report.success_rate,
                'total_execution_time': report.total_execution_time,
                'start_time': report.start_time.isoformat(),
                'end_time': report.end_time.isoformat()
            },
            'results': [asdict(result) for result in report.results],
            'operation_history': self.operation_history,
            'generated_at': datetime.now().isoformat()
        }
        
        # Ensure output directory exists
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Save report
        with open(output_file, 'w') as f:
            json.dump(report_data, f, indent=2, default=str)
            
        self.log_info(
            f"Backup report saved to {output_path}",
            context={'report_path': str(output_file.absolute())}
        )
        
        return str(output_file.absolute())
        
    def log_aws_api_call(self, service: str, operation: str, success: bool, 
                        duration: float, context: Optional[Dict[str, Any]] = None) -> None:
        """
        Log AWS API call details for monitoring and debugging.
        
        Args:
            service: AWS service name (e.g., 'quicksight', 's3', 'dynamodb')
            operation: API operation name
            success: Whether the call was successful
            duration: Call duration in seconds
            context: Additional context information
        """
        log_context = {
            'aws_service': service,
            'aws_operation': operation,
            'success': success,
            'duration_seconds': duration,
            'api_call': True
        }
        
        if context:
            log_context.update(context)
            
        level = 'info' if success else 'error'
        message = f"AWS API call: {service}.{operation} ({'success' if success else 'failed'})"
        
        if level == 'info':
            self.log_info(message, log_context)
        else:
            self.log_error(message, context=log_context)
            
    def get_logger(self) -> logging.Logger:
        """Get the underlying logger instance for direct use."""
        return self.logger
        
    def close(self) -> None:
        """Close all logging handlers and clean up resources."""
        for handler in self.logger.handlers:
            handler.close()
        self.logger.handlers.clear()