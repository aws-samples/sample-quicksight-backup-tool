"""
Error handling and retry logic for QuickSight backup operations.
"""

import time
import random
import logging
from typing import Callable, Any, Optional, Dict, Type, List, Tuple
from functools import wraps
from botocore.exceptions import ClientError, BotoCoreError, NoCredentialsError
from botocore.exceptions import EndpointConnectionError, ConnectTimeoutError, ReadTimeoutError

from ..models.exceptions import (
    QuickSightBackupError, ConfigurationError, AWSCredentialsError,
    QuickSightAPIError, DynamoDBError, S3Error, BackupJobError
)


class RetryConfig:
    """Configuration for retry behavior."""
    
    def __init__(self, 
                 max_attempts: int = 3,
                 base_delay: float = 1.0,
                 max_delay: float = 60.0,
                 exponential_base: float = 2.0,
                 jitter: bool = True):
        """
        Initialize retry configuration.
        
        Args:
            max_attempts: Maximum number of retry attempts
            base_delay: Base delay in seconds for first retry
            max_delay: Maximum delay in seconds between retries
            exponential_base: Base for exponential backoff calculation
            jitter: Whether to add random jitter to delays
        """
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter


class ErrorHandler:
    """Comprehensive error handling and retry logic for AWS operations."""
    
    # AWS error codes that should trigger retries
    RETRYABLE_ERROR_CODES = {
        'Throttling',
        'ThrottlingException',
        'ProvisionedThroughputExceededException',
        'RequestLimitExceeded',
        'ServiceUnavailable',
        'InternalServerError',
        'InternalFailure',
        'ServiceException',
        'SlowDown',
        'RequestTimeout',
        'RequestTimeoutException'
    }
    
    # AWS error codes that should not be retried
    NON_RETRYABLE_ERROR_CODES = {
        'AccessDenied',
        'AccessDeniedException',
        'InvalidParameterValue',
        'InvalidParameterValueException',
        'ResourceNotFound',
        'ResourceNotFoundException',
        'ValidationException',
        'InvalidRequestException',
        'UnauthorizedOperation',
        'Forbidden',
        'NoSuchBucket',
        'NoSuchKey'
    }
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize the error handler.
        
        Args:
            logger: Optional logger instance for error reporting
        """
        self.logger = logger or logging.getLogger(__name__)
        
    def handle_api_error(self, error: Exception, operation: str, 
                        service: str = 'aws') -> Tuple[bool, Optional[str]]:
        """
        Handle AWS API errors and determine if retry should be attempted.
        
        Args:
            error: The exception that occurred
            operation: Name of the operation that failed
            service: AWS service name
            
        Returns:
            Tuple[bool, Optional[str]]: (should_retry, error_message)
        """
        if isinstance(error, ClientError):
            error_code = error.response['Error']['Code']
            error_message = error.response['Error']['Message']
            
            # Log the error with context
            self.logger.error(
                f"AWS API error in {service}.{operation}: {error_code} - {error_message}",
                extra={
                    'context': {
                        'service': service,
                        'operation': operation,
                        'error_code': error_code,
                        'error_message': error_message,
                        'request_id': error.response.get('ResponseMetadata', {}).get('RequestId')
                    }
                }
            )
            
            # Determine if error is retryable
            should_retry = error_code in self.RETRYABLE_ERROR_CODES
            
            # Convert to appropriate custom exception
            if error_code in ['AccessDenied', 'AccessDeniedException', 'UnauthorizedOperation']:
                raise AWSCredentialsError(
                    f"Access denied for {service}.{operation}: {error_message}",
                    error_code=error_code,
                    context={'service': service, 'operation': operation}
                )
            elif service == 'quicksight':
                raise QuickSightAPIError(
                    f"QuickSight API error: {error_message}",
                    error_code=error_code,
                    context={'operation': operation}
                )
            elif service == 'dynamodb':
                raise DynamoDBError(
                    f"DynamoDB error: {error_message}",
                    error_code=error_code,
                    context={'operation': operation}
                )
            elif service == 's3':
                raise S3Error(
                    f"S3 error: {error_message}",
                    error_code=error_code,
                    context={'operation': operation}
                )
            
            return should_retry, f"{error_code}: {error_message}"
            
        elif isinstance(error, (EndpointConnectionError, ConnectTimeoutError, ReadTimeoutError)):
            # Network-related errors are generally retryable
            error_message = f"Network error in {service}.{operation}: {str(error)}"
            self.logger.warning(error_message)
            return True, error_message
            
        elif isinstance(error, NoCredentialsError):
            # Credentials errors are not retryable
            error_message = "AWS credentials not found or invalid"
            self.logger.error(error_message)
            raise AWSCredentialsError(error_message)
            
        elif isinstance(error, BotoCoreError):
            # Other boto3 errors
            error_message = f"Boto3 error in {service}.{operation}: {str(error)}"
            self.logger.error(error_message)
            return False, error_message
            
        else:
            # Unknown errors
            error_message = f"Unknown error in {service}.{operation}: {str(error)}"
            self.logger.error(error_message, exc_info=True)
            return False, error_message
            
    def handle_network_error(self, error: Exception, operation: str) -> Tuple[bool, str]:
        """
        Handle network-related errors.
        
        Args:
            error: The network exception that occurred
            operation: Name of the operation that failed
            
        Returns:
            Tuple[bool, str]: (should_retry, error_message)
        """
        error_message = f"Network error in {operation}: {str(error)}"
        self.logger.warning(error_message)
        
        # Most network errors are retryable
        return True, error_message
        
    def should_retry(self, error: Exception, attempt: int, max_attempts: int) -> bool:
        """
        Determine if an operation should be retried based on the error and attempt count.
        
        Args:
            error: The exception that occurred
            attempt: Current attempt number (1-based)
            max_attempts: Maximum number of attempts allowed
            
        Returns:
            bool: True if operation should be retried
        """
        if attempt >= max_attempts:
            return False
            
        # Handle specific error types
        if isinstance(error, ClientError):
            error_code = error.response['Error']['Code']
            return error_code in self.RETRYABLE_ERROR_CODES
            
        if isinstance(error, (EndpointConnectionError, ConnectTimeoutError, ReadTimeoutError)):
            return True
            
        if isinstance(error, (AWSCredentialsError, ConfigurationError)):
            return False
            
        # Default to not retrying unknown errors
        return False
        
    def calculate_backoff_delay(self, attempt: int, config: RetryConfig) -> float:
        """
        Calculate the delay before the next retry attempt using exponential backoff.
        
        Args:
            attempt: Current attempt number (1-based)
            config: Retry configuration
            
        Returns:
            float: Delay in seconds
        """
        # Calculate exponential backoff delay
        delay = config.base_delay * (config.exponential_base ** (attempt - 1))
        
        # Cap at maximum delay
        delay = min(delay, config.max_delay)
        
        # Add jitter if enabled
        if config.jitter:
            jitter_range = delay * 0.1  # 10% jitter
            jitter = random.uniform(-jitter_range, jitter_range)
            delay += jitter
            
        return max(0, delay)
        
    def retry_with_backoff(self, 
                          func: Callable,
                          config: Optional[RetryConfig] = None,
                          operation_name: Optional[str] = None,
                          service_name: str = 'aws') -> Callable:
        """
        Decorator to add retry logic with exponential backoff to a function.
        
        Args:
            func: Function to wrap with retry logic
            config: Retry configuration (uses default if None)
            operation_name: Name of the operation for logging
            service_name: AWS service name for error handling
            
        Returns:
            Callable: Wrapped function with retry logic
        """
        if config is None:
            config = RetryConfig()
            
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            
            for attempt in range(1, config.max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                    
                except Exception as error:
                    last_error = error
                    
                    # Log the attempt
                    op_name = operation_name or func.__name__
                    self.logger.warning(
                        f"Attempt {attempt}/{config.max_attempts} failed for {op_name}: {str(error)}",
                        extra={
                            'context': {
                                'operation': op_name,
                                'attempt': attempt,
                                'max_attempts': config.max_attempts,
                                'error_type': type(error).__name__
                            }
                        }
                    )
                    
                    # Check if we should retry
                    if not self.should_retry(error, attempt, config.max_attempts):
                        self.logger.error(f"Non-retryable error in {op_name}, giving up")
                        raise error
                        
                    # If this was the last attempt, don't wait
                    if attempt == config.max_attempts:
                        break
                        
                    # Calculate and apply backoff delay
                    delay = self.calculate_backoff_delay(attempt, config)
                    self.logger.info(
                        f"Retrying {op_name} in {delay:.2f} seconds (attempt {attempt + 1}/{config.max_attempts})"
                    )
                    time.sleep(delay)
                    
            # If we get here, all attempts failed
            self.logger.error(f"All {config.max_attempts} attempts failed for {operation_name or func.__name__}")
            raise last_error
            
        return wrapper
        
    def create_retry_decorator(self, 
                             config: Optional[RetryConfig] = None,
                             operation_name: Optional[str] = None,
                             service_name: str = 'aws'):
        """
        Create a retry decorator with specific configuration.
        
        Args:
            config: Retry configuration
            operation_name: Operation name for logging
            service_name: AWS service name
            
        Returns:
            Callable: Decorator function
        """
        def decorator(func):
            return self.retry_with_backoff(func, config, operation_name, service_name)
        return decorator
        
    def handle_backup_job_error(self, error: Exception, job_type: str, 
                               job_id: Optional[str] = None) -> None:
        """
        Handle errors specific to backup job operations.
        
        Args:
            error: The exception that occurred
            job_type: Type of backup job (e.g., 'asset_bundle_export')
            job_id: Optional job ID for context
        """
        context = {'job_type': job_type}
        if job_id:
            context['job_id'] = job_id
            
        if isinstance(error, ClientError):
            error_code = error.response['Error']['Code']
            error_message = error.response['Error']['Message']
            
            self.logger.error(
                f"Backup job error: {error_code} - {error_message}",
                extra={'context': context}
            )
            
            raise BackupJobError(
                f"Backup job failed: {error_message}",
                error_code=error_code,
                context=context
            )
        else:
            self.logger.error(
                f"Backup job error: {str(error)}",
                extra={'context': context},
                exc_info=True
            )
            
            raise BackupJobError(
                f"Backup job failed: {str(error)}",
                context=context
            )
            
    def get_error_remediation_steps(self, error: Exception) -> List[str]:
        """
        Get suggested remediation steps for common errors.
        
        Args:
            error: The exception that occurred
            
        Returns:
            List[str]: List of suggested remediation steps
        """
        if isinstance(error, AWSCredentialsError):
            return [
                "Check that AWS credentials are properly configured",
                "Verify AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables",
                "Ensure AWS CLI is configured with 'aws configure'",
                "Check IAM role permissions if using EC2 instance profiles",
                "Verify the AWS region is correctly specified"
            ]
            
        elif isinstance(error, QuickSightAPIError):
            if hasattr(error, 'error_code'):
                if error.error_code == 'AccessDeniedException':
                    return [
                        "Verify QuickSight permissions for the AWS account",
                        "Check that the user has QuickSight admin privileges",
                        "Ensure the AWS account ID is correct",
                        "Verify QuickSight is enabled in the specified region"
                    ]
                elif error.error_code == 'ResourceNotFoundException':
                    return [
                        "Check that the specified QuickSight resources exist",
                        "Verify the AWS account ID and region",
                        "Ensure the namespace exists (default: 'default')"
                    ]
                    
        elif isinstance(error, DynamoDBError):
            return [
                "Check DynamoDB permissions (dynamodb:CreateTable, dynamodb:PutItem, etc.)",
                "Verify the DynamoDB table names are valid",
                "Check AWS region configuration",
                "Ensure sufficient DynamoDB capacity or use on-demand billing"
            ]
            
        elif isinstance(error, S3Error):
            if hasattr(error, 'error_code'):
                if error.error_code == 'NoSuchBucket':
                    return [
                        "Create the S3 bucket before running the backup",
                        "Verify the bucket name is correct",
                        "Check that the bucket exists in the specified region"
                    ]
                elif error.error_code == 'AccessDenied':
                    return [
                        "Check S3 permissions (s3:GetObject, s3:PutObject, etc.)",
                        "Verify bucket policy allows the required operations",
                        "Check IAM user/role permissions for S3 access"
                    ]
                    
        elif isinstance(error, ConfigurationError):
            return [
                "Review the configuration file for syntax errors",
                "Validate all required configuration parameters are provided",
                "Check that AWS account ID is a 12-digit number",
                "Verify region names and resource names follow AWS naming conventions"
            ]
            
        # Default remediation steps
        return [
            "Check the error logs for more detailed information",
            "Verify AWS credentials and permissions",
            "Ensure all required AWS services are available in the region",
            "Check network connectivity to AWS services"
        ]