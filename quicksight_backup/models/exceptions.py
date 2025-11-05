"""
Custom exception classes for QuickSight backup operations.
"""

from typing import Optional, Dict, Any


class QuickSightBackupError(Exception):
    """Base exception for QuickSight backup operations."""
    
    def __init__(self, message: str, error_code: Optional[str] = None, 
                 context: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.context = context or {}


class ConfigurationError(QuickSightBackupError):
    """Exception raised for configuration-related errors."""
    pass


class AWSCredentialsError(QuickSightBackupError):
    """Exception raised for AWS credentials-related errors."""
    pass


class QuickSightAPIError(QuickSightBackupError):
    """Exception raised for QuickSight API-related errors."""
    pass


class DynamoDBError(QuickSightBackupError):
    """Exception raised for DynamoDB-related errors."""
    pass


class S3Error(QuickSightBackupError):
    """Exception raised for S3-related errors."""
    pass


class BackupJobError(QuickSightBackupError):
    """Exception raised for backup job execution errors."""
    pass