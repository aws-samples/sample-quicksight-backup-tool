"""
QuickSight Backup Tool

A comprehensive backup solution for Amazon QuickSight resources.
"""

from .config import ConfigurationManager
from .orchestrator import QuickSightBackupOrchestrator
from .models import (
    BackupConfig,
    BackupResult,
    BackupReport,
    BackupStatus,
    AssetInventory,
    QuickSightBackupError,
    ConfigurationError,
    AWSCredentialsError,
    QuickSightAPIError,
    DynamoDBError,
    S3Error,
    BackupJobError
)

__version__ = "1.0.0"
__author__ = "QuickSight Backup Tool"

__all__ = [
    "ConfigurationManager",
    "QuickSightBackupOrchestrator",
    "BackupConfig",
    "BackupResult",
    "BackupReport",
    "BackupStatus",
    "AssetInventory",
    "QuickSightBackupError",
    "ConfigurationError",
    "AWSCredentialsError",
    "QuickSightAPIError",
    "DynamoDBError",
    "S3Error",
    "BackupJobError"
]