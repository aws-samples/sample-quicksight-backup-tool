"""
Data models for backup operation results.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Any
from enum import Enum


class BackupStatus(Enum):
    """Status enumeration for backup operations."""
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"
    IN_PROGRESS = "in_progress"


@dataclass
class BackupResult:
    """Result of a backup operation for a specific resource type."""
    
    resource_type: str
    success: bool
    items_processed: int
    items_failed: int
    error_messages: List[str] = field(default_factory=list)
    execution_time: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)
    status: BackupStatus = BackupStatus.SUCCESS
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def add_error(self, error_message: str) -> None:
        """Add an error message to the result."""
        self.error_messages.append(error_message)
        self.items_failed += 1
        if self.items_processed > 0:
            self.status = BackupStatus.PARTIAL
        else:
            self.status = BackupStatus.FAILED
            self.success = False


@dataclass
class BackupReport:
    """Comprehensive report of all backup operations."""
    
    total_resources: int
    successful_resources: int
    failed_resources: int
    partial_resources: int
    total_execution_time: float
    start_time: datetime
    end_time: datetime
    results: List[BackupResult] = field(default_factory=list)
    
    @property
    def success_rate(self) -> float:
        """Calculate the overall success rate."""
        if self.total_resources == 0:
            return 0.0
        return (self.successful_resources / self.total_resources) * 100
    
    def add_result(self, result: BackupResult) -> None:
        """Add a backup result to the report."""
        self.results.append(result)
        if result.status == BackupStatus.SUCCESS:
            self.successful_resources += 1
        elif result.status == BackupStatus.FAILED:
            self.failed_resources += 1
        elif result.status == BackupStatus.PARTIAL:
            self.partial_resources += 1