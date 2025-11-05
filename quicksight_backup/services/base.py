"""
Base interfaces and abstract classes for backup services.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from quicksight_backup.models.backup_result import BackupResult
from quicksight_backup.models.config import BackupConfig


class BaseBackupService(ABC):
    """Abstract base class for all backup services."""
    
    def __init__(self, config: BackupConfig):
        self.config = config
        self._clients: Dict[str, Any] = {}
    
    @abstractmethod
    def backup(self) -> BackupResult:
        """Execute the backup operation for this service."""
        pass
    
    @abstractmethod
    def validate_prerequisites(self) -> bool:
        """Validate that all prerequisites for backup are met."""
        pass
    
    def get_client(self, service_name: str) -> Any:
        """Get or create an AWS service client."""
        if service_name not in self._clients:
            self._clients[service_name] = self._create_client(service_name)
        return self._clients[service_name]
    
    @abstractmethod
    def _create_client(self, service_name: str) -> Any:
        """Create an AWS service client."""
        pass


class BaseConfigurationManager(ABC):
    """Abstract base class for configuration management."""
    
    @abstractmethod
    def load_config(self, config_path: str) -> BackupConfig:
        """Load configuration from file."""
        pass
    
    @abstractmethod
    def validate_config(self, config: BackupConfig) -> bool:
        """Validate configuration settings."""
        pass
    
    @abstractmethod
    def get_aws_credentials(self) -> Dict[str, Optional[str]]:
        """Get AWS credentials from various sources."""
        pass


class BaseErrorHandler(ABC):
    """Abstract base class for error handling."""
    
    @abstractmethod
    def handle_api_error(self, error: Exception, operation: str) -> bool:
        """Handle AWS API errors with retry logic."""
        pass
    
    @abstractmethod
    def should_retry(self, error: Exception, attempt: int) -> bool:
        """Determine if an operation should be retried."""
        pass
    
    @abstractmethod
    def calculate_backoff_delay(self, attempt: int) -> float:
        """Calculate backoff delay for retry attempts."""
        pass