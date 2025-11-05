"""
Configuration management for QuickSight backup operations.
"""

import json
import yaml
import os
from pathlib import Path
from typing import Dict, Any, Optional
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from ..models.config import BackupConfig
from ..models.exceptions import ConfigurationError, AWSCredentialsError


class ConfigurationManager:
    """Manages configuration loading, validation, and AWS client initialization."""
    
    def __init__(self):
        self._config: Optional[BackupConfig] = None
        self._aws_session: Optional[boto3.Session] = None
    
    def load_config(self, config_path: str) -> BackupConfig:
        """
        Load configuration from YAML or JSON file.
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            BackupConfig: Loaded and validated configuration
            
        Raises:
            ConfigurationError: If configuration is invalid or cannot be loaded
        """
        try:
            config_file = Path(config_path)
            
            if not config_file.exists():
                raise ConfigurationError(f"Configuration file not found: {config_path}")
            
            # Load configuration based on file extension
            with open(config_file, 'r', encoding='utf-8') as f:
                if config_file.suffix.lower() in ['.yaml', '.yml']:
                    config_data = yaml.safe_load(f)
                elif config_file.suffix.lower() == '.json':
                    config_data = json.load(f)
                else:
                    raise ConfigurationError(
                        f"Unsupported configuration file format: {config_file.suffix}. "
                        "Supported formats: .yaml, .yml, .json"
                    )
            
            # Convert nested dict to flat structure for BackupConfig
            flat_config = self._flatten_config(config_data)
            
            # Create BackupConfig instance
            self._config = BackupConfig(**flat_config)
            
            # Validate configuration
            validation_errors = self._config.validate()
            if validation_errors:
                raise ConfigurationError(
                    f"Configuration validation failed:\n" + 
                    "\n".join(f"- {error}" for error in validation_errors)
                )
            
            return self._config
            
        except yaml.YAMLError as e:
            raise ConfigurationError(f"YAML parsing error: {str(e)}")
        except json.JSONDecodeError as e:
            raise ConfigurationError(f"JSON parsing error: {str(e)}")
        except TypeError as e:
            raise ConfigurationError(f"Configuration structure error: {str(e)}")
        except Exception as e:
            raise ConfigurationError(f"Failed to load configuration: {str(e)}")
    
    def _flatten_config(self, config_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Flatten nested configuration dictionary to match BackupConfig fields.
        
        Args:
            config_data: Nested configuration dictionary
            
        Returns:
            Dict[str, Any]: Flattened configuration dictionary
        """
        flat_config = {}
        
        # AWS settings
        aws_config = config_data.get('aws', {})
        flat_config['aws_region'] = aws_config.get('region')
        flat_config['aws_account_id'] = aws_config.get('account_id')
        flat_config['identity_region'] = aws_config.get('identity_region')
        flat_config['aws_access_key_id'] = aws_config.get('access_key_id')
        flat_config['aws_secret_access_key'] = aws_config.get('secret_access_key')
        flat_config['aws_session_token'] = aws_config.get('session_token')
        
        # DynamoDB settings
        dynamodb_config = config_data.get('dynamodb', {})
        flat_config['dynamodb_users_table'] = dynamodb_config.get('users_table_name')
        flat_config['dynamodb_groups_table'] = dynamodb_config.get('groups_table_name')
        flat_config['users_group_table_name'] = dynamodb_config.get('users_group_table_name')
        
        # S3 settings
        s3_config = config_data.get('s3', {})
        flat_config['s3_bucket_name'] = s3_config.get('bucket_name')
        flat_config['s3_prefix_format'] = s3_config.get('prefix_format', 'YYYY/MM/DD')
        
        # Backup options
        backup_config = config_data.get('backup', {})
        flat_config['include_dependencies'] = backup_config.get('include_dependencies', True)
        flat_config['include_permissions'] = backup_config.get('include_permissions', True)
        flat_config['include_tags'] = backup_config.get('include_tags', True)
        flat_config['export_format'] = backup_config.get('export_format', 'QUICKSIGHT_JSON')
        flat_config['max_assets_per_bundle'] = backup_config.get('max_assets_per_bundle', 50)
        
        # Logging settings
        logging_config = config_data.get('logging', {})
        flat_config['logging_level'] = logging_config.get('level', 'INFO')
        flat_config['logging_file_path'] = logging_config.get('file_path', './logs/backup.log')
        
        # Remove None values
        return {k: v for k, v in flat_config.items() if v is not None}
    
    def validate_config(self, config: BackupConfig) -> bool:
        """
        Validate configuration settings.
        
        Args:
            config: Configuration to validate
            
        Returns:
            bool: True if valid
            
        Raises:
            ConfigurationError: If configuration is invalid
        """
        validation_errors = config.validate()
        if validation_errors:
            raise ConfigurationError(
                f"Configuration validation failed:\n" + 
                "\n".join(f"- {error}" for error in validation_errors)
            )
        return True
    
    def validate_aws_connectivity(self, config: Optional[BackupConfig] = None) -> bool:
        """
        Validate AWS connectivity and permissions.
        
        Args:
            config: Configuration to validate (uses loaded config if None)
            
        Returns:
            bool: True if connectivity is valid
            
        Raises:
            ConfigurationError: If configuration is not loaded
            AWSCredentialsError: If AWS connectivity fails
        """
        if config is None:
            if self._config is None:
                raise ConfigurationError("No configuration loaded")
            config = self._config
        
        connectivity_errors = config.validate_aws_connectivity()
        if connectivity_errors:
            raise AWSCredentialsError(
                f"AWS connectivity validation failed:\n" + 
                "\n".join(f"- {error}" for error in connectivity_errors)
            )
        return True
    
    def get_aws_credentials(self, config: Optional[BackupConfig] = None) -> Dict[str, Any]:
        """
        Get AWS credentials dictionary for boto3 client initialization.
        
        Args:
            config: Configuration to use (uses loaded config if None)
            
        Returns:
            Dict[str, Any]: AWS credentials and region configuration
            
        Raises:
            ConfigurationError: If configuration is not loaded
        """
        if config is None:
            if self._config is None:
                raise ConfigurationError("No configuration loaded")
            config = self._config
        
        credentials = {
            'region_name': config.aws_region
        }
        
        # Add explicit credentials if provided
        if config.aws_access_key_id and config.aws_secret_access_key:
            credentials['aws_access_key_id'] = config.aws_access_key_id
            credentials['aws_secret_access_key'] = config.aws_secret_access_key
            
            if config.aws_session_token:
                credentials['aws_session_token'] = config.aws_session_token
        
        return credentials
    
    def create_aws_session(self, config: Optional[BackupConfig] = None) -> boto3.Session:
        """
        Create AWS session with configured credentials.
        
        Args:
            config: Configuration to use (uses loaded config if None)
            
        Returns:
            boto3.Session: Configured AWS session
            
        Raises:
            ConfigurationError: If configuration is not loaded
            AWSCredentialsError: If session creation fails
        """
        if config is None:
            if self._config is None:
                raise ConfigurationError("No configuration loaded")
            config = self._config
        
        try:
            credentials = self.get_aws_credentials(config)
            self._aws_session = boto3.Session(**credentials)
            return self._aws_session
            
        except NoCredentialsError as e:
            raise AWSCredentialsError(f"AWS credentials not found: {str(e)}")
        except Exception as e:
            raise AWSCredentialsError(f"Failed to create AWS session: {str(e)}")
    
    def get_quicksight_client(self, config: Optional[BackupConfig] = None):
        """
        Get configured QuickSight client.
        
        Args:
            config: Configuration to use (uses loaded config if None)
            
        Returns:
            boto3.client: QuickSight client
        """
        if config is None:
            if self._config is None:
                raise ConfigurationError("No configuration loaded")
            config = self._config
        
        # Use identity_region for QuickSight if configured, otherwise use aws_region
        identity_region = config.identity_region or config.aws_region
        
        credentials = self.get_aws_credentials(config)
        credentials['region_name'] = identity_region
        
        try:
            session = boto3.Session(**credentials)
            return session.client('quicksight')
        except Exception as e:
            raise AWSCredentialsError(f"Failed to create QuickSight client: {str(e)}")
    
    def get_dynamodb_client(self, config: Optional[BackupConfig] = None):
        """
        Get configured DynamoDB client.
        
        Args:
            config: Configuration to use (uses loaded config if None)
            
        Returns:
            boto3.client: DynamoDB client
        """
        session = self.create_aws_session(config)
        return session.client('dynamodb')
    
    def get_s3_client(self, config: Optional[BackupConfig] = None):
        """
        Get configured S3 client.
        
        Args:
            config: Configuration to use (uses loaded config if None)
            
        Returns:
            boto3.client: S3 client
        """
        session = self.create_aws_session(config)
        return session.client('s3')
    
    def create_sample_config(self, output_path: str) -> None:
        """
        Create a sample configuration file.
        
        Args:
            output_path: Path where to create the sample configuration
        """
        sample_config = {
            'aws': {
                'region': 'us-east-1',
                'account_id': '123456789012',
                'identity_region': 'us-east-1'  # Optional: separate region for user/group operations
            },
            'dynamodb': {
                'users_table_name': 'quicksight-users-backup',
                'groups_table_name': 'quicksight-groups-backup',
                'users_group_table_name': 'quicksight-users-groups-backup'
            },
            's3': {
                'bucket_name': 'my-quicksight-backups',
                'prefix_format': 'YYYY/MM/DD'
            },
            'backup': {
                'include_dependencies': True,
                'include_permissions': True,
                'include_tags': True,
                'export_format': 'QUICKSIGHT_JSON',
                'max_assets_per_bundle': 50
            },
            'logging': {
                'level': 'INFO',
                'file_path': './logs/backup.log'
            }
        }
        
        output_file = Path(output_path)
        
        # Create directory if it doesn't exist
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Write YAML format
        with open(output_file, 'w', encoding='utf-8') as f:
            yaml.dump(sample_config, f, default_flow_style=False, indent=2)
    
    @property
    def config(self) -> Optional[BackupConfig]:
        """Get the currently loaded configuration."""
        return self._config
    
    @property
    def aws_session(self) -> Optional[boto3.Session]:
        """Get the current AWS session."""
        return self._aws_session