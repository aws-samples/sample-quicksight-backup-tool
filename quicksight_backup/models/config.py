"""
Configuration data models for QuickSight backup operations.
"""

from dataclasses import dataclass
from typing import Optional, List
import re
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from .exceptions import ConfigurationError


@dataclass
class BackupConfig:
    """Configuration settings for QuickSight backup operations."""
    
    # No default values
    s3_bucket_name: str

    # AWS Configuration
    aws_region: str
    aws_account_id: str
    identity_region: Optional[str] = None  # Optional separate region for user/group operations
    
    # DynamoDB Configuration
    dynamodb_users_table: str = "quicksight-users-backup"
    dynamodb_groups_table: str = "quicksight-groups-backup"
    users_group_table_name: str = "quicksight-users-groups-backup"
    
    # S3 Configuration    
    s3_prefix_format: str = "YYYY/MM/DD"
    s3_prefix: str = "quicksight-backups"  # Custom S3 prefix for asset bundles
    
    # Backup Options
    include_dependencies: bool = True
    include_permissions: bool = True
    include_tags: bool = True
    export_format: str = "QUICKSIGHT_JSON"
    max_assets_per_bundle: int = 50  # Maximum assets per bundle (1-100)
    
    # Logging Configuration
    logging_level: str = "INFO"
    logging_file_path: str = "./logs/backup.log"
    
    # Optional AWS Credentials
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    
    def validate(self) -> List[str]:
        """
        Validate configuration settings and return list of validation errors.
        
        Returns:
            List[str]: List of validation error messages. Empty if valid.
        """
        errors = []
        
        # Validate AWS settings
        errors.extend(self._validate_aws_settings())
        
        # Validate DynamoDB settings
        errors.extend(self._validate_dynamodb_settings())
        
        # Validate S3 settings
        errors.extend(self._validate_s3_settings())
        
        # Validate backup options
        errors.extend(self._validate_backup_options())
        
        # Validate logging settings
        errors.extend(self._validate_logging_settings())
        
        return errors
    
    def _validate_aws_settings(self) -> List[str]:
        """Validate AWS configuration settings."""
        errors = []
        
        # Validate AWS region
        if not self.aws_region:
            errors.append("AWS region is required")
        elif not re.match(r'^[a-z0-9-]+$', self.aws_region):
            errors.append("AWS region format is invalid")
        
        # Validate identity region if provided
        if self.identity_region and not re.match(r'^[a-z0-9-]+$', self.identity_region):
            errors.append("Identity region format is invalid")
        
        # Validate AWS account ID
        if not self.aws_account_id:
            errors.append("AWS account ID is required")
        elif not re.match(r'^\d{12}$', self.aws_account_id):
            errors.append("AWS account ID must be a 12-digit number")
        
        return errors
    
    def _validate_dynamodb_settings(self) -> List[str]:
        """Validate DynamoDB configuration settings."""
        errors = []
        
        # Validate table names
        if not self.dynamodb_users_table:
            errors.append("DynamoDB users table name is required")
        elif not self._is_valid_dynamodb_table_name(self.dynamodb_users_table):
            errors.append("DynamoDB users table name is invalid")
        
        if not self.dynamodb_groups_table:
            errors.append("DynamoDB groups table name is required")
        elif not self._is_valid_dynamodb_table_name(self.dynamodb_groups_table):
            errors.append("DynamoDB groups table name is invalid")
        
        if not self.users_group_table_name:
            errors.append("DynamoDB users-group table name is required")
        elif not self._is_valid_dynamodb_table_name(self.users_group_table_name):
            errors.append("DynamoDB users-group table name is invalid")
        
        # Check for duplicate table names
        table_names = [self.dynamodb_users_table, self.dynamodb_groups_table, self.users_group_table_name]
        unique_names = set(name for name in table_names if name)
        if len(unique_names) != len([name for name in table_names if name]):
            errors.append("All DynamoDB table names must be different")
        
        return errors
    
    def _validate_s3_settings(self) -> List[str]:
        """Validate S3 configuration settings."""
        errors = []
        
        # Validate S3 bucket name
        if not self.s3_bucket_name:
            errors.append("S3 bucket name is required")
        elif not self._is_valid_s3_bucket_name(self.s3_bucket_name):
            errors.append("S3 bucket name is invalid")
        
        # Validate S3 prefix format
        if not self.s3_prefix_format:
            errors.append("S3 prefix format is required")
        elif self.s3_prefix_format not in ["YYYY/MM/DD", "YYYY-MM-DD", "YYYYMMDD"]:
            errors.append("S3 prefix format must be one of: YYYY/MM/DD, YYYY-MM-DD, YYYYMMDD")
        
        # Validate S3 prefix
        if not self.s3_prefix:
            errors.append("S3 prefix is required")
        elif not self._is_valid_s3_prefix(self.s3_prefix):
            errors.append("S3 prefix contains invalid characters")
        
        return errors
    
    def _validate_backup_options(self) -> List[str]:
        """Validate backup option settings."""
        errors = []
        
        # Validate export format
        if self.export_format not in ["QUICKSIGHT_JSON", "CLOUDFORMATION_JSON"]:
            errors.append("Export format must be QUICKSIGHT_JSON or CLOUDFORMATION_JSON")
        
        # Validate max_assets_per_bundle
        if not isinstance(self.max_assets_per_bundle, int):
            errors.append("max_assets_per_bundle must be an integer")
        elif not (1 <= self.max_assets_per_bundle <= 100):
            errors.append("max_assets_per_bundle must be between 1 and 100 inclusive")
        
        return errors
    
    def _validate_logging_settings(self) -> List[str]:
        """Validate logging configuration settings."""
        errors = []
        
        # Validate logging level
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.logging_level not in valid_levels:
            errors.append(f"Logging level must be one of: {', '.join(valid_levels)}")
        
        # Validate logging file path
        if not self.logging_file_path:
            errors.append("Logging file path is required")
        
        return errors
    
    def _is_valid_dynamodb_table_name(self, table_name: str) -> bool:
        """
        Validate DynamoDB table name according to AWS naming rules.
        
        Args:
            table_name: The table name to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        if not table_name:
            return False
        
        # DynamoDB table name rules:
        # - Length: 3-255 characters
        # - Characters: a-z, A-Z, 0-9, underscore, hyphen, period
        # - Cannot start with a number
        if len(table_name) < 3 or len(table_name) > 255:
            return False
        
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.-]*$', table_name):
            return False
        
        return True
    
    def _is_valid_s3_bucket_name(self, bucket_name: str) -> bool:
        """
        Validate S3 bucket name according to AWS naming rules.
        
        Args:
            bucket_name: The bucket name to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        if not bucket_name:
            return False
        
        # S3 bucket name rules:
        # - Length: 3-63 characters
        # - Lowercase letters, numbers, hyphens, periods
        # - Must start and end with letter or number
        # - Cannot have consecutive periods or hyphens
        # - Cannot be formatted as IP address
        if len(bucket_name) < 3 or len(bucket_name) > 63:
            return False
        
        if not re.match(r'^[a-z0-9][a-z0-9.-]*[a-z0-9]$', bucket_name):
            return False
        
        # Check for consecutive periods or hyphens
        if '..' in bucket_name or '--' in bucket_name or '.-' in bucket_name or '-.' in bucket_name:
            return False
        
        # Check if it looks like an IP address
        if re.match(r'^\d+\.\d+\.\d+\.\d+$', bucket_name):
            return False
        
        return True
    
    def _is_valid_s3_prefix(self, prefix: str) -> bool:
        """
        Validate S3 prefix according to AWS S3 key naming rules.
        
        Args:
            prefix: The S3 prefix to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        if not prefix:
            return False
        
        # S3 key prefix rules:
        # - Cannot start or end with slash
        # - Cannot contain consecutive slashes
        # - Should use safe characters (alphanumeric, hyphens, underscores, periods)
        # - Avoid special characters that might cause issues
        
        # Check for leading/trailing slashes
        if prefix.startswith('/') or prefix.endswith('/'):
            return False
        
        # Check for consecutive slashes
        if '//' in prefix:
            return False
        
        # Check for valid characters (allow alphanumeric, hyphens, underscores, periods, slashes)
        if not re.match(r'^[a-zA-Z0-9._/-]+$', prefix):
            return False
        
        return True
    
    def validate_max_assets_per_bundle(self) -> bool:
        """
        Validate that max_assets_per_bundle is within acceptable range.
        
        Returns:
            bool: True if valid, False otherwise
        """
        return isinstance(self.max_assets_per_bundle, int) and 1 <= self.max_assets_per_bundle <= 100
    
    def validate_aws_connectivity(self) -> List[str]:
        """
        Validate AWS connectivity and permissions.
        
        Returns:
            List[str]: List of connectivity/permission error messages. Empty if valid.
        """
        errors = []
        
        try:
            # Create session with provided credentials or default credential chain
            session_kwargs = {'region_name': self.aws_region}
            if self.aws_access_key_id and self.aws_secret_access_key:
                session_kwargs.update({
                    'aws_access_key_id': self.aws_access_key_id,
                    'aws_secret_access_key': self.aws_secret_access_key
                })
                if self.aws_session_token:
                    session_kwargs['aws_session_token'] = self.aws_session_token
            
            session = boto3.Session(**session_kwargs)
            
            # Test QuickSight access (use identity_region if configured, otherwise aws_region)
            try:
                identity_region = self.identity_region or self.aws_region
                identity_session_kwargs = session_kwargs.copy()
                identity_session_kwargs['region_name'] = identity_region
                identity_session = boto3.Session(**identity_session_kwargs)
                
                quicksight = identity_session.client('quicksight')
                quicksight.list_users(AwsAccountId=self.aws_account_id, Namespace='default', MaxResults=1)
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == 'AccessDeniedException':
                    errors.append("Insufficient permissions for QuickSight operations")
                elif error_code == 'InvalidParameterValueException':
                    errors.append("Invalid AWS account ID for QuickSight")
                else:
                    errors.append(f"QuickSight access error: {error_code}")
            except Exception as e:
                errors.append(f"QuickSight connectivity error: {str(e)}")
            
            # Test DynamoDB access
            try:
                dynamodb = session.client('dynamodb')
                dynamodb.list_tables(Limit=1)
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == 'AccessDeniedException':
                    errors.append("Insufficient permissions for DynamoDB operations")
                else:
                    errors.append(f"DynamoDB access error: {error_code}")
            except Exception as e:
                errors.append(f"DynamoDB connectivity error: {str(e)}")
            
            # Test S3 access
            try:
                s3 = session.client('s3')
                s3.head_bucket(Bucket=self.s3_bucket_name)
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == 'NoSuchBucket':
                    errors.append(f"S3 bucket '{self.s3_bucket_name}' does not exist")
                elif error_code == 'AccessDenied':
                    errors.append(f"Insufficient permissions for S3 bucket '{self.s3_bucket_name}'")
                else:
                    errors.append(f"S3 access error: {error_code}")
            except Exception as e:
                errors.append(f"S3 connectivity error: {str(e)}")
                
        except NoCredentialsError:
            errors.append("AWS credentials not found. Please configure credentials.")
        except Exception as e:
            errors.append(f"AWS session creation error: {str(e)}")
        
        return errors