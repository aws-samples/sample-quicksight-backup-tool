"""
Service for backing up QuickSight users and groups.
"""

import logging
import time
from typing import List, Dict, Any, Optional
from botocore.exceptions import ClientError, BotoCoreError
import boto3

from quicksight_backup.models.config import BackupConfig
from quicksight_backup.models.backup_result import BackupResult, BackupStatus
from quicksight_backup.models.user_group import (
    User, 
    Group, 
    UserGroupMembership,
    transform_users_from_api_response,
    transform_groups_from_api_response,
    create_user_group_memberships
)
from quicksight_backup.models.exceptions import (
    QuickSightAPIError,
    DynamoDBError,
    AWSCredentialsError
)
from quicksight_backup.services.base import BaseBackupService


class UserGroupBackupService(BaseBackupService):
    """Service for backing up QuickSight users and groups to DynamoDB."""
    
    def __init__(self, config: BackupConfig):
        super().__init__(config)
        self.logger = logging.getLogger(__name__)
    
    def get_effective_region(self) -> str:
        """
        Get the effective region for user/group operations.
        
        Returns:
            str: identity_region if configured, otherwise aws_region
        """
        return self.config.identity_region or self.config.aws_region
    
    def generate_date_prefix(self) -> str:
        """
        Generate date prefix for table names using the configured prefix_format.
        Converts all formats to table-name-safe format (YYYY-MM-DD).
        
        Returns:
            str: Date prefix string in YYYY-MM-DD format for table names
        """
        from datetime import datetime
        
        now = datetime.now()
        
        # Always return YYYY-MM-DD format for table names (DynamoDB table name safe)
        date_prefix = f"{now.year:04d}-{now.month:02d}-{now.day:02d}"
        
        # Log if using a different format than configured for informational purposes
        if self.config.s3_prefix_format not in ["YYYY/MM/DD", "YYYY-MM-DD", "YYYYMMDD"]:
            self.logger.warning(f"Invalid prefix format '{self.config.s3_prefix_format}', using YYYY-MM-DD for table names")
        
        return date_prefix
        
    def backup(self) -> BackupResult:
        """
        Execute backup of both users and groups.
        
        Returns:
            BackupResult: Combined result of user and group backup operations
        """
        start_time = time.time()
        result = BackupResult(
            resource_type="users_and_groups",
            success=True,
            items_processed=0,
            items_failed=0
        )
        
        try:
            # Backup users
            user_result = self.backup_users()
            result.items_processed += user_result.items_processed
            result.items_failed += user_result.items_failed
            result.error_messages.extend(user_result.error_messages)
            
            # Backup groups
            group_result = self.backup_groups()
            result.items_processed += group_result.items_processed
            result.items_failed += group_result.items_failed
            result.error_messages.extend(group_result.error_messages)
            
            # Backup user-group memberships
            membership_result = self.backup_user_group_memberships()
            result.items_processed += membership_result.items_processed
            result.items_failed += membership_result.items_failed
            result.error_messages.extend(membership_result.error_messages)
            
            # Determine overall success
            if result.items_failed > 0:
                if result.items_processed > result.items_failed:
                    result.status = BackupStatus.PARTIAL
                else:
                    result.status = BackupStatus.FAILED
                    result.success = False
            
        except Exception as e:
            self.logger.error(f"Unexpected error during user/group backup: {str(e)}")
            result.add_error(f"Unexpected error: {str(e)}")
            
        result.execution_time = time.time() - start_time
        return result
    
    def backup_users(self) -> BackupResult:
        """
        Backup QuickSight users to DynamoDB.
        
        Returns:
            BackupResult: Result of user backup operation
        """
        start_time = time.time()
        result = BackupResult(
            resource_type="users",
            success=True,
            items_processed=0,
            items_failed=0
        )
        
        try:
            self.logger.info("Starting user backup operation")
            
            # Get all users from QuickSight
            users_data = self.get_user_list()
            self.logger.info(f"Retrieved {len(users_data)} users from QuickSight")
            
            # Transform to User objects
            users = transform_users_from_api_response(users_data)
            
            # Store users to DynamoDB
            success = self.store_users_to_dynamodb(users)
            
            if success:
                result.items_processed = len(users)
                self.logger.info(f"Successfully backed up {len(users)} users to DynamoDB")
            else:
                result.add_error("Failed to store users to DynamoDB")
                
        except QuickSightAPIError as e:
            self.logger.error(f"QuickSight API error during user backup: {str(e)}")
            result.add_error(f"QuickSight API error: {str(e)}")
        except DynamoDBError as e:
            self.logger.error(f"DynamoDB error during user backup: {str(e)}")
            result.add_error(f"DynamoDB error: {str(e)}")
        except Exception as e:
            self.logger.error(f"Unexpected error during user backup: {str(e)}")
            result.add_error(f"Unexpected error: {str(e)}")
            
        result.execution_time = time.time() - start_time
        return result
    
    def backup_groups(self) -> BackupResult:
        """
        Backup QuickSight groups to DynamoDB.
        
        Returns:
            BackupResult: Result of group backup operation
        """
        start_time = time.time()
        result = BackupResult(
            resource_type="groups",
            success=True,
            items_processed=0,
            items_failed=0
        )
        
        try:
            self.logger.info("Starting group backup operation")
            
            # Get all groups from QuickSight
            groups_data = self.get_group_list()
            self.logger.info(f"Retrieved {len(groups_data)} groups from QuickSight")
            
            # Get group memberships
            group_members = self._get_group_memberships(groups_data)
            
            # Transform to Group objects
            groups = transform_groups_from_api_response(groups_data, group_members)
            
            # Store groups to DynamoDB
            success = self.store_groups_to_dynamodb(groups)
            
            if success:
                result.items_processed = len(groups)
                self.logger.info(f"Successfully backed up {len(groups)} groups to DynamoDB")
            else:
                result.add_error("Failed to store groups to DynamoDB")
                
        except QuickSightAPIError as e:
            self.logger.error(f"QuickSight API error during group backup: {str(e)}")
            result.add_error(f"QuickSight API error: {str(e)}")
        except DynamoDBError as e:
            self.logger.error(f"DynamoDB error during group backup: {str(e)}")
            result.add_error(f"DynamoDB error: {str(e)}")
        except Exception as e:
            self.logger.error(f"Unexpected error during group backup: {str(e)}")
            result.add_error(f"Unexpected error: {str(e)}")
            
        result.execution_time = time.time() - start_time
        return result
    
    def backup_user_group_memberships(self) -> BackupResult:
        """
        Backup QuickSight user-group memberships to DynamoDB.
        
        Returns:
            BackupResult: Result of user-group membership backup operation
        """
        start_time = time.time()
        result = BackupResult(
            resource_type="user_group_memberships",
            success=True,
            items_processed=0,
            items_failed=0
        )
        
        try:
            self.logger.info("Starting user-group membership backup operation")
            
            # Get all users and groups from QuickSight
            users_data = self.get_user_list()
            groups_data = self.get_group_list()
            
            # Get group memberships
            group_members = self._get_group_memberships(groups_data)
            
            # Transform to User and Group objects
            users = transform_users_from_api_response(users_data)
            groups = transform_groups_from_api_response(groups_data, group_members)
            
            # Create membership relationships
            memberships = create_user_group_memberships(users, groups)
            
            self.logger.info(f"Created {len(memberships)} user-group membership relationships")
            
            # Store memberships to DynamoDB
            success = self.store_user_group_memberships_to_dynamodb(memberships)
            
            if success:
                result.items_processed = len(memberships)
                self.logger.info(f"Successfully backed up {len(memberships)} user-group memberships to DynamoDB")
            else:
                result.add_error("Failed to store user-group memberships to DynamoDB")
                
        except QuickSightAPIError as e:
            self.logger.error(f"QuickSight API error during user-group membership backup: {str(e)}")
            result.add_error(f"QuickSight API error: {str(e)}")
        except DynamoDBError as e:
            self.logger.error(f"DynamoDB error during user-group membership backup: {str(e)}")
            result.add_error(f"DynamoDB error: {str(e)}")
        except Exception as e:
            self.logger.error(f"Unexpected error during user-group membership backup: {str(e)}")
            result.add_error(f"Unexpected error: {str(e)}")
            
        result.execution_time = time.time() - start_time
        return result
    
    def get_user_list(self) -> List[Dict[str, Any]]:
        """
        Retrieve all QuickSight users with pagination handling.
        
        Returns:
            List[Dict[str, Any]]: List of user dictionaries from QuickSight API
            
        Raises:
            QuickSightAPIError: If API call fails
        """
        try:
            quicksight = self.get_client('quicksight-admin')
            users = []
            next_token = None
            
            while True:
                # Prepare API call parameters
                params = {
                    'AwsAccountId': self.config.aws_account_id,
                    'Namespace': 'default',
                    'MaxResults': 100  # Maximum allowed by API
                }
                
                if next_token:
                    params['NextToken'] = next_token
                
                # Make API call
                response = quicksight.list_users(**params)
                
                # Add users from this page
                users.extend(response.get('UserList', []))
                
                # Check for more pages
                next_token = response.get('NextToken')
                if not next_token:
                    break
                    
                self.logger.debug(f"Retrieved {len(users)} users so far, continuing pagination")
            
            return users
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            raise QuickSightAPIError(f"Failed to list users: {error_code} - {error_message}")
        except BotoCoreError as e:
            raise QuickSightAPIError(f"AWS connection error while listing users: {str(e)}")
    
    def get_group_list(self) -> List[Dict[str, Any]]:
        """
        Retrieve all QuickSight groups with pagination handling.
        
        Returns:
            List[Dict[str, Any]]: List of group dictionaries from QuickSight API
            
        Raises:
            QuickSightAPIError: If API call fails
        """
        try:
            quicksight = self.get_client('quicksight-admin')
            groups = []
            next_token = None
            
            while True:
                # Prepare API call parameters
                params = {
                    'AwsAccountId': self.config.aws_account_id,
                    'Namespace': 'default',
                    'MaxResults': 100  # Maximum allowed by API
                }
                
                if next_token:
                    params['NextToken'] = next_token
                
                # Make API call
                response = quicksight.list_groups(**params)
                
                # Add groups from this page
                groups.extend(response.get('GroupList', []))
                
                # Check for more pages
                next_token = response.get('NextToken')
                if not next_token:
                    break
                    
                self.logger.debug(f"Retrieved {len(groups)} groups so far, continuing pagination")
            
            return groups
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            raise QuickSightAPIError(f"Failed to list groups: {error_code} - {error_message}")
        except BotoCoreError as e:
            raise QuickSightAPIError(f"AWS connection error while listing groups: {str(e)}")
    
    def _get_group_memberships(self, groups_data: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """
        Get membership information for all groups.
        
        Args:
            groups_data: List of group dictionaries from QuickSight API
            
        Returns:
            Dict[str, List[str]]: Dictionary mapping group names to member lists
        """
        group_members = {}
        quicksight = self.get_client('quicksight-admin')
        
        for group_data in groups_data:
            group_name = group_data.get('GroupName')
            if not group_name:
                continue
                
            try:
                members = []
                next_token = None
                
                while True:
                    # Prepare API call parameters
                    params = {
                        'GroupName': group_name,
                        'AwsAccountId': self.config.aws_account_id,
                        'Namespace': 'default',
                        'MaxResults': 100
                    }
                    
                    if next_token:
                        params['NextToken'] = next_token
                    
                    # Make API call
                    response = quicksight.list_group_memberships(**params)
                    
                    # Add members from this page
                    for member in response.get('GroupMemberList', []):
                        member_name = member.get('MemberName')
                        if member_name:
                            members.append(member_name)
                    
                    # Check for more pages
                    next_token = response.get('NextToken')
                    if not next_token:
                        break
                
                group_members[group_name] = members
                self.logger.debug(f"Retrieved {len(members)} members for group '{group_name}'")
                
            except ClientError as e:
                self.logger.warning(f"Failed to get members for group '{group_name}': {e}")
                group_members[group_name] = []  # Continue with empty member list
        
        return group_members  
  
    def store_users_to_dynamodb(self, users: List[User]) -> bool:
        """
        Store users to DynamoDB table with batch writing.
        
        Args:
            users: List of User objects to store
            
        Returns:
            bool: True if successful, False otherwise
            
        Raises:
            DynamoDBError: If DynamoDB operations fail
        """
        if not users:
            self.logger.info("No users to store")
            return True
            
        try:
            dynamodb = self.get_client('dynamodb')
            
            # Generate date prefix and create table name with date prefix
            date_prefix = self.generate_date_prefix()
            table_name = f"{date_prefix}-{self.config.dynamodb_users_table}"
            
            # Ensure table exists
            self._ensure_users_table_exists(table_name)
            
            # Convert users to DynamoDB items
            items = [user.to_dynamodb_item() for user in users]
            
            # Batch write items (DynamoDB batch write limit is 25 items)
            batch_size = 25
            total_batches = (len(items) + batch_size - 1) // batch_size
            
            for i in range(0, len(items), batch_size):
                batch = items[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                
                # Prepare batch write request
                request_items = {
                    table_name: [
                        {'PutRequest': {'Item': self._convert_to_dynamodb_types(item)}}
                        for item in batch
                    ]
                }
                
                # Execute batch write with retry logic
                self._execute_batch_write_with_retry(request_items)
                
                self.logger.debug(f"Completed batch {batch_num}/{total_batches} for users")
            
            self.logger.info(f"Successfully stored {len(users)} users to DynamoDB table '{table_name}'")
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            raise DynamoDBError(f"Failed to store users: {error_code} - {error_message}")
        except BotoCoreError as e:
            raise DynamoDBError(f"AWS connection error while storing users: {str(e)}")
    
    def store_groups_to_dynamodb(self, groups: List[Group]) -> bool:
        """
        Store groups to DynamoDB table with batch writing.
        
        Args:
            groups: List of Group objects to store
            
        Returns:
            bool: True if successful, False otherwise
            
        Raises:
            DynamoDBError: If DynamoDB operations fail
        """
        if not groups:
            self.logger.info("No groups to store")
            return True
            
        try:
            dynamodb = self.get_client('dynamodb')
            
            # Generate date prefix and create table name with date prefix
            date_prefix = self.generate_date_prefix()
            table_name = f"{date_prefix}-{self.config.dynamodb_groups_table}"
            
            # Ensure table exists
            self._ensure_groups_table_exists(table_name)
            
            # Convert groups to DynamoDB items
            items = [group.to_dynamodb_item() for group in groups]
            
            # Batch write items (DynamoDB batch write limit is 25 items)
            batch_size = 25
            total_batches = (len(items) + batch_size - 1) // batch_size
            
            for i in range(0, len(items), batch_size):
                batch = items[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                
                # Prepare batch write request
                request_items = {
                    table_name: [
                        {'PutRequest': {'Item': self._convert_to_dynamodb_types(item)}}
                        for item in batch
                    ]
                }
                
                # Execute batch write with retry logic
                self._execute_batch_write_with_retry(request_items)
                
                self.logger.debug(f"Completed batch {batch_num}/{total_batches} for groups")
            
            self.logger.info(f"Successfully stored {len(groups)} groups to DynamoDB table '{table_name}'")
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            raise DynamoDBError(f"Failed to store groups: {error_code} - {error_message}")
        except BotoCoreError as e:
            raise DynamoDBError(f"AWS connection error while storing groups: {str(e)}")
    
    def store_user_group_memberships_to_dynamodb(self, memberships: List[UserGroupMembership]) -> bool:
        """
        Store user-group memberships to DynamoDB table with batch writing.
        
        Args:
            memberships: List of UserGroupMembership objects to store
            
        Returns:
            bool: True if successful, False otherwise
            
        Raises:
            DynamoDBError: If DynamoDB operations fail
        """
        if not memberships:
            self.logger.info("No user-group memberships to store")
            return True
            
        try:
            dynamodb = self.get_client('dynamodb')
            
            # Generate date prefix and create table name with date prefix
            date_prefix = self.generate_date_prefix()
            table_name = f"{date_prefix}-{self.config.users_group_table_name}"
            
            # Ensure table exists
            self._ensure_user_group_memberships_table_exists(table_name)
            
            # Convert memberships to DynamoDB items
            items = [membership.to_dynamodb_item() for membership in memberships]
            
            # Batch write items (DynamoDB batch write limit is 25 items)
            batch_size = 25
            total_batches = (len(items) + batch_size - 1) // batch_size
            
            for i in range(0, len(items), batch_size):
                batch = items[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                
                # Prepare batch write request
                request_items = {
                    table_name: [
                        {'PutRequest': {'Item': self._convert_to_dynamodb_types(item)}}
                        for item in batch
                    ]
                }
                
                # Execute batch write with retry logic
                self._execute_batch_write_with_retry(request_items)
                
                self.logger.debug(f"Completed batch {batch_num}/{total_batches} for user-group memberships")
            
            self.logger.info(f"Successfully stored {len(memberships)} user-group memberships to DynamoDB table '{table_name}'")
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            raise DynamoDBError(f"Failed to store user-group memberships: {error_code} - {error_message}")
        except BotoCoreError as e:
            raise DynamoDBError(f"AWS connection error while storing user-group memberships: {str(e)}")
    
    def validate_prerequisites(self) -> bool:
        """
        Validate that all prerequisites for backup are met.
        
        Returns:
            bool: True if all prerequisites are met
        """
        try:
            # Test QuickSight access
            quicksight = self.get_client('quicksight-admin')
            quicksight.list_users(
                AwsAccountId=self.config.aws_account_id,
                Namespace='default',
                MaxResults=1
            )
            
            # Test DynamoDB access
            dynamodb = self.get_client('dynamodb')
            dynamodb.list_tables(Limit=1)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Prerequisites validation failed: {str(e)}")
            return False
    
    def _create_client(self, service_name: str):
        """
        Create an AWS service client.
        
        Args:
            service_name: Name of the AWS service
            
        Returns:
            AWS service client
        """
        # Use identity_region for QuickSight operations, aws_region for others
        service = "quicksight"

        if service_name == 'quicksight-admin':
            region = self.get_effective_region()
        else:
            region = self.config.aws_region
            service = service_name
            
        session_kwargs = {'region_name': region}
        
        # Add credentials if provided in config
        if self.config.aws_access_key_id and self.config.aws_secret_access_key:
            session_kwargs.update({
                'aws_access_key_id': self.config.aws_access_key_id,
                'aws_secret_access_key': self.config.aws_secret_access_key
            })
            if self.config.aws_session_token:
                session_kwargs['aws_session_token'] = self.config.aws_session_token
        
        try:
            session = boto3.Session(**session_kwargs)
            return session.client(service)
        except Exception as e:
            raise AWSCredentialsError(f"Failed to create {service} client: {str(e)}")
    
    def _ensure_users_table_exists(self, table_name: str):
        """Ensure the users DynamoDB table exists, create if it doesn't."""
        dynamodb = self.get_client('dynamodb')
        
        try:
            # Check if table exists
            dynamodb.describe_table(TableName=table_name)
            self.logger.debug(f"Users table '{table_name}' already exists")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                # Table doesn't exist, create it
                self.logger.info(f"Creating users table '{table_name}'")
                self._create_users_table(table_name)
            else:
                raise
    
    def _ensure_groups_table_exists(self, table_name: str):
        """Ensure the groups DynamoDB table exists, create if it doesn't."""
        dynamodb = self.get_client('dynamodb')
        
        try:
            # Check if table exists
            dynamodb.describe_table(TableName=table_name)
            self.logger.debug(f"Groups table '{table_name}' already exists")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                # Table doesn't exist, create it
                self.logger.info(f"Creating groups table '{table_name}'")
                self._create_groups_table(table_name)
            else:
                raise
    
    def _create_users_table(self, table_name: str):
        """Create the users DynamoDB table."""
        dynamodb = self.get_client('dynamodb')
        
        table_definition = {
            'TableName': table_name,
            'KeySchema': [
                {
                    'AttributeName': 'user_name',
                    'KeyType': 'HASH'  # Partition key
                }
            ],
            'AttributeDefinitions': [
                {
                    'AttributeName': 'user_name',
                    'AttributeType': 'S'
                }
            ],
            'BillingMode': 'PAY_PER_REQUEST'  # On-demand billing
        }
        
        dynamodb.create_table(**table_definition)
        
        # Wait for table to be created
        waiter = dynamodb.get_waiter('table_exists')
        waiter.wait(TableName=table_name, WaiterConfig={'Delay': 5, 'MaxAttempts': 12})
        
        self.logger.info(f"Successfully created users table '{table_name}'")
    
    def _create_groups_table(self, table_name: str):
        """Create the groups DynamoDB table."""
        dynamodb = self.get_client('dynamodb')
        
        table_definition = {
            'TableName': table_name,
            'KeySchema': [
                {
                    'AttributeName': 'group_name',
                    'KeyType': 'HASH'  # Partition key
                }
            ],
            'AttributeDefinitions': [
                {
                    'AttributeName': 'group_name',
                    'AttributeType': 'S'
                }
            ],
            'BillingMode': 'PAY_PER_REQUEST'  # On-demand billing
        }
        
        dynamodb.create_table(**table_definition)
        
        # Wait for table to be created
        waiter = dynamodb.get_waiter('table_exists')
        waiter.wait(TableName=table_name, WaiterConfig={'Delay': 5, 'MaxAttempts': 12})
        
        self.logger.info(f"Successfully created groups table '{table_name}'")
    
    def _ensure_user_group_memberships_table_exists(self, table_name: str):
        """Ensure the user-group memberships DynamoDB table exists, create if it doesn't."""
        dynamodb = self.get_client('dynamodb')
        
        try:
            # Check if table exists
            dynamodb.describe_table(TableName=table_name)
            self.logger.debug(f"User-group memberships table '{table_name}' already exists")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                # Table doesn't exist, create it
                self.logger.info(f"Creating user-group memberships table '{table_name}'")
                self._create_user_group_memberships_table(table_name)
            else:
                raise
    
    def _create_user_group_memberships_table(self, table_name: str):
        """Create the user-group memberships DynamoDB table."""
        dynamodb = self.get_client('dynamodb')
        
        table_definition = {
            'TableName': table_name,
            'KeySchema': [
                {
                    'AttributeName': 'membership_id',
                    'KeyType': 'HASH'  # Partition key
                }
            ],
            'AttributeDefinitions': [
                {
                    'AttributeName': 'membership_id',
                    'AttributeType': 'S'
                }
            ],
            'BillingMode': 'PAY_PER_REQUEST'  # On-demand billing
        }
        
        dynamodb.create_table(**table_definition)
        
        # Wait for table to be created
        waiter = dynamodb.get_waiter('table_exists')
        waiter.wait(TableName=table_name, WaiterConfig={'Delay': 5, 'MaxAttempts': 12})
        
        self.logger.info(f"Successfully created user-group memberships table '{table_name}'")
    
    def _convert_to_dynamodb_types(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert Python types to DynamoDB attribute value format.
        
        Args:
            item: Dictionary with Python values
            
        Returns:
            Dict[str, Any]: Dictionary with DynamoDB attribute value format
        """
        dynamodb_item = {}
        
        for key, value in item.items():
            if isinstance(value, str):
                dynamodb_item[key] = {'S': value}
            elif isinstance(value, bool):
                dynamodb_item[key] = {'BOOL': value}
            elif isinstance(value, (int, float)):
                dynamodb_item[key] = {'N': str(value)}
            elif isinstance(value, list):
                # Convert list to DynamoDB list format
                if value:  # Only if list is not empty
                    # Assume all items in list are strings for user/group members
                    dynamodb_item[key] = {'SS': value}
                else:
                    dynamodb_item[key] = {'SS': []}
            elif value is None:
                # Skip None values
                continue
            else:
                # Convert other types to string
                dynamodb_item[key] = {'S': str(value)}
        
        return dynamodb_item
    
    def _execute_batch_write_with_retry(self, request_items: Dict[str, Any], max_retries: int = 3):
        """
        Execute batch write with retry logic for unprocessed items.
        
        Args:
            request_items: DynamoDB batch write request items
            max_retries: Maximum number of retry attempts
        """
        dynamodb = self.get_client('dynamodb')
        
        for attempt in range(max_retries + 1):
            try:
                response = dynamodb.batch_write_item(RequestItems=request_items)
                
                # Check for unprocessed items
                unprocessed_items = response.get('UnprocessedItems', {})
                
                if not unprocessed_items:
                    # All items processed successfully
                    return
                
                if attempt < max_retries:
                    # Retry with unprocessed items
                    request_items = unprocessed_items
                    backoff_delay = (2 ** attempt) + (time.time() % 1)  # Exponential backoff with jitter
                    self.logger.warning(f"Retrying batch write after {backoff_delay:.2f}s due to unprocessed items")
                    time.sleep(backoff_delay)
                else:
                    # Max retries exceeded
                    raise DynamoDBError(f"Failed to process all items after {max_retries} retries")
                    
            except ClientError as e:
                if e.response['Error']['Code'] == 'ProvisionedThroughputExceededException' and attempt < max_retries:
                    # Throttling error, retry with backoff
                    backoff_delay = (2 ** attempt) + (time.time() % 1)
                    self.logger.warning(f"Throttling detected, retrying after {backoff_delay:.2f}s")
                    time.sleep(backoff_delay)
                else:
                    raise