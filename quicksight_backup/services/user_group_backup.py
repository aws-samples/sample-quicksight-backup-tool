"""
Service for backing up Quick Sight users and groups.
"""

import logging
import time
from typing import List, Dict, Any, Optional
from botocore.exceptions import ClientError, BotoCoreError
import boto3

from quicksight_backup.models.config import BackupConfig
from quicksight_backup.models.backup_result import (
    BackupResult,
    BackupStatus,
    aggregate_resource_counts,
)
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
    """Service for backing up Quick Sight users and groups to DynamoDB."""
    
    def __init__(self, config: BackupConfig):
        super().__init__(config)
        self.logger = logging.getLogger(__name__)
        self._backup_date_prefix: Optional[str] = None

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

        if self._backup_date_prefix is None:
            now = datetime.now()
            self._backup_date_prefix = f"{now.year:04d}-{now.month:02d}-{now.day:02d}"

        # Log if using a different format than configured for informational purposes
        if self.config.s3_prefix_format not in ["YYYY/MM/DD", "YYYY-MM-DD", "YYYYMMDD"]:
            self.logger.warning(f"Invalid prefix format '{self.config.s3_prefix_format}', using YYYY-MM-DD for table names")

        return self._backup_date_prefix

    @staticmethod
    def _create_child_result(operation_type: str, resource_type: str) -> BackupResult:
        """Create a child operation result with an empty canonical resource count."""
        return BackupResult(
            resource_type=operation_type,
            success=False,
            items_processed=0,
            items_failed=0,
            status=BackupStatus.IN_PROGRESS,
            metadata={
                "resource_counts": {
                    resource_type: {"successful": 0, "failed": 0, "skipped": 0}
                }
            },
        )

    @staticmethod
    def _mark_child_success(
        result: BackupResult, resource_type: str, record_count: int
    ) -> None:
        """Mark every known child record as successfully persisted."""
        result.items_processed = record_count
        result.items_failed = 0
        result.success = True
        result.status = BackupStatus.SUCCESS
        result.metadata["resource_counts"][resource_type] = {
            "successful": record_count,
            "failed": 0,
            "skipped": 0,
        }

    @staticmethod
    def _mark_child_failure(
        result: BackupResult,
        resource_type: str,
        error_message: str,
        known_record_count: Optional[int],
    ) -> None:
        """Mark known records failed, or retain zero counts when enumeration is unknown."""
        failed_count = known_record_count if known_record_count is not None else 0
        result.error_messages.append(error_message)
        result.items_processed = 0
        result.items_failed = failed_count
        result.success = False
        result.status = BackupStatus.FAILED
        result.metadata["resource_counts"][resource_type] = {
            "successful": 0,
            "failed": failed_count,
            "skipped": 0,
        }

    def backup(self) -> BackupResult:
        """
        Run backup of users, groups, and memberships.

        Returns:
            BackupResult: Combined result of user and group backup operations
        """
        start_time = time.time()
        result = BackupResult(
            resource_type="users_and_groups",
            success=False,
            items_processed=0,
            items_failed=0,
            status=BackupStatus.IN_PROGRESS,
        )
        child_results = []
        unexpected_failure = False

        try:
            for backup_operation in (
                self.backup_users,
                self.backup_groups,
                self.backup_user_group_memberships,
            ):
                child_results.append(backup_operation())
        except Exception as e:
            self.logger.error(f"Unexpected error during user/group backup: {str(e)}")
            result.error_messages.append(f"Unexpected error: {str(e)}")
            unexpected_failure = True

        for child_result in child_results:
            result.items_processed += child_result.items_processed
            result.items_failed += child_result.items_failed
            result.error_messages.extend(child_result.error_messages)

        resource_counts = {
            resource_type: {"successful": 0, "failed": 0, "skipped": 0}
            for resource_type in ("user", "group", "membership")
        }
        resource_counts.update(aggregate_resource_counts(child_results))
        result.metadata["resource_counts"] = resource_counts

        has_success = any(
            child_result.status in (BackupStatus.SUCCESS, BackupStatus.PARTIAL)
            for child_result in child_results
        )
        has_failure = unexpected_failure or any(
            child_result.status != BackupStatus.SUCCESS for child_result in child_results
        )

        if not has_failure:
            result.status = BackupStatus.SUCCESS
            result.success = True
        elif has_success:
            result.status = BackupStatus.PARTIAL
            result.success = False
        else:
            result.status = BackupStatus.FAILED
            result.success = False

        result.execution_time = time.time() - start_time
        return result

    def backup_users(self) -> BackupResult:
        """Backup Quick Sight users to DynamoDB."""
        start_time = time.time()
        resource_type = "user"
        result = self._create_child_result("users", resource_type)
        known_record_count: Optional[int] = None

        try:
            self.logger.info("Starting user backup operation")
            users_data = self.get_user_list()
            self.logger.info(f"Retrieved {len(users_data)} users from Quick Sight")
            users = transform_users_from_api_response(users_data)
            known_record_count = len(users)

            if self.store_users_to_dynamodb(users):
                self._mark_child_success(result, resource_type, known_record_count)
                self.logger.info(
                    f"Successfully backed up {known_record_count} users to DynamoDB"
                )
            else:
                self._mark_child_failure(
                    result,
                    resource_type,
                    "Failed to store users to DynamoDB",
                    known_record_count,
                )

        except QuickSightAPIError as e:
            self.logger.error(f"Quick Sight API error during user backup: {str(e)}")
            self._mark_child_failure(
                result,
                resource_type,
                f"Quick Sight API error: {str(e)}",
                known_record_count,
            )
        except DynamoDBError as e:
            self.logger.error(f"DynamoDB error during user backup: {str(e)}")
            self._mark_child_failure(
                result,
                resource_type,
                f"DynamoDB error: {str(e)}",
                known_record_count,
            )
        except Exception as e:
            self.logger.error(f"Unexpected error during user backup: {str(e)}")
            self._mark_child_failure(
                result,
                resource_type,
                f"Unexpected error: {str(e)}",
                known_record_count,
            )

        result.execution_time = time.time() - start_time
        return result

    def backup_groups(self) -> BackupResult:
        """Backup Quick Sight groups to DynamoDB."""
        start_time = time.time()
        resource_type = "group"
        result = self._create_child_result("groups", resource_type)
        known_record_count: Optional[int] = None

        try:
            self.logger.info("Starting group backup operation")
            groups_data = self.get_group_list()
            self.logger.info(f"Retrieved {len(groups_data)} groups from Quick Sight")
            known_record_count = len(groups_data)
            group_members = self._get_group_memberships(groups_data)
            groups = transform_groups_from_api_response(groups_data, group_members)

            if self.store_groups_to_dynamodb(groups):
                self._mark_child_success(result, resource_type, known_record_count)
                self.logger.info(
                    f"Successfully backed up {known_record_count} groups to DynamoDB"
                )
            else:
                self._mark_child_failure(
                    result,
                    resource_type,
                    "Failed to store groups to DynamoDB",
                    known_record_count,
                )

        except QuickSightAPIError as e:
            self.logger.error(f"Quick Sight API error during group backup: {str(e)}")
            self._mark_child_failure(
                result,
                resource_type,
                f"Quick Sight API error: {str(e)}",
                known_record_count,
            )
        except DynamoDBError as e:
            self.logger.error(f"DynamoDB error during group backup: {str(e)}")
            self._mark_child_failure(
                result,
                resource_type,
                f"DynamoDB error: {str(e)}",
                known_record_count,
            )
        except Exception as e:
            self.logger.error(f"Unexpected error during group backup: {str(e)}")
            self._mark_child_failure(
                result,
                resource_type,
                f"Unexpected error: {str(e)}",
                known_record_count,
            )

        result.execution_time = time.time() - start_time
        return result

    def backup_user_group_memberships(self) -> BackupResult:
        """Backup Quick Sight user-group memberships to DynamoDB."""
        start_time = time.time()
        resource_type = "membership"
        result = self._create_child_result("user_group_memberships", resource_type)
        known_record_count: Optional[int] = None

        try:
            self.logger.info("Starting user-group membership backup operation")
            users_data = self.get_user_list()
            groups_data = self.get_group_list()
            group_members = self._get_group_memberships(groups_data)
            users = transform_users_from_api_response(users_data)
            groups = transform_groups_from_api_response(groups_data, group_members)
            memberships = create_user_group_memberships(users, groups)
            known_record_count = len(memberships)
            self.logger.info(
                f"Created {known_record_count} user-group membership relationships"
            )

            if self.store_user_group_memberships_to_dynamodb(memberships):
                self._mark_child_success(result, resource_type, known_record_count)
                self.logger.info(
                    f"Successfully backed up {known_record_count} "
                    "user-group memberships to DynamoDB"
                )
            else:
                self._mark_child_failure(
                    result,
                    resource_type,
                    "Failed to store user-group memberships to DynamoDB",
                    known_record_count,
                )

        except QuickSightAPIError as e:
            self.logger.error(
                f"Quick Sight API error during user-group membership backup: {str(e)}"
            )
            self._mark_child_failure(
                result,
                resource_type,
                f"Quick Sight API error: {str(e)}",
                known_record_count,
            )
        except DynamoDBError as e:
            self.logger.error(
                f"DynamoDB error during user-group membership backup: {str(e)}"
            )
            self._mark_child_failure(
                result,
                resource_type,
                f"DynamoDB error: {str(e)}",
                known_record_count,
            )
        except Exception as e:
            self.logger.error(
                f"Unexpected error during user-group membership backup: {str(e)}"
            )
            self._mark_child_failure(
                result,
                resource_type,
                f"Unexpected error: {str(e)}",
                known_record_count,
            )

        result.execution_time = time.time() - start_time
        return result

    def get_user_list(self) -> List[Dict[str, Any]]:
        """
        Retrieve all Quick Sight users with pagination handling.
        
        Returns:
            List[Dict[str, Any]]: List of user dictionaries from Quick Sight API
            
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
        Retrieve all Quick Sight groups with pagination handling.
        
        Returns:
            List[Dict[str, Any]]: List of group dictionaries from Quick Sight API
            
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
            groups_data: List of group dictionaries from Quick Sight API
            
        Returns:
            Dict[str, List[str]]: Dictionary mapping group names to member lists
        """
        group_members = {}
        quicksight = self.get_client('quicksight-admin')
        
        for group_data in groups_data:
            group_name = group_data.get('GroupName')
            if not group_name:
                raise QuickSightAPIError("Group entry is missing GroupName")
                
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
                error_code = e.response["Error"]["Code"]
                error_message = e.response["Error"]["Message"]
                raise QuickSightAPIError(
                    f"Failed to list members for group '{group_name}': "
                    f"{error_code} - {error_message}"
                )
        
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
                
                # Run batch write with retry logic
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
                
                # Run batch write with retry logic
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
                
                # Run batch write with retry logic
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
            # Test Quick Sight access
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
        # Use identity_region for Quick Sight operations, aws_region for others
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