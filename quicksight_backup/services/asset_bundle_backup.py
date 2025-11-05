"""
Asset bundle backup service for QuickSight resources.
"""

import boto3
import logging
import random
import time
from datetime import datetime
from typing import List, Dict, Any, Optional
from botocore.exceptions import ClientError

from quicksight_backup.services.base import BaseBackupService
from quicksight_backup.models.backup_result import BackupResult
from quicksight_backup.models.asset_inventory import AssetInventory
from quicksight_backup.models.config import BackupConfig
from quicksight_backup.models.exceptions import QuickSightBackupError


logger = logging.getLogger(__name__)


class AssetBundleBackupService(BaseBackupService):
    """Service for backing up QuickSight assets using AssetBundle APIs."""
    
    def __init__(self, config: BackupConfig):
        super().__init__(config)
        self.quicksight_client = None
        self.s3_client = None
        self.skipped_items = []
        self._discovered_datasources = []
    
    def get_effective_region(self) -> str:
        """
        Get the effective region for user/group operations.
        
        Returns:
            str: identity_region if configured, otherwise aws_region
        """
        return self.config.identity_region or self.config.aws_region
    
    def _create_client(self, service_name: str) -> Any:
        """Create AWS service client."""

        service = 'quicksight'        

        if service_name == 'quicksight-admin':
            region = self.get_effective_region()
        else:
            region = self.config.aws_region
            service = service_name

        session_kwargs = {'region_name': region}
        
        # Use explicit credentials if provided, otherwise use default credential chain
        if self.config.aws_access_key_id and self.config.aws_secret_access_key:
            session_kwargs.update({
                'aws_access_key_id': self.config.aws_access_key_id,
                'aws_secret_access_key': self.config.aws_secret_access_key
            })
            if self.config.aws_session_token:
                session_kwargs['aws_session_token'] = self.config.aws_session_token
        
        session = boto3.Session(**session_kwargs)
        return session.client(service)
    
    def backup(self) -> BackupResult:
        """Execute asset bundle backup operation."""
        start_time = time.time()
        result = BackupResult(
            resource_type="asset_bundles",
            success=False,
            items_processed=0,
            items_failed=0,
            error_messages=[],
            execution_time=0.0,
            timestamp=datetime.now()
        )
        
        try:
            # Validate configuration
            if not self.validate_max_assets_per_bundle(self.config.max_assets_per_bundle):
                raise QuickSightBackupError(f"Invalid max_assets_per_bundle value: {self.config.max_assets_per_bundle}. Must be between 1 and 100 inclusive.")
            
            # Initialize clients
            self.quicksight_client = self.get_client('quicksight')
            self.s3_client = self.get_client('s3')
            
            # Discover assets
            logger.info("Starting asset discovery...")
            inventory = self.discover_assets()
            
            if inventory.total_count == 0:
                logger.warning("No assets found to backup")
                result.success = True
                return result
            
            logger.info(f"Discovered {inventory.total_count} assets for backup")
            
            # Backup assets by type
            total_processed = 0
            total_failed = 0
            
            for asset_type in ['datasources', 'datasets', 'analyses', 'dashboards']:
                assets = getattr(inventory, asset_type)
                if assets:
                    processed, failed = self._backup_asset_type(asset_type, assets)
                    total_processed += processed
                    total_failed += failed
            
            result.items_processed = total_processed
            result.items_failed = total_failed
            result.success = total_failed == 0
            
            # Include skipped items in metadata for reporting
            if hasattr(self, 'skipped_items') and self.skipped_items:
                result.metadata['skipped_items'] = self.skipped_items
                logger.info(f"Total skipped items: {len(self.skipped_items)}")
            
        except Exception as e:
            logger.error(f"Asset bundle backup failed: {str(e)}")
            result.error_messages.append(str(e))
            result.success = False
        
        finally:
            result.execution_time = time.time() - start_time
        
        return result
    
    def discover_assets(self) -> AssetInventory:
        """Discover all QuickSight assets for backup."""
        inventory = AssetInventory()
        
        try:
            # Discover datasources
            logger.info("Discovering datasources...")
            inventory.datasources = self._list_datasources()
            
            # Discover datasets (excluding FILE datasets)
            logger.info("Discovering datasets...")
            inventory.datasets = self._list_datasets()
            
            # Discover analyses
            logger.info("Discovering analyses...")
            inventory.analyses = self._list_analyses()
            
            # Discover dashboards
            logger.info("Discovering dashboards...")
            inventory.dashboards = self._list_dashboards()
            
        except Exception as e:
            logger.error(f"Asset discovery failed: {str(e)}")
            raise QuickSightBackupError(f"Failed to discover assets: {str(e)}")
        
        return inventory
    
    def validate_max_assets_per_bundle(self, max_assets: int) -> bool:
        """
        Validate that max_assets_per_bundle is within acceptable range.
        
        Args:
            max_assets: The maximum assets per bundle value to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        return isinstance(max_assets, int) and 1 <= max_assets <= 100
    
    def chunk_assets_for_bundles(self, assets: List[Dict[str, Any]], max_per_bundle: int) -> List[List[Dict[str, Any]]]:
        """
        Split large asset collections into chunks based on max_assets_per_bundle.
        
        Args:
            assets: List of assets to chunk
            max_per_bundle: Maximum number of assets per bundle
            
        Returns:
            List of asset chunks, each containing at most max_per_bundle assets
        """
        if not assets:
            return []
        
        if max_per_bundle <= 0 or max_per_bundle > 100:
            raise ValueError(f"max_per_bundle must be between 1 and 100, but its current value is {max_per_bundle}")
        
        chunks = []
        for i in range(0, len(assets), max_per_bundle):
            chunk = assets[i:i + max_per_bundle]
            chunks.append(chunk)
        
        logger.info(f"Split {len(assets)} assets into {len(chunks)} chunks (max {max_per_bundle} per chunk)")
        return chunks
    
    def _list_datasources(self) -> List[Dict[str, Any]]:
        """List all QuickSight datasources, excluding S3 manifest-based datasources."""
        datasources = []
        skipped_count = 0
        paginator = self.quicksight_client.get_paginator('list_data_sources')
        
        try:
            for page in paginator.paginate(AwsAccountId=self.config.aws_account_id):
                for datasource in page.get('DataSources', []):
                    # Check if this is an S3 datasource created via local manifest file upload
                    if self._is_s3_local_manifest_datasource(datasource):
                        skipped_count += 1
                        # Track skipped item for reporting
                        skipped_item = {
                            'resource_id': datasource.get('DataSourceId', 'Unknown'),
                            'resource_type': 'datasource',
                            'resource_name': datasource.get('Name', 'Unknown'),
                            'reason': 'S3 datasource created via local manifest file upload'
                        }
                        self.skipped_items.append(skipped_item)
                        logger.info(f"Skipping S3 manifest-based datasource: {datasource.get('Name', 'Unknown')} (ID: {datasource.get('DataSourceId', 'Unknown')})")
                    # Check if this datasource has invalid VPC connection
                    elif self._has_invalid_vpc_connection(datasource):
                        skipped_count += 1
                        # Track skipped item for reporting
                        skipped_item = {
                            'resource_id': datasource.get('DataSourceId', 'Unknown'),
                            'resource_type': 'datasource',
                            'resource_name': datasource.get('Name', 'Unknown'),
                            'reason': 'Datasource contains invalid VPC connection ID'
                        }
                        self.skipped_items.append(skipped_item)
                        logger.info(f"Skipping datasource with invalid VPC connection: {datasource.get('Name', 'Unknown')} (ID: {datasource.get('DataSourceId', 'Unknown')})")
                    else:
                        datasources.append(datasource)
                        logger.debug(f"Found datasource: {datasource.get('Name', 'Unknown')}")
        
        except ClientError as e:
            logger.error(f"Failed to list datasources: {str(e)}")
            raise QuickSightBackupError(f"Failed to list datasources: {str(e)}")
        
        if skipped_count > 0:
            logger.info(f"Skipped {skipped_count} datasources (S3 manifest-based and invalid VPC connections)")
        
        logger.info(f"Found {len(datasources)} datasources (problematic datasources excluded)")
        
        # Store discovered datasources for later reference during dataset validation
        self._discovered_datasources = datasources
        
        return datasources
    
    def _is_s3_local_manifest_datasource(self, datasource: Dict[str, Any]) -> bool:
        """
        Check if a datasource is an S3 datasource created via local manifest file upload.
        
        These datasources have Type "S3" but lack S3Parameters under DataSourceParameters,
        indicating they were created through local manifest upload rather than standard configuration.
        
        Args:
            datasource: Datasource metadata from ListDataSources API
            
        Returns:
            bool: True if this is an S3 manifest-based datasource that should be skipped
        """
        try:
            # Check if datasource type is S3
            datasource_type = datasource.get('Type')
            if datasource_type != 'S3':
                return False
            
            # Get detailed datasource information to check DataSourceParameters
            datasource_id = datasource.get('DataSourceId')
            if not datasource_id:
                logger.warning(f"Datasource missing DataSourceId: {datasource}")
                return False
            
            # Describe the datasource to get full configuration
            response = self.quicksight_client.describe_data_source(
                AwsAccountId=self.config.aws_account_id,
                DataSourceId=datasource_id
            )
            
            datasource_details = response.get('DataSource', {})
            data_source_parameters = datasource_details.get('DataSourceParameters', {})
            
            # Check if S3Parameters are missing - this indicates a manifest-based datasource
            s3_parameters = data_source_parameters.get('S3Parameters')
            
            if s3_parameters is None:
                logger.debug(f"S3 datasource {datasource_id} lacks S3Parameters - identified as local manifest upload")
                return True
            
            return False
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ResourceNotFoundException':
                logger.warning(f"Datasource {datasource_id} not found during detailed check")
                return False
            elif error_code == 'AccessDeniedException':
                logger.warning(f"Access denied for datasource {datasource_id} - including in backup")
                return False
            else:
                logger.error(f"Error checking datasource {datasource_id}: {error_code}")
                return False
        
        except Exception as e:
            logger.error(f"Unexpected error checking datasource {datasource.get('DataSourceId', 'Unknown')}: {str(e)}")
            return False
    
    def _has_invalid_vpc_connection(self, datasource: Dict[str, Any]) -> bool:
        """
        Check if a datasource has an invalid VPC connection ID.
        
        Validates that VPC connection IDs match the pattern ^[\\w\\-]+$ as required by AWS.
        
        Args:
            datasource: Datasource metadata from ListDataSources API
            
        Returns:
            bool: True if this datasource has an invalid VPC connection and should be skipped
        """
        import re
        
        try:
            # Get detailed datasource information to check VPC connections
            datasource_id = datasource.get('DataSourceId')
            if not datasource_id:
                logger.warning(f"Datasource missing DataSourceId: {datasource}")
                return False
            
            # Describe the datasource to get full configuration
            response = self.quicksight_client.describe_data_source(
                AwsAccountId=self.config.aws_account_id,
                DataSourceId=datasource_id
            )
            
            datasource_details = response.get('DataSource', {})
            vpc_connection_properties = datasource_details.get('VpcConnectionProperties')
            
            # If no VPC connection, it's valid
            if not vpc_connection_properties:
                return False
            
            vpc_connection_arn = vpc_connection_properties.get('VpcConnectionArn')
            if not vpc_connection_arn:
                return False
            
            # Extract VPC connection ID from ARN
            # ARN format: arn:aws:quicksight:region:account:vpcConnection/vpc-connection-id
            vpc_connection_id = vpc_connection_arn.split('/')[-1] if '/' in vpc_connection_arn else vpc_connection_arn
            
            # Validate VPC connection ID pattern: ^[\\w\\-]+$
            vpc_id_pattern = r'^[\w\-]+$'
            if not re.match(vpc_id_pattern, vpc_connection_id):
                logger.debug(f"Skipping datasource {datasource_id} due to invalid VPC connection ID: {vpc_connection_id}")
                return True
            
            return False
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ResourceNotFoundException':
                logger.warning(f"Datasource {datasource_id} not found during VPC validation")
                return False
            elif error_code == 'AccessDeniedException':
                logger.warning(f"Access denied for datasource {datasource_id} during VPC validation - including in backup")
                return False
            else:
                logger.error(f"Error validating VPC connection for datasource {datasource_id}: {error_code}")
                return False
        
        except Exception as e:
            logger.error(f"Unexpected error validating VPC connection for datasource {datasource.get('DataSourceId', 'Unknown')}: {str(e)}")
            return False
    
    def _validate_dataset(self, dataset: Dict[str, Any]) -> bool:
        """
        Validate a dataset using describe_data_set operation.
        
        Handles InvalidParameterValueException by skipping the dataset and logging the failure.
        
        Args:
            dataset: Dataset metadata from ListDataSets API
            
        Returns:
            bool: True if dataset is valid and should be included, False if it should be skipped
        """
        try:
            dataset_id = dataset.get('DataSetId')
            if not dataset_id:
                logger.warning(f"Dataset missing DataSetId: {dataset}")
                return False
            
            # Validate dataset by describing it
            response = self.quicksight_client.describe_data_set(
                AwsAccountId=self.config.aws_account_id,
                DataSetId=dataset_id
            )
            
            # Check for datasource dependencies
            dataset_details = response.get('DataSet', {})
            if self._has_skipped_datasource_dependency(dataset_details, dataset):
                return False
            
            # If describe_data_set succeeds and no dependency issues, the dataset is valid
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            
            if error_code == 'InvalidParameterValueException':
                # Track skipped item for reporting
                skipped_item = {
                    'resource_id': dataset.get('DataSetId', 'Unknown'),
                    'resource_type': 'dataset',
                    'resource_name': dataset.get('Name', 'Unknown'),
                    'reason': f'Dataset validation failed: {error_message}'
                }
                self.skipped_items.append(skipped_item)
                logger.info(f"Skipping dataset with invalid parameters: {dataset.get('Name', 'Unknown')} (ID: {dataset.get('DataSetId', 'Unknown')}) - {error_message}")
                return False
            elif error_code == 'ResourceNotFoundException':
                logger.warning(f"Dataset {dataset_id} not found during validation")
                return False
            elif error_code == 'AccessDeniedException':
                logger.warning(f"Access denied for dataset {dataset_id} during validation - including in backup")
                return True  # Include in backup if access is denied during validation
            else:
                logger.error(f"Error validating dataset {dataset_id}: {error_code} - {error_message}")
                return True  # Include in backup for other errors to avoid false negatives
        
        except Exception as e:
            logger.error(f"Unexpected error validating dataset {dataset.get('DataSetId', 'Unknown')}: {str(e)}")
            return True  # Include in backup for unexpected errors
    
    def _has_skipped_datasource_dependency(self, dataset_details: Dict[str, Any], dataset_summary: Dict[str, Any]) -> bool:
        """
        Check if a dataset has dependencies on skipped datasources.
        
        Examines the PhysicalTableMap to find DataSourceArn references and checks if any
        of those datasources were skipped during datasource discovery.
        
        Args:
            dataset_details: Full dataset details from describe_data_set API
            dataset_summary: Dataset summary from list_data_sets API (for logging)
            
        Returns:
            bool: True if dataset should be skipped due to datasource dependencies
        """
        try:
            dataset_id = dataset_details.get('DataSetId', {})
            physical_table_map = dataset_details.get('PhysicalTableMap', {})
            if not physical_table_map:
                return False
            
            # Get list of skipped datasource ARNs
            skipped_datasource_arns = self._get_skipped_datasource_arns()
            if not skipped_datasource_arns:
                return False
            
            # Check each physical table for datasource dependencies
            for table_id, table_config in physical_table_map.items():
                # Check different table types for DataSourceArn
                datasource_arn = None
                
                if 'RelationalTable' in table_config:
                    datasource_arn = table_config['RelationalTable'].get('DataSourceArn')
                elif 'CustomSql' in table_config:
                    datasource_arn = table_config['CustomSql'].get('DataSourceArn')
                elif 'S3Source' in table_config:
                    datasource_arn = table_config['S3Source'].get('DataSourceArn')
                
                # If we found a datasource ARN, check if it's in the skipped list or doesn't exist
                if datasource_arn:
                    if datasource_arn in skipped_datasource_arns:
                        # Track skipped item for reporting
                        skipped_item = {
                            'resource_id': dataset_summary.get('DataSetId', 'Unknown'),
                            'resource_type': 'dataset',
                            'resource_name': dataset_summary.get('Name', 'Unknown'),
                            'reason': f'Dataset depends on skipped datasource: {datasource_arn}'
                        }
                        self.skipped_items.append(skipped_item)
                        logger.info(f"Skipping dataset due to skipped datasource dependency: {dataset_summary.get('Name', 'Unknown')} (ID: {dataset_summary.get('DataSetId', 'Unknown')}) - depends on {datasource_arn}")
                        return True
                    
                    # Check if datasource exists in discovered datasources
                    if not self._datasource_exists(datasource_arn):
                        # Track skipped item for reporting
                        skipped_item = {
                            'resource_id': dataset_summary.get('DataSetId', 'Unknown'),
                            'resource_type': 'dataset',
                            'resource_name': dataset_summary.get('Name', 'Unknown'),
                            'reason': f'Dataset references non-existent datasource: {datasource_arn}'
                        }
                        self.skipped_items.append(skipped_item)
                        logger.info(f"Skipping dataset due to non-existent datasource reference: {dataset_summary.get('Name', 'Unknown')} (ID: {dataset_summary.get('DataSetId', 'Unknown')}) - references {datasource_arn}")
                        return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking datasource dependencies for dataset {dataset_summary.get('DataSetId', 'Unknown')}: {str(e)}")
            return False  # Don't skip on error to avoid false positives
    
    def _get_skipped_datasource_arns(self) -> set:
        """
        Get a set of ARNs for datasources that were skipped during discovery.
        
        Returns:
            set: Set of datasource ARNs that were skipped
        """
        skipped_arns = set()
        
        for skipped_item in self.skipped_items:
            if skipped_item.get('resource_type') == 'datasource':
                resource_id = skipped_item.get('resource_id')
                if resource_id:
                    # Construct the datasource ARN from the resource ID
                    # ARN format: arn:aws:quicksight:region:account:datasource/datasource-id
                    datasource_arn = f"arn:aws:quicksight:{self.config.aws_region}:{self.config.aws_account_id}:datasource/{resource_id}"
                    skipped_arns.add(datasource_arn)
        
        return skipped_arns
    
    def _datasource_exists(self, datasource_arn: str) -> bool:
        """
        Check if a datasource exists by using describe_data_source API call.
        
        Args:
            datasource_arn: The ARN of the datasource to check
            
        Returns:
            bool: True if datasource exists, False otherwise
        """
        try:
            # Extract datasource ID from ARN
            # ARN format: arn:aws:quicksight:region:account:datasource/datasource-id
            datasource_id = datasource_arn.split('/')[-1] if '/' in datasource_arn else datasource_arn
            
            # Use describe_data_source to check if datasource exists
            self.quicksight_client.describe_data_source(
                AwsAccountId=self.config.aws_account_id,
                DataSourceId=datasource_id
            )
            
            # If describe_data_source succeeds, the datasource exists
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ResourceNotFoundException':
                logger.debug(f"Datasource {datasource_id} not found")
                return False
            elif error_code == 'AccessDeniedException':
                logger.warning(f"Access denied for datasource {datasource_id} - assuming it exists")
                return True  # Assume exists if access is denied to avoid false positives
            else:
                logger.error(f"Error checking datasource existence for {datasource_id}: {error_code}")
                return True  # Assume exists on other errors to avoid false positives
                
        except Exception as e:
            logger.error(f"Unexpected error checking datasource existence for ARN {datasource_arn}: {str(e)}")
            return True  # Assume exists on error to avoid false positives
    
    def _validate_analysis_or_dashboard(self, item: Dict[str, Any], item_type: str) -> bool:
        """
        Validate an analysis or dashboard for dataset dependencies and theme references.
        
        Checks if the analysis/dashboard references any skipped datasets or contains
        ThemeArn references to non-existent themes.
        
        Args:
            item: Analysis or dashboard metadata from list API
            item_type: Either 'analysis' or 'dashboard' for logging purposes
            
        Returns:
            bool: True if item is valid and should be included, False if it should be skipped
        """
        try:
            item_id = item.get('AnalysisId') if item_type == 'analysis' else item.get('DashboardId')
            if not item_id:
                logger.warning(f"{item_type.capitalize()} missing ID: {item}")
                return False
            
            # Get detailed information about the analysis/dashboard
            if item_type == 'analysis':
                response = self.quicksight_client.describe_analysis(
                    AwsAccountId=self.config.aws_account_id,
                    AnalysisId=item_id
                )
                item_details = response.get('Analysis', {})
            else:
                response = self.quicksight_client.describe_dashboard(
                    AwsAccountId=self.config.aws_account_id,
                    DashboardId=item_id
                )
                item_details = response.get('Dashboard', {})
            
            # Check for dataset dependencies
            if self._has_skipped_dataset_dependency(item_details, item, item_type):
                return False
            
            # Check for theme dependencies
            if self._has_invalid_theme_dependency(item_details, item, item_type):
                return False
            
            # Check for datasource dependencies in referenced datasets
            if self._has_invalid_datasource_dependency(item_details, item, item_type):
                return False
            
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            
            if error_code == 'ResourceNotFoundException':
                logger.warning(f"{item_type.capitalize()} {item_id} not found during validation")
                return False
            elif error_code == 'AccessDeniedException':
                logger.warning(f"Access denied for {item_type} {item_id} during validation - including in backup")
                return True  # Include in backup if access is denied during validation
            else:
                logger.error(f"Error validating {item_type} {item_id}: {error_code} - {error_message}")
                return True  # Include in backup for other errors to avoid false negatives
        
        except Exception as e:
            logger.error(f"Unexpected error validating {item_type} {item.get('AnalysisId' if item_type == 'analysis' else 'DashboardId', 'Unknown')}: {str(e)}")
            return True  # Include in backup for unexpected errors
    
    def _has_skipped_dataset_dependency(self, item_details: Dict[str, Any], item_summary: Dict[str, Any], item_type: str) -> bool:
        """
        Check if an analysis or dashboard has dependencies on skipped datasets.
        
        Args:
            item_details: Full item details from describe API
            item_summary: Item summary from list API (for logging)
            item_type: Either 'analysis' or 'dashboard'
            
        Returns:
            bool: True if item should be skipped due to dataset dependencies
        """
        try:
            # Get list of skipped dataset ARNs
            skipped_dataset_arns = self._get_skipped_dataset_arns()
            if not skipped_dataset_arns:
                return False
            
            # Check data sets referenced in the item
            if item_type == 'analysis':
                data_set_references = item_details.get('DataSetArns', [])
            else:
                data_set_references = item_details['Version'].get('DataSetArns', [])

            for dataset_arn in data_set_references:                
                if dataset_arn in skipped_dataset_arns:
                    # Track skipped item for reporting
                    item_id = item_summary.get('AnalysisId') if item_type == 'analysis' else item_summary.get('DashboardId')
                    skipped_item = {
                        'resource_id': item_id,
                        'resource_type': item_type,
                        'resource_name': item_summary.get('Name', 'Unknown'),
                        'reason': f'{item_type.capitalize()} depends on skipped dataset: {dataset_arn}'
                    }
                    self.skipped_items.append(skipped_item)
                    logger.info(f"Skipping {item_type} with id {item_id} due to dataset dependency: {item_summary.get('Name', 'Unknown')} (ID: {item_id}) - depends on {dataset_arn}")
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking dataset dependencies for {item_type} {item_summary.get('AnalysisId' if item_type == 'analysis' else 'DashboardId', 'Unknown')}: {str(e)}")
            return False  # Don't skip on error to avoid false positives
    
    def _has_invalid_theme_dependency(self, item_details: Dict[str, Any], item_summary: Dict[str, Any], item_type: str) -> bool:
        """
        Check if an analysis or dashboard has invalid theme dependencies.
        
        Args:
            item_details: Full item details from describe API
            item_summary: Item summary from list API (for logging)
            item_type: Either 'analysis' or 'dashboard'
            
        Returns:
            bool: True if item should be skipped due to theme dependencies
        """

        if item_type == 'analysis':
            theme_arn = item_details.get('ThemeArn', [])
        else:
            theme_arn = item_details['Version'].get('ThemeArn', [])
        
        if not theme_arn:
            return False  # No theme dependency
        
        # Extract theme ID from ARN
        # ARN format: arn:aws:quicksight:region:account:theme/theme-id
        theme_id = theme_arn.split('/')[-1] if '/' in theme_arn else theme_arn
        
        # Check if theme exists
        try:
            self.quicksight_client.describe_theme(
                AwsAccountId=self.config.aws_account_id,
                ThemeId=theme_id
            )
            return False  # Theme exists, no issue
            
        except ClientError as theme_error:
            if theme_error.response['Error']['Code'] == 'ResourceNotFoundException':
                # Theme doesn't exist, skip the analysis/dashboard
                item_id = item_summary.get('AnalysisId') if item_type == 'analysis' else item_summary.get('DashboardId')
                skipped_item = {
                    'resource_id': item_id,
                    'resource_type': item_type,
                    'resource_name': item_summary.get('Name', 'Unknown'),
                    'reason': f'{item_type.capitalize()} references non-existent theme: {theme_arn}'
                }
                self.skipped_items.append(skipped_item)
                logger.info(f"Skipping {item_type} with id {item_id} due to invalid theme reference: {item_summary.get('Name', 'Unknown')} (ID: {item_id}) - theme {theme_arn} not found")
                return True
            else:
                # Other error, don't skip
                logger.warning(f"Error checking theme {theme_arn} for {item_type} {item_summary.get('Name', 'Unknown')}: {theme_error}")
                return False
        
        except Exception as e:
            logger.error(f"Error checking theme dependencies for {item_type} {item_summary.get('AnalysisId' if item_type == 'analysis' else 'DashboardId', 'Unknown')}: {str(e)}")
            return False  # Don't skip on error to avoid false positives
    
    def _has_invalid_datasource_dependency(self, item_details: Dict[str, Any], item_summary: Dict[str, Any], item_type: str) -> bool:
        """
        Check if an analysis or dashboard has invalid datasource dependencies through its datasets.
        
        Validates that all datasources referenced by the datasets used in the analysis/dashboard exist.
        
        Args:
            item_details: Full item details from describe API
            item_summary: Item summary from list API (for logging)
            item_type: Either 'analysis' or 'dashboard'
            
        Returns:
            bool: True if item should be skipped due to datasource dependencies
        """
        try:
            # Get data set references from the definition
            definition = item_details.get('Definition', {})
            data_set_references = definition.get('DataSetIdentifierDeclarations', [])
            
            if not data_set_references:
                return False  # No dataset dependencies
            
            # Check each dataset for datasource dependencies
            for dataset_ref in data_set_references:
                dataset_arn = dataset_ref.get('DataSetArn')
                if not dataset_arn:
                    continue
                
                # Extract dataset ID from ARN
                dataset_id = dataset_arn.split('/')[-1] if '/' in dataset_arn else dataset_arn
                
                # Get dataset details to check its datasource dependencies
                try:
                    dataset_response = self.quicksight_client.describe_data_set(
                        AwsAccountId=self.config.aws_account_id,
                        DataSetId=dataset_id
                    )
                    
                    dataset_details = dataset_response.get('DataSet', {})
                    physical_table_map = dataset_details.get('PhysicalTableMap', {})
                    
                    # Check each physical table for datasource dependencies
                    for table_id, table_config in physical_table_map.items():
                        datasource_arn = None
                        
                        if 'RelationalTable' in table_config:
                            datasource_arn = table_config['RelationalTable'].get('DataSourceArn')
                        elif 'CustomSql' in table_config:
                            datasource_arn = table_config['CustomSql'].get('DataSourceArn')
                        elif 'S3Source' in table_config:
                            datasource_arn = table_config['S3Source'].get('DataSourceArn')
                        
                        # If we found a datasource ARN, check if it exists
                        if datasource_arn and not self._datasource_exists(datasource_arn):
                            # Track skipped item for reporting
                            item_id = item_summary.get('AnalysisId') if item_type == 'analysis' else item_summary.get('DashboardId')
                            skipped_item = {
                                'resource_id': item_id,
                                'resource_type': item_type,
                                'resource_name': item_summary.get('Name', 'Unknown'),
                                'reason': f'{item_type.capitalize()} depends on dataset {dataset_arn} which references non-existent datasource: {datasource_arn}'
                            }
                            self.skipped_items.append(skipped_item)
                            logger.info(f"Skipping {item_type} due to datasource dependency: {item_summary.get('Name', 'Unknown')} (ID: {item_id}) - dataset {dataset_arn} references non-existent datasource {datasource_arn}")
                            return True
                
                except ClientError as dataset_error:
                    if dataset_error.response['Error']['Code'] == 'ResourceNotFoundException':
                        # Dataset doesn't exist, but this should have been caught earlier
                        logger.warning(f"Dataset {dataset_id} not found during datasource validation for {item_type} {item_summary.get('Name', 'Unknown')}")
                        continue
                    elif dataset_error.response['Error']['Code'] == 'AccessDeniedException':
                        # Can't access dataset, assume it's valid
                        logger.warning(f"Access denied for dataset {dataset_id} during datasource validation for {item_type} {item_summary.get('Name', 'Unknown')}")
                        continue
                    else:
                        logger.error(f"Error checking dataset {dataset_id} for {item_type} {item_summary.get('Name', 'Unknown')}: {dataset_error}")
                        continue
            
            return False  # No invalid datasource dependencies found
            
        except Exception as e:
            logger.error(f"Error checking datasource dependencies for {item_type} {item_summary.get('AnalysisId' if item_type == 'analysis' else 'DashboardId', 'Unknown')}: {str(e)}")
            return False  # Don't skip on error to avoid false positives
    
    def _get_skipped_dataset_arns(self) -> set:
        """
        Get a set of ARNs for datasets that were skipped during discovery.
        
        Returns:
            set: Set of dataset ARNs that were skipped
        """
        skipped_arns = set()
        
        for skipped_item in self.skipped_items:
            if skipped_item.get('resource_type') == 'dataset':
                resource_id = skipped_item.get('resource_id')
                if resource_id:
                    # Construct the dataset ARN from the resource ID
                    # ARN format: arn:aws:quicksight:region:account:dataset/dataset-id
                    dataset_arn = f"arn:aws:quicksight:{self.config.aws_region}:{self.config.aws_account_id}:dataset/{resource_id}"
                    skipped_arns.add(dataset_arn)
        
        return skipped_arns
    
    def _list_datasets(self) -> List[Dict[str, Any]]:
        """List all QuickSight datasets, excluding FILE datasets."""
        datasets = []
        skipped_count = 0
        paginator = self.quicksight_client.get_paginator('list_data_sets')
        
        try:
            for page in paginator.paginate(AwsAccountId=self.config.aws_account_id):
                for dataset in page.get('DataSetSummaries', []):
                    # Skip FILE datasets as per requirements
                    if dataset.get('ImportMode') not in ['DIRECT_QUERY', 'SPICE']:
                        skipped_count += 1
                        # Track skipped item for reporting
                        skipped_item = {
                            'resource_id': dataset.get('DataSetId', 'Unknown'),
                            'resource_type': 'dataset',
                            'resource_name': dataset.get('Name', 'Unknown'),
                            'reason': 'FILE dataset type not supported by AssetBundle API'
                        }
                        self.skipped_items.append(skipped_item)
                        logger.info(f"Skipping FILE dataset: {dataset.get('Name', 'Unknown')} (ID: {dataset.get('DataSetId', 'Unknown')})")
                        continue
                    
                    # Validate dataset using describe_data_set operation
                    if self._validate_dataset(dataset):
                        datasets.append(dataset)
                        logger.debug(f"Found dataset: {dataset.get('Name', 'Unknown')}")
                    else:
                        skipped_count += 1
                        # Skipped item tracking is handled in _validate_dataset method
        
        except ClientError as e:
            logger.error(f"Failed to list datasets: {str(e)}")
            raise QuickSightBackupError(f"Failed to list datasets: {str(e)}")
        
        if skipped_count > 0:
            logger.info(f"Skipped {skipped_count} datasets (FILE datasets and validation failures)")
        
        logger.info(f"Found {len(datasets)} datasets (problematic datasets excluded)")
        return datasets
    
    def _list_analyses(self) -> List[Dict[str, Any]]:
        """List all QuickSight analyses, excluding those with invalid dependencies."""
        analyses = []
        skipped_count = 0
        paginator = self.quicksight_client.get_paginator('list_analyses')
        
        try:
            for page in paginator.paginate(AwsAccountId=self.config.aws_account_id):
                for analysis in page.get('AnalysisSummaryList', []):
                    # Validate analysis dependencies
                    if self._validate_analysis_or_dashboard(analysis, 'analysis'):
                        analyses.append(analysis)
                        logger.debug(f"Found analysis: {analysis.get('Name', 'Unknown')}")
                    else:
                        skipped_count += 1
                        # Skipped item tracking is handled in _validate_analysis_or_dashboard method
        
        except ClientError as e:
            logger.error(f"Failed to list analyses: {str(e)}")
            raise QuickSightBackupError(f"Failed to list analyses: {str(e)}")
        
        if skipped_count > 0:
            logger.info(f"Skipped {skipped_count} analyses (dependency validation failures)")
        
        logger.info(f"Found {len(analyses)} analyses (problematic analyses excluded)")
        return analyses
    
    def _list_dashboards(self) -> List[Dict[str, Any]]:
        """List all QuickSight dashboards, excluding those with invalid dependencies."""
        dashboards = []
        skipped_count = 0
        paginator = self.quicksight_client.get_paginator('list_dashboards')
        
        try:
            for page in paginator.paginate(AwsAccountId=self.config.aws_account_id):
                for dashboard in page.get('DashboardSummaryList', []):
                    # Validate dashboard dependencies
                    if self._validate_analysis_or_dashboard(dashboard, 'dashboard'):
                        dashboards.append(dashboard)
                        logger.debug(f"Found dashboard: {dashboard.get('Name', 'Unknown')}")
                    else:
                        skipped_count += 1
                        # Skipped item tracking is handled in _validate_analysis_or_dashboard method
        
        except ClientError as e:
            logger.error(f"Failed to list dashboards: {str(e)}")
            raise QuickSightBackupError(f"Failed to list dashboards: {str(e)}")
        
        if skipped_count > 0:
            logger.info(f"Skipped {skipped_count} dashboards (dependency validation failures)")
        
        logger.info(f"Found {len(dashboards)} dashboards (problematic dashboards excluded)")
        return dashboards
    
    def validate_prerequisites(self) -> bool:
        """Validate prerequisites for asset bundle backup."""
        try:
            # Test QuickSight access
            self.quicksight_client = self.get_client('quicksight-admin')
            self.quicksight_client.describe_account_settings(AwsAccountId=self.config.aws_account_id)
            
            # Test S3 access
            self.s3_client = self.get_client('s3')
            self.s3_client.head_bucket(Bucket=self.config.s3_bucket_name)
            
            return True
        
        except Exception as e:
            logger.error(f"Prerequisites validation failed: {str(e)}")
            return False
    
    def _backup_asset_type(self, asset_type: str, assets: List[Dict[str, Any]]) -> tuple[int, int]:
        """Backup assets of a specific type."""
        if not assets:
            return 0, 0
        
        logger.info(f"Starting backup for {len(assets)} {asset_type}")
        processed = 0
        failed = 0

        max_retries = 5
        base_delay = 1.0  # Start with 1 second
        max_delay = 60.0  # Maximum delay of 60 seconds
        
        try:
            # Get ARNs for the assets
            asset_arns = []
            for asset in assets:
                arn = asset.get('Arn')
                if arn:
                    asset_arns.append(arn)
                else:
                    logger.warning(f"Asset missing ARN: {asset}")
                    failed += 1
            
            if not asset_arns:
                logger.warning(f"No valid ARNs found for {asset_type}")
                return 0, len(assets)
            
            # Chunk assets based on max_assets_per_bundle configuration
            asset_chunks = self.chunk_assets_for_bundles(assets, self.config.max_assets_per_bundle)
            arn_chunks = []
            
            # Create corresponding ARN chunks
            arn_index = 0
            for asset_chunk in asset_chunks:
                chunk_arns = []
                for asset in asset_chunk:
                    if asset.get('Arn') in asset_arns:
                        chunk_arns.append(asset.get('Arn'))
                arn_chunks.append(chunk_arns)
            
            # Process each chunk as a separate bundle
            for chunk_index, arn_chunk in enumerate(arn_chunks):
                if not arn_chunk:
                    continue
                
                bundle_number = chunk_index + 1 if len(arn_chunks) > 1 else None
                
                try:
                    for attempt in range(max_retries + 1):
                        """Start an AssetBundle export job with exponential backoff for throttling."""
                        try:
                            # Start export job for this chunk
                            job_id = self.start_export_job(arn_chunk, asset_type, bundle_number, attempt, max_retries)
                            
                            # Poll for completion
                            job_status = self.poll_export_job(job_id)
                            
                            if job_status['JobStatus'] == 'SUCCESSFUL':
                                # Download and upload to S3
                                download_url = job_status['DownloadUrl']
                                s3_key = self.generate_s3_key(asset_type, datetime.now(), bundle_number)
                                
                                if self.download_and_upload_bundle(download_url, s3_key):
                                    chunk_processed = len(arn_chunk)
                                    processed += chunk_processed
                                    bundle_info = f" (bundle {bundle_number})" if bundle_number else ""
                                    logger.info(f"Successfully backed up {chunk_processed} {asset_type}{bundle_info} to {s3_key}")
                                else:
                                    chunk_failed = len(arn_chunk)
                                    failed += chunk_failed
                                    logger.error(f"Failed to upload {asset_type} bundle {bundle_number or 1} to S3")
                                # Break to avoid retry
                                break
                            else:
                                chunk_failed = len(arn_chunk)
                                failed += chunk_failed
                                logger.error(f"Export job failed for {asset_type} bundle {bundle_number or 1}: {job_status.get('Errors', [])}")
                                break
                        except ClientError as e:
                            error_code = e.response['Error']['Code']
                            error_message = e.response['Error']['Message']
                            
                            if error_code == 'ThrottlingException' and attempt < max_retries:
                                # Calculate exponential backoff delay with jitter
                                delay = min(base_delay * (2 ** attempt), max_delay)
                                # Add jitter (random factor between 0.5 and 1.5)
                                jitter = random.uniform(0.5, 1.5)
                                actual_delay = delay * jitter
                                
                                logger.warning(f"Export job throttled, retrying in {actual_delay:.2f} seconds (attempt {attempt + 1}/{max_retries + 1})")
                                time.sleep(actual_delay)
                                continue
                            else:
                                # Non-throttling error or max retries exceeded
                                if error_code == 'ThrottlingException':
                                    logger.error(f"Export job failed after {max_retries + 1} attempts due to throttling")
                                else:
                                    logger.error(f"Failed to start export job: {error_code} - {error_message}")
                                raise QuickSightBackupError(f"Failed to start export job: {error_code} - {error_message}")  
                
                except Exception as e:
                    chunk_failed = len(arn_chunk)
                    failed += chunk_failed
                    logger.error(f"Failed to backup {asset_type} bundle {bundle_number or 1}: {str(e)}")
        
        except Exception as e:
            logger.error(f"Failed to backup {asset_type}: {str(e)}")
            failed = len(assets)
        
        return processed, failed
    
    def start_export_job(self, asset_arns: List[str], asset_type: str, bundle_number: Optional[int] = None, attempt: Optional[int] = 0, max_retries: Optional[int] = 5) -> str:
        
        try:
            # Prepare export job request
            job_id_suffix = f"bundle-{bundle_number}-{int(time.time())}" if bundle_number else str(int(time.time()))
            export_job_config = {
                'AwsAccountId': self.config.aws_account_id,
                'AssetBundleExportJobId': f"{asset_type}-{job_id_suffix}",
                'ResourceArns': asset_arns,
                'ExportFormat': self.config.export_format,
                'IncludeAllDependencies': self.config.include_dependencies,
                'IncludePermissions': self.config.include_permissions,
                'IncludeTags': self.config.include_tags
            }
            
            if attempt > 0:
                logger.info(f"Retrying export job for {len(asset_arns)} {asset_type} (attempt {attempt + 1}/{max_retries + 1})")
            else:
                logger.info(f"Starting export job for {len(asset_arns)} {asset_type}")
            
            response = self.quicksight_client.start_asset_bundle_export_job(**export_job_config)
            
            job_id = response['AssetBundleExportJobId']
            logger.info(f"Export job started with ID: {job_id}")
            
            return job_id
        
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            logger.error(f"Failed to start export job: {error_code} - {error_message}")
            raise QuickSightBackupError(f"Failed to start export job: {error_code} - {error_message}")
        
        # This should never be reached, but just in case
        raise QuickSightBackupError("Failed to start export job after all retry attempts")
    
    def poll_export_job(self, job_id: str, max_wait_time: int = 1200) -> Dict[str, Any]:
        """Poll export job until completion or timeout."""
        start_time = time.time()
        poll_interval = 30  # Start with 30 seconds
        max_poll_interval = max_wait_time  # Max 20 minutes
        
        logger.info(f"Polling export job {job_id} for completion...")
        
        while time.time() - start_time < max_wait_time:
            try:
                response = self.quicksight_client.describe_asset_bundle_export_job(
                    AwsAccountId=self.config.aws_account_id,
                    AssetBundleExportJobId=job_id
                )
                
                job_status = response['JobStatus']
                logger.debug(f"Export job {job_id} status: {job_status}")
                
                if job_status in ['SUCCESSFUL', 'FAILED']:
                    logger.info(f"Export job {job_id} completed with status: {job_status}")
                    return response
                
                elif job_status in ['QUEUED_FOR_IMMEDIATE_EXECUTION', 'IN_PROGRESS']:
                    # Continue polling
                    time.sleep(poll_interval)
                    logger.info(f"Export job {job_id} is running with status: {job_status}")
                    # Gradually increase poll interval to reduce API calls
                    poll_interval = min(poll_interval * 1.2, max_poll_interval)
                
                else:
                    logger.warning(f"Unexpected job status: {job_status}")
                    time.sleep(poll_interval)
            
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == 'ResourceNotFoundException':
                    raise QuickSightBackupError(f"Export job {job_id} not found")
                if error_code == 'ThrottlingException':
                    logger.warning(f"Throttling detected while polling job {job_id} with status: {job_status}")
                    raise e
                else:
                    logger.error(f"Error polling export job: {error_code}")
                    time.sleep(poll_interval)
        
        # Timeout reached
        raise QuickSightBackupError(f"Export job {job_id} timed out after {max_wait_time} seconds")
    
    def generate_s3_key(self, asset_type: str, timestamp: datetime, bundle_number: Optional[int] = None) -> str:
        """Generate S3 key with configurable prefix and date-based organization."""
        # Format timestamp according to configured prefix format
        if self.config.s3_prefix_format == "YYYY/MM/DD":
            date_prefix = timestamp.strftime("%Y/%m/%d")
        elif self.config.s3_prefix_format == "YYYY-MM-DD":
            date_prefix = timestamp.strftime("%Y-%m-%d")
        elif self.config.s3_prefix_format == "YYYYMMDD":
            date_prefix = timestamp.strftime("%Y%m%d")
        else:
            # Default fallback
            date_prefix = timestamp.strftime("%Y/%m/%d")
        
        # Generate unique filename
        timestamp_str = timestamp.strftime("%H%M%S")
        if bundle_number:
            filename = f"{asset_type}_bundle_{bundle_number}-{timestamp_str}.zip"
        else:
            filename = f"{asset_type}-{timestamp_str}.zip"
        
        # Combine custom prefix with date prefix and filename
        return f"{self.config.s3_prefix}/{date_prefix}/{asset_type}/{filename}"
    
    def download_and_upload_bundle(self, download_url: str, s3_key: str) -> bool:
        """Download asset bundle from QuickSight and upload to S3."""
        import requests
        import tempfile
        import os
        
        temp_file = None
        try:
            logger.info(f"Downloading asset bundle from QuickSight...")
            
            # Download the asset bundle
            response = requests.get(download_url, stream=True, timeout=300)
            response.raise_for_status()
            
            # Create temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as temp_file:
                # Stream download to avoid memory issues with large files
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        temp_file.write(chunk)
                
                temp_file_path = temp_file.name
            
            # Get file size for logging
            file_size = os.path.getsize(temp_file_path)
            logger.info(f"Downloaded asset bundle ({file_size} bytes)")
            
            # Upload to S3
            logger.info(f"Uploading asset bundle to S3: s3://{self.config.s3_bucket_name}/{s3_key}")
            
            # Use multipart upload for large files (>100MB)
            if file_size > 100 * 1024 * 1024:
                self._multipart_upload_to_s3(temp_file_path, s3_key, file_size)
            else:
                self._simple_upload_to_s3(temp_file_path, s3_key)
            
            logger.info(f"Successfully uploaded asset bundle to S3")
            return True
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download asset bundle: {str(e)}")
            return False
        
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            logger.error(f"S3 upload failed: {error_code} - {error_message}")
            return False
        
        except Exception as e:
            logger.error(f"Unexpected error during download/upload: {str(e)}")
            return False
        
        finally:
            # Clean up temporary file
            if temp_file and os.path.exists(temp_file_path):
                try:
                    os.unlink(temp_file_path)
                    logger.debug("Cleaned up temporary file")
                except Exception as e:
                    logger.warning(f"Failed to clean up temporary file: {str(e)}")
    
    def _simple_upload_to_s3(self, file_path: str, s3_key: str) -> None:
        """Upload file to S3 using simple upload."""
        with open(file_path, 'rb') as file_obj:
            self.s3_client.upload_fileobj(
                file_obj,
                self.config.s3_bucket_name,
                s3_key,
                ExtraArgs={
                    'ServerSideEncryption': 'AES256',
                    'Metadata': {
                        'backup-tool': 'quicksight-backup',
                        'backup-type': 'asset-bundle',
                        'backup-timestamp': datetime.now().isoformat()
                    }
                }
            )
    
    def _multipart_upload_to_s3(self, file_path: str, s3_key: str, file_size: int) -> None:
        """Upload large file to S3 using multipart upload."""
        # Initialize multipart upload
        response = self.s3_client.create_multipart_upload(
            Bucket=self.config.s3_bucket_name,
            Key=s3_key,
            ServerSideEncryption='AES256',
            Metadata={
                'backup-tool': 'quicksight-backup',
                'backup-type': 'asset-bundle',
                'backup-timestamp': datetime.now().isoformat()
            }
        )
        
        upload_id = response['UploadId']
        parts = []
        part_size = 100 * 1024 * 1024  # 100MB parts
        
        try:
            with open(file_path, 'rb') as file_obj:
                part_number = 1
                
                while True:
                    data = file_obj.read(part_size)
                    if not data:
                        break
                    
                    logger.debug(f"Uploading part {part_number} ({len(data)} bytes)")
                    
                    part_response = self.s3_client.upload_part(
                        Bucket=self.config.s3_bucket_name,
                        Key=s3_key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=data
                    )
                    
                    parts.append({
                        'ETag': part_response['ETag'],
                        'PartNumber': part_number
                    })
                    
                    part_number += 1
            
            # Complete multipart upload
            self.s3_client.complete_multipart_upload(
                Bucket=self.config.s3_bucket_name,
                Key=s3_key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
            
            logger.info(f"Completed multipart upload with {len(parts)} parts")
        
        except Exception as e:
            # Abort multipart upload on error
            try:
                self.s3_client.abort_multipart_upload(
                    Bucket=self.config.s3_bucket_name,
                    Key=s3_key,
                    UploadId=upload_id
                )
                logger.info("Aborted multipart upload due to error")
            except Exception as abort_error:
                logger.warning(f"Failed to abort multipart upload: {str(abort_error)}")
            
            raise e