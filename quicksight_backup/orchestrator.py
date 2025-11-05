"""
Main backup orchestrator for QuickSight backup operations.
"""

import logging
import time
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path

from quicksight_backup.config.manager import ConfigurationManager
from quicksight_backup.models.config import BackupConfig
from quicksight_backup.models.backup_result import BackupResult, BackupReport, BackupStatus
from quicksight_backup.services.user_group_backup import UserGroupBackupService
from quicksight_backup.services.asset_bundle_backup import AssetBundleBackupService
from quicksight_backup.services.logging import LoggingService
from quicksight_backup.models.exceptions import (
    ConfigurationError,
    AWSCredentialsError,
    QuickSightBackupError
)


class QuickSightBackupOrchestrator:
    """Main orchestrator class that coordinates all backup services."""
    
    def __init__(self, args: {}):
        """
        Initialize the backup orchestrator.
        
        Args:
            config_path: Path to the configuration file
        """
        self.config_path = args.config
        self.mode = args.mode
        self.config: Optional[BackupConfig] = None
        self.config_manager: Optional[ConfigurationManager] = None
        self.logging_service: Optional[LoggingService] = None
        self.logger = logging.getLogger(__name__)
        
        # Services
        self.user_group_service: Optional[UserGroupBackupService] = None
        self.asset_bundle_service: Optional[AssetBundleBackupService] = None
        
        # Results
        self.backup_results: List[BackupResult] = []
        self.backup_report: Optional[BackupReport] = None
    
    def initialize(self) -> bool:
        """
        Initialize the orchestrator with configuration and services.
        
        Returns:
            bool: True if initialization successful
            
        Raises:
            ConfigurationError: If configuration loading fails
            AWSCredentialsError: If AWS connectivity validation fails
        """
        try:
            self.logger.info("Initializing QuickSight Backup Orchestrator")
            
            # Load configuration
            self.config_manager = ConfigurationManager()
            self.config = self.config_manager.load_config(self.config_path)
            
            # Initialize logging service
            self.logging_service = LoggingService(self.config)
            
            # Validate AWS connectivity
            self.config_manager.validate_aws_connectivity(self.config)
            
            # Initialize backup services
            self.user_group_service = UserGroupBackupService(self.config)
            self.asset_bundle_service = AssetBundleBackupService(self.config)
            
            # Validate prerequisites for each service
            if not self.user_group_service.validate_prerequisites():
                raise QuickSightBackupError("User/Group backup service prerequisites validation failed")
            
            if not self.asset_bundle_service.validate_prerequisites():
                raise QuickSightBackupError("Asset bundle backup service prerequisites validation failed")
            
            self.logger.info("Orchestrator initialization completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Orchestrator initialization failed: {str(e)}")
            raise
    
    def execute_backup(self) -> BackupReport:
        """
        Execute the complete backup workflow.
        
        Returns:
            BackupReport: Comprehensive backup report
            
        Raises:
            QuickSightBackupError: If backup execution fails
        """
        if not self.config:
            raise QuickSightBackupError("Orchestrator not initialized. Call initialize() first.")
        
        start_time = datetime.now()
        self.backup_results = []
        
        try:
            self.logger.info("Starting QuickSight backup workflow")
            self.logging_service.log_info("Backup workflow started", {
                'config_path': self.config_path,
                'aws_account_id': self.config.aws_account_id,
                'aws_region': self.config.aws_region
            })
            
            if self.mode == 'full' or self.mode == 'users-only':
                # Step 1: Backup users and groups
                self.logger.info("Step 1: Executing user and group backup")
                user_group_result = self._execute_user_group_backup()
                self.backup_results.append(user_group_result)
            
            # Step 2: Backup asset bundles
            if self.mode == 'full' or self.mode == 'assets-only':
                self.logger.info("Step 2: Executing asset bundle backup")
                asset_bundle_result = self._execute_asset_bundle_backup()
                self.backup_results.append(asset_bundle_result)
            
            # Generate backup report
            end_time = datetime.now()
            self.backup_report = self._generate_backup_report(start_time, end_time)
            
            # Log completion
            self.logging_service.log_info("Backup workflow completed", {
                'total_execution_time': self.backup_report.total_execution_time,
                'success_rate': self.backup_report.success_rate,
                'total_resources': self.backup_report.total_resources
            })
            
            self.logger.info(f"Backup workflow completed. Success rate: {self.backup_report.success_rate:.1f}%")
            
            return self.backup_report
            
        except Exception as e:
            self.logger.error(f"Backup workflow failed: {str(e)}")
            self.logging_service.log_error("Backup workflow failed", e, {
                'config_path': self.config_path
            })
            
            # Generate partial report even on failure
            end_time = datetime.now()
            self.backup_report = self._generate_backup_report(start_time, end_time)
            
            raise QuickSightBackupError(f"Backup workflow failed: {str(e)}")
    
    def _execute_user_group_backup(self) -> BackupResult:
        """
        Execute user and group backup operation.
        
        
        Returns:
            BackupResult: Result of user/group backup
        """
        try:
            self.logging_service.log_info("Starting user and group backup")
            
            # Execute backup
            result = self.user_group_service.backup()
            
            # Log result
            if result.success:
                self.logging_service.log_info("User and group backup completed successfully", {
                    'items_processed': result.items_processed,
                    'execution_time': result.execution_time
                })
            else:
                self.logging_service.log_error("User and group backup failed", None, {
                    'items_processed': result.items_processed,
                    'items_failed': result.items_failed,
                    'error_messages': result.error_messages
                })            
           
            
            return result
            
        except Exception as e:
            self.logger.error(f"User/group backup failed: {str(e)}")
            
            # Create failure result
            result = BackupResult(
                resource_type="users_and_groups",
                success=False,
                items_processed=0,
                items_failed=1,
                error_messages=[str(e)],
                execution_time=0.0,
                timestamp=datetime.now(),
                status=BackupStatus.FAILED
            )
            
            
            return result
    
    def _execute_asset_bundle_backup(self) -> BackupResult:
        """
        Execute asset bundle backup operation.
        
        Returns:
            BackupResult: Result of asset bundle backup
        """
        try:
            self.logging_service.log_info("Starting asset bundle backup")
            
            # Execute backup
            result = self.asset_bundle_service.backup()
            
            # Log result
            if result.success:
                self.logging_service.log_info("Asset bundle backup completed successfully", {
                    'items_processed': result.items_processed,
                    'execution_time': result.execution_time
                })
            else:
                self.logging_service.log_error("Asset bundle backup failed", None, {
                    'items_processed': result.items_processed,
                    'items_failed': result.items_failed,
                    'error_messages': result.error_messages
                })
            
            return result
            
        except Exception as e:
            self.logger.error(f"Asset bundle backup failed: {str(e)}")
            
            # Create failure result
            result = BackupResult(
                resource_type="asset_bundles",
                success=False,
                items_processed=0,
                items_failed=1,
                error_messages=[str(e)],
                execution_time=0.0,
                timestamp=datetime.now(),
                status=BackupStatus.FAILED
            )            
            
            
            return result
    
    def _generate_backup_report(self, start_time: datetime, end_time: datetime) -> BackupReport:
        """
        Generate comprehensive backup report.
        
        Args:
            start_time: Backup start time
            end_time: Backup end time
            
        Returns:
            BackupReport: Comprehensive backup report
        """
        total_execution_time = (end_time - start_time).total_seconds()
        
        # Count results by status
        successful_resources = sum(1 for r in self.backup_results if r.status == BackupStatus.SUCCESS)
        failed_resources = sum(1 for r in self.backup_results if r.status == BackupStatus.FAILED)
        partial_resources = sum(1 for r in self.backup_results if r.status == BackupStatus.PARTIAL)
        
        report = BackupReport(
            total_resources=len(self.backup_results),
            successful_resources=successful_resources,
            failed_resources=failed_resources,
            partial_resources=partial_resources,
            total_execution_time=total_execution_time,
            start_time=start_time,
            end_time=end_time,
            results=self.backup_results.copy()
        )
        
        return report 
   
    def generate_backup_manifest(self, output_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate backup manifest listing all backed up resources.
        
        Args:
            output_path: Optional path to save manifest file
            
        Returns:
            Dict[str, Any]: Backup manifest data
        """
        if not self.backup_report:
            raise QuickSightBackupError("No backup report available. Execute backup first.")
        
        manifest = {
            'backup_metadata': {
                'timestamp': self.backup_report.start_time.isoformat(),
                'tool_version': '1.0.0',
                'aws_account_id': self.config.aws_account_id,
                'aws_region': self.config.aws_region,
                'total_execution_time': self.backup_report.total_execution_time,
                'success_rate': self.backup_report.success_rate
            },
            'backup_summary': {
                'total_resources': self.backup_report.total_resources,
                'successful_resources': self.backup_report.successful_resources,
                'failed_resources': self.backup_report.failed_resources,
                'partial_resources': self.backup_report.partial_resources
            },
            'resource_details': []
        }
        
        # Add details for each backup result
        for result in self.backup_report.results:
            resource_detail = {
                'resource_type': result.resource_type,
                'status': result.status.value,
                'items_processed': result.items_processed,
                'items_failed': result.items_failed,
                'execution_time': result.execution_time,
                'timestamp': result.timestamp.isoformat(),
                'error_messages': result.error_messages,
                'metadata': result.metadata
            }
            
            # Add storage locations based on resource type
            if result.resource_type == "users_and_groups":
                resource_detail['storage_locations'] = {
                    'users_table': self.config.dynamodb_users_table,
                    'groups_table': self.config.dynamodb_groups_table
                }
            elif result.resource_type == "asset_bundles":
                resource_detail['storage_locations'] = {
                    's3_bucket': self.config.s3_bucket_name,
                    's3_prefix_format': self.config.s3_prefix_format
                }
            
            manifest['resource_details'].append(resource_detail)
        
        # Save manifest to file if path provided
        if output_path:
            self._save_manifest_to_file(manifest, output_path)
        
        return manifest
    
    def _save_manifest_to_file(self, manifest: Dict[str, Any], output_path: str) -> None:
        """
        Save backup manifest to JSON file.
        
        Args:
            manifest: Manifest data to save
            output_path: Path to save the manifest file
        """
        import json
        
        try:
            output_file = Path(output_path)
            
            # Create directory if it doesn't exist
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Write manifest to JSON file
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(manifest, f, indent=2, default=str)
            
            self.logger.info(f"Backup manifest saved to: {output_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to save backup manifest: {str(e)}")
            raise QuickSightBackupError(f"Failed to save backup manifest: {str(e)}")
    
    def generate_backup_report_summary(self) -> str:
        """
        Generate a human-readable backup report summary.
        
        Returns:
            str: Formatted backup report summary
        """
        if not self.backup_report:
            raise QuickSightBackupError("No backup report available. Execute backup first.")
        
        report_lines = [
            "=" * 60,
            "QuickSight Backup Report Summary",
            "=" * 60,
            f"Backup Date: {self.backup_report.start_time.strftime('%Y-%m-%d %H:%M:%S')}",
            f"AWS Account: {self.config.aws_account_id}",
            f"AWS Region: {self.config.aws_region}",
            f"Total Execution Time: {self.backup_report.total_execution_time:.2f} seconds",
            "",
            "Overall Results:",
            f"  Total Resources: {self.backup_report.total_resources}",
            f"  Successful: {self.backup_report.successful_resources}",
            f"  Failed: {self.backup_report.failed_resources}",
            f"  Partial: {self.backup_report.partial_resources}",
            f"  Success Rate: {self.backup_report.success_rate:.1f}%",
            "",
            "Resource Details:",
            "-" * 40
        ]
        
        for result in self.backup_report.results:
            status_symbol = "✓" if result.status == BackupStatus.SUCCESS else "✗" if result.status == BackupStatus.FAILED else "⚠"
            
            report_lines.extend([
                f"{status_symbol} {result.resource_type.replace('_', ' ').title()}:",
                f"    Status: {result.status.value}",
                f"    Items Processed: {result.items_processed}",
                f"    Items Failed: {result.items_failed}",
                f"    Execution Time: {result.execution_time:.2f}s"
            ])
            
            if result.error_messages:
                report_lines.append("    Errors:")
                for error in result.error_messages:
                    report_lines.append(f"      - {error}")
            
            report_lines.append("")
        
        # Add storage information
        report_lines.extend([
            "Storage Locations:",
            "-" * 40,
            f"DynamoDB Users Table: {self.config.dynamodb_users_table}",
            f"DynamoDB Groups Table: {self.config.dynamodb_groups_table}",
            f"S3 Bucket: {self.config.s3_bucket_name}",
            f"S3 Prefix Format: {self.config.s3_prefix_format}",
            "",
            "=" * 60
        ])
        
        return "\n".join(report_lines)
    
    def save_backup_report(self, output_path: str) -> None:
        """
        Save backup report summary to a text file.
        
        Args:
            output_path: Path to save the report file
        """
        try:
            report_summary = self.generate_backup_report_summary()
            
            output_file = Path(output_path)
            
            # Create directory if it doesn't exist
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Write report to file
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(report_summary)
            
            self.logger.info(f"Backup report saved to: {output_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to save backup report: {str(e)}")
            raise QuickSightBackupError(f"Failed to save backup report: {str(e)}")
    
    def get_backup_statistics(self) -> Dict[str, Any]:
        """
        Get backup statistics for monitoring and alerting.
        
        Returns:
            Dict[str, Any]: Backup statistics
        """
        if not self.backup_report:
            raise QuickSightBackupError("No backup report available. Execute backup first.")
        
        statistics = {
            'timestamp': self.backup_report.end_time.isoformat(),
            'success_rate': self.backup_report.success_rate,
            'total_execution_time': self.backup_report.total_execution_time,
            'total_resources': self.backup_report.total_resources,
            'successful_resources': self.backup_report.successful_resources,
            'failed_resources': self.backup_report.failed_resources,
            'partial_resources': self.backup_report.partial_resources,
            'resource_breakdown': {}
        }
        
        # Add breakdown by resource type
        for result in self.backup_report.results:
            statistics['resource_breakdown'][result.resource_type] = {
                'status': result.status.value,
                'items_processed': result.items_processed,
                'items_failed': result.items_failed,
                'execution_time': result.execution_time,
                'error_count': len(result.error_messages)
            }
        
        return statistics
    
    @property
    def is_initialized(self) -> bool:
        """Check if orchestrator is properly initialized."""
        return (
            self.config is not None and
            self.config_manager is not None and
            self.logging_service is not None and
            self.user_group_service is not None and
            self.asset_bundle_service is not None
        )
    
    @property
    def has_backup_results(self) -> bool:
        """Check if backup has been executed and results are available."""
        return self.backup_report is not None and len(self.backup_results) > 0