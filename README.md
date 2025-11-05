# QuickSight Backup Tool

A comprehensive backup solution for Amazon QuickSight resources including users, groups, datasources, datasets, analyses, and dashboards.

## ⚠️ Important Disclaimer

**This code is provided as an example implementation for educational and inspirational purposes.** It demonstrates one approach to implementing a QuickSight backup strategy using AWS APIs and services. Before deploying this solution in a production environment, you should:

- **Review and understand** all code components and their implications for your specific environment
- **Adapt the implementation** to meet your organization's security, compliance, and operational requirements
- **Conduct thorough testing** in a non-production environment that mirrors your production setup
- **Implement appropriate monitoring, alerting, and error handling** for your operational needs
- **Ensure compliance** with your industry regulations and internal policies regarding data backup and retention
- **Consider additional security measures** such as encryption at rest, access controls, and audit logging
- **Validate backup integrity** and test restore procedures regularly
- **Review AWS service limits** and costs associated with your specific usage patterns

This tool serves as a starting point and reference implementation. Production deployments should be thoroughly reviewed by your security, compliance, and operations teams before implementation.

## Overview

The QuickSight Backup Tool provides automated backup capabilities for your Amazon QuickSight environment, helping you maintain disaster recovery capabilities and facilitate migration scenarios. The tool uses AWS APIs to export and backup all critical QuickSight components to DynamoDB (for users/groups) and S3 (for asset bundles).

## Features

- **Complete Resource Coverage**: Backup users, groups, datasources, datasets, analyses, and dashboards
- **Dual Storage Strategy**: DynamoDB for user/group metadata, S3 for asset bundles
- **Flexible Backup Modes**: Full backup, users-only, or assets-only
- **Configurable Bundle Sizing**: Control asset bundle sizes (1-100 assets per bundle) for optimal performance
- **Automatic Bundle Chunking**: Large asset collections are automatically split into multiple bundles
- **Robust Error Handling**: Comprehensive error handling with detailed logging
- **Progress Tracking**: Real-time progress indicators and detailed reports
- **Configurable Storage**: Customizable table names and S3 bucket configuration
- **Date-based Organization**: Automatic YYYY/MM/DD prefix structure in S3
- **Historical Backup Preservation**: Date-prefixed DynamoDB table names preserve all previous backups

## Installation

### Prerequisites

- Python 3.8 or higher
- AWS CLI configured with appropriate credentials
- Required AWS permissions (see [Permissions](#permissions) section)

### Clone from source

```bash
git clone https://github.com/quicksight-backup/quicksight-backup-tool.git
cd quicksight-backup-tool
```

### Create a python venv (recommended)

```bash
python3 -m venv ./.venv
source .venv/bin/activate
```

### Install package
```bash
pip install -e .
```


### Development Installation

```bash
git clone https://github.com/quicksight-backup/quicksight-backup-tool.git
cd quicksight-backup-tool
pip install -e ".[dev]"
```

## Quick Start

1. **Create a configuration file** (see [Configuration](#configuration) section):

```yaml
# config.yaml
aws:
  region: us-east-1
  account_id: "123456789012"

dynamodb:
  users_table_name: "quicksight-users-backup"
  groups_table_name: "quicksight-groups-backup"
  users_group_table_name: "quicksight-users-groups-backup"

s3:
  bucket_name: "my-quicksight-backups"
  prefix_format: "YYYY/MM/DD"
  prefix: "quicksight-backups"

backup:
  include_dependencies: true
  include_permissions: true
  include_tags: true
  export_format: "QUICKSIGHT_JSON"
  max_assets_per_bundle: 50

logging:
  level: "INFO"
  file_path: "./logs/backup.log"
```

2. **Run the backup**:

```bash
quicksight-backup --config config.yaml
```

## Usage

### Command Line Interface

```bash
quicksight-backup [OPTIONS]
```

#### Required Arguments

- `--config, -c`: Path to configuration file (YAML or JSON format)

#### Optional Arguments

- `--mode, -m`: Backup mode (`full`, `users-only`, `assets-only`) [default: full]
- `--output-dir, -o`: Output directory for reports and manifests
- `--verbose, -v`: Enable verbose (DEBUG) logging
- `--log-file`: Path to log file
- `--dry-run`: Validate configuration without executing backup
- `--no-progress`: Disable progress indicators
- `--generate-manifest`: Generate backup manifest file
- `--generate-report`: Generate human-readable backup report
- `--version`: Show version information

#### Examples

```bash
# Full backup with verbose logging
quicksight-backup --config config.yaml --verbose

# Backup only users and groups
quicksight-backup --config config.yaml --mode users-only

# Backup only assets (datasources, datasets, analyses, dashboards)
quicksight-backup --config config.yaml --mode assets-only

# Dry run to validate configuration
quicksight-backup --config config.yaml --dry-run

# Save output to specific directory
quicksight-backup --config config.yaml --output-dir ./backups

# Log to file with progress disabled (for automation)
quicksight-backup --config config.yaml --log-file backup.log --no-progress

# Example output with bundle chunking
# INFO: Split 75 datasources into 3 bundles (max 25 per bundle)
# INFO: Successfully backed up 25 datasources (bundle 1) to 2024/01/15/datasources_bundle_1-143022.zip
# INFO: Successfully backed up 25 datasources (bundle 2) to 2024/01/15/datasources_bundle_2-143022.zip  
# INFO: Successfully backed up 25 datasources (bundle 3) to 2024/01/15/datasources_bundle_3-143022.zip
```

## Configuration

The tool supports both YAML and JSON configuration formats. Below is a complete configuration reference:

### YAML Configuration

```yaml
# AWS Configuration
aws:
  region: us-east-1                    # AWS region for assets (datasources, datasets, analyses, dashboards)
  identity_region: us-east-1           # AWS region for users and groups (optional, defaults to region)
  account_id: "123456789012"           # AWS account ID

# DynamoDB Configuration
dynamodb:
  users_table_name: "quicksight-users-backup"           # Base table name for user data
  groups_table_name: "quicksight-groups-backup"         # Base table name for group data
  users_group_table_name: "quicksight-users-groups-backup"  # Base table name for user-group memberships

# S3 Configuration
s3:
  bucket_name: "my-quicksight-backups"  # S3 bucket for asset bundles
  prefix_format: "YYYY/MM/DD"          # Date-based prefix structure
  prefix: "quicksight-backups"         # Custom S3 prefix for organizing asset bundles

# Backup Options
backup:
  include_dependencies: true           # Include asset dependencies
  include_permissions: true            # Include sharing permissions
  include_tags: true                   # Include resource tags
  export_format: "QUICKSIGHT_JSON"     # Export format (QUICKSIGHT_JSON only)
  max_assets_per_bundle: 50            # Maximum assets per bundle (1-100), defaults to 50

# Logging Configuration
logging:
  level: "INFO"                        # Log level (DEBUG, INFO, WARNING, ERROR)
  file_path: "./logs/backup.log"       # Log file path (optional)
```

### JSON Configuration

```json
{
  "aws": {
    "region": "us-east-1",
    "identity_region": "us-east-1",
    "account_id": "123456789012"
  },
  "dynamodb": {
    "users_table_name": "quicksight-users-backup",
    "groups_table_name": "quicksight-groups-backup",
    "users_group_table_name": "quicksight-users-groups-backup"
  },
  "s3": {
    "bucket_name": "my-quicksight-backups",
    "prefix_format": "YYYY/MM/DD",
    "prefix": "quicksight-backups"
  },
  "backup": {
    "include_dependencies": true,
    "include_permissions": true,
    "include_tags": true,
    "export_format": "QUICKSIGHT_JSON",
    "max_assets_per_bundle": 50
  },
  "logging": {
    "level": "INFO",
    "file_path": "./logs/backup.log"
  }
}
```

## Bundle Configuration and Performance

### Asset Bundle Sizing

The `max_assets_per_bundle` parameter controls how many assets are included in each export bundle. This setting affects backup performance, reliability, and AWS API limits.

#### Recommended Values

| Environment Type | Recommended Value | Rationale |
|------------------|-------------------|-----------|
| **Development** | 10-20 | Faster iterations, easier debugging |
| **Production** | 30-40 | Balance between performance and reliability |
| **Enterprise** | 20-30 | Conservative approach for large environments |
| **Lambda** | 15-25 | Account for Lambda timeout constraints |
| **Cross-Account** | 35-45 | Optimize for network latency |

#### Performance Considerations

**Smaller Bundles (1-25 assets):**
- ✅ Faster export job completion
- ✅ Lower memory usage
- ✅ Better error isolation
- ❌ More API calls and S3 objects
- ❌ Increased backup time for large environments

**Larger Bundles (50-100 assets):**
- ✅ Fewer API calls and S3 objects
- ✅ Faster overall backup for large environments
- ❌ Longer export job times
- ❌ Higher memory usage
- ❌ Larger blast radius for failures

#### Bundle Size Guidelines

```yaml
# For environments with < 100 total assets
backup:
  max_assets_per_bundle: 50    # Single bundle for most asset types

# For environments with 100-500 total assets  
backup:
  max_assets_per_bundle: 30    # 2-4 bundles per asset type

# For environments with > 500 total assets
backup:
  max_assets_per_bundle: 25    # Multiple bundles, optimized for reliability
```

#### Troubleshooting Bundle Issues

**Export Job Timeouts:**
```
Asset Bundle Error: Export job timed out after 300 seconds
```
**Solution**: Reduce `max_assets_per_bundle` to 20-30

**Too Many S3 Objects:**
```
Warning: Created 50+ bundles for datasources
```
**Solution**: Increase `max_assets_per_bundle` to 40-60

**Memory Issues in Lambda:**
```
Lambda Error: Task timed out after 15.00 seconds
```
**Solution**: Reduce `max_assets_per_bundle` to 15-20

## Permissions

The tool requires the following AWS IAM permissions:

### QuickSight Permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "quicksight:ListUsers",
        "quicksight:ListGroups",
        "quicksight:ListDataSources",
        "quicksight:ListDataSets",
        "quicksight:ListAnalyses",
        "quicksight:ListDashboards",
        "quicksight:StartAssetBundleExportJob",
        "quicksight:DescribeAssetBundleExportJob"
      ],
      "Resource": "*"
    }
  ]
}
```

### DynamoDB Permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:CreateTable",
        "dynamodb:PutItem",
        "dynamodb:BatchWriteItem",
        "dynamodb:DescribeTable"
      ],
      "Resource": [
        "arn:aws:dynamodb:*:*:table/*quicksight-users-backup*",
        "arn:aws:dynamodb:*:*:table/*quicksight-groups-backup*",
        "arn:aws:dynamodb:*:*:table/*quicksight-users-groups-backup*"
      ]
    }
  ]
}
```

### S3 Permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:PutObjectAcl",
        "s3:GetObject"
      ],
      "Resource": "arn:aws:s3:::my-quicksight-backups/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket"
      ],
      "Resource": "arn:aws:s3:::my-quicksight-backups"
    }
  ]
}
```

## Date Prefix Functionality

### Historical Backup Preservation

The tool automatically creates date-prefixed DynamoDB table names to preserve historical backups. Each backup run creates new tables with the current date as a prefix, ensuring that previous backups are never overwritten.

#### Table Naming Convention

Tables are created with the format: `YYYY-MM-DD-{base_table_name}`

**Example for a backup run on 2025-10-19:**
- Users: `2025-10-19-quicksight-users-backup`
- Groups: `2025-10-19-quicksight-groups-backup`
- User-Group Memberships: `2025-10-19-quicksight-users-groups-backup`

#### Benefits

- **Point-in-Time Recovery**: Access complete backups from any specific date
- **Change Tracking**: Compare user/group configurations across different dates
- **Rollback Capability**: Restore from any previous backup without data loss
- **Audit Trail**: Maintain historical records for compliance and analysis

#### Configuration Impact

The `prefix_format` setting in the S3 configuration is used to determine the date format, but DynamoDB table names always use the `YYYY-MM-DD` format for compatibility with DynamoDB naming requirements.

```yaml
s3:
  prefix_format: "YYYY/MM/DD"  # Can be YYYY/MM/DD, YYYY-MM-DD, or YYYYMMDD
```

All formats are converted to `YYYY-MM-DD` for DynamoDB table names to ensure:
- No special characters (like `/`) that are invalid in table names
- Consistent naming across all backup runs
- Proper sorting and organization of historical backups

#### Table Management

**Important**: Each backup run creates new DynamoDB tables. Consider implementing a cleanup policy to manage costs:

```bash
# Example: List all backup tables
aws dynamodb list-tables --query 'TableNames[?contains(@, `quicksight-`) && contains(@, `-backup`)]'

# Example: Delete old backup tables (be careful!)
aws dynamodb delete-table --table-name 2025-10-01-quicksight-users-backup
```

## Output Structure

### DynamoDB Tables

#### Users Table Schema
```json
{
  "user_name": "string (partition key)",
  "arn": "string",
  "email": "string", 
  "role": "string",
  "identity_type": "string",
  "active": "boolean",
  "principal_id": "string",
  "backup_timestamp": "string (ISO 8601)",
  "custom_permissions_name": "string"
}
```

#### Groups Table Schema
```json
{
  "group_name": "string (partition key)",
  "arn": "string",
  "description": "string",
  "principal_id": "string", 
  "members": ["list of user names"],
  "backup_timestamp": "string (ISO 8601)"
}
```

#### User-Group Memberships Table Schema
```json
{
  "membership_id": "string (partition key, format: user_name#group_name)",
  "user_name": "string",
  "group_name": "string",
  "user_arn": "string",
  "group_arn": "string",
  "backup_timestamp": "string (ISO 8601)"
}
```

### S3 Structure

```
my-quicksight-backups/
└── quicksight-backups/                          # Custom S3 prefix
    ├── 2024/01/15/
    │   ├── datasources/
    │   │   ├── datasources-143022.zip                # Single bundle (≤ max_assets_per_bundle)
    │   │   └── datasources_bundle_1-143045.zip       # Multiple bundles when assets exceed limit
    │   ├── datasets/
    │   │   ├── datasets_bundle_1-143045.zip          # Multiple bundles when assets exceed limit
    │   │   └── datasets_bundle_2-143045.zip          # Sequential numbering for multiple bundles
    │   ├── analyses/
    │   │   └── analyses-143108.zip                   # Single bundle
    │   └── dashboards/
    │       ├── dashboards_bundle_1-143131.zip        # First of multiple dashboard bundles
    │       └── dashboards_bundle_2-143131.zip        # Second dashboard bundle
    └── 2024/01/16/
        ├── datasources/
        │   └── datasources-090015.zip
        ├── datasets/
        │   └── datasets-090030.zip
        └── ...
```

**S3 Key Structure:**
- **Path Format**: `{custom_prefix}/{date_prefix}/{asset_type}/{filename}`
- **Single Bundle**: `{asset_type}-{timestamp}.zip` (when assets ≤ max_assets_per_bundle)
- **Multiple Bundles**: `{asset_type}_bundle_{number}-{timestamp}.zip` (when assets > max_assets_per_bundle)

**Example S3 Keys:**
- `quicksight-backups/2024/01/15/datasources/datasources-143022.zip`
- `quicksight-backups/2024/01/15/datasets/datasets_bundle_1-143045.zip`
- `quicksight-backups/2024/01/15/analyses/analyses-143108.zip`

### Reports and Manifests

The tool generates two types of output files:

1. **Backup Manifest** (`backup_manifest_YYYYMMDD_HHMMSS.json`): Machine-readable JSON file listing all backed up resources
2. **Backup Report** (`backup_report_YYYYMMDD_HHMMSS.txt`): Human-readable summary with statistics and any errors

## Troubleshooting

### Common Issues

#### 1. Configuration Errors

**Error**: `Configuration file does not exist`
```bash
quicksight-backup --config config.yaml
# Error: Configuration file does not exist: config.yaml
```

**Solution**: Ensure the configuration file path is correct and the file exists.

**Error**: `Configuration file must be YAML or JSON`
```bash
quicksight-backup --config config.txt
# Error: Configuration file must be YAML or JSON: config.txt
```

**Solution**: Use a `.yaml`, `.yml`, or `.json` file extension.

#### 2. AWS Credentials Issues

**Error**: `Unable to locate credentials`
```
AWS Credentials Error: Unable to locate credentials. You can configure credentials by running "aws configure".
```

**Solutions**:
- Run `aws configure` to set up credentials
- Set environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
- Use IAM roles if running on EC2/Lambda
- Configure AWS profiles: `aws configure --profile myprofile`

**Error**: `Access Denied`
```
AWS Credentials Error: An error occurred (AccessDenied) when calling the ListUsers operation
```

**Solution**: Ensure your AWS credentials have the required permissions (see [Permissions](#permissions) section).

#### 3. QuickSight API Issues

**Error**: `User is not registered in QuickSight`
```
QuickSight Error: User: arn:aws:iam::123456789012:user/myuser is not registered in QuickSight
```

**Solution**: Register the user in QuickSight or use credentials for a registered QuickSight user.

**Error**: `Rate exceeded`
```
QuickSight Error: Rate exceeded for operation: ListUsers
```

**Solution**: The tool automatically handles rate limiting with exponential backoff. If this persists, try running during off-peak hours.

#### 4. DynamoDB Issues

**Error**: `Table already exists`
```
DynamoDB Error: Table already exists: quicksight-users-backup
```

**Solution**: This is expected behavior. The tool will use the existing table. Ensure the table schema matches expectations.

**Error**: `Requested resource not found`
```
DynamoDB Error: Requested resource not found: Table: quicksight-users-backup not found
```

**Solution**: The tool will automatically create the table. Ensure you have `dynamodb:CreateTable` permissions.

**Error**: `Too many tables created`
```
DynamoDB Warning: Multiple date-prefixed tables detected for base table: quicksight-users-backup
```

**Solution**: This is expected behavior with date-prefixed tables. Each backup run creates new tables. To manage costs, consider implementing a cleanup policy for old backup tables.

#### 5. S3 Issues

**Error**: `The specified bucket does not exist`
```
S3 Error: The specified bucket does not exist: my-quicksight-backups
```

**Solution**: Create the S3 bucket manually or ensure the bucket name is correct in your configuration.

**Error**: `Access Denied`
```
S3 Error: An error occurred (AccessDenied) when calling the PutObject operation
```

**Solution**: Ensure your AWS credentials have `s3:PutObject` permissions for the specified bucket.

#### 6. Asset Bundle Export Issues

**Error**: `Export job failed`
```
Asset Bundle Error: Export job failed with status: FAILED
```

**Solutions**:
- Check if the assets exist and are accessible
- Verify that FILE datasets are properly excluded
- Ensure assets don't have circular dependencies
- Check CloudTrail logs for detailed error information

**Error**: `No assets found to export`
```
Asset Bundle Warning: No assets found to export for type: DASHBOARD
```

**Solution**: This is normal if you don't have any resources of that type. The backup will continue with other resource types.

#### 7. Bundle Configuration Issues

**Error**: `Invalid max_assets_per_bundle value`
```
Configuration Error: max_assets_per_bundle must be between 1 and 100 inclusive
```

**Solution**: Set `max_assets_per_bundle` to a value between 1 and 100 in your configuration file.

**Error**: `Export job failed with large bundle`
```
Asset Bundle Error: Export job failed - bundle too large
```

**Solutions**:
- Reduce `max_assets_per_bundle` to 20-30
- Check for assets with large dependencies
- Verify network connectivity for large downloads

**Warning**: `Creating many small bundles`
```
Warning: Split 200 assets into 20 bundles (max 10 per bundle)
```

**Solution**: Consider increasing `max_assets_per_bundle` to 25-40 to reduce the number of bundles and API calls.

### Debug Mode

Enable debug logging for detailed troubleshooting:

```bash
quicksight-backup --config config.yaml --verbose --log-file debug.log
```

This will provide detailed information about:
- API calls and responses
- Configuration validation
- Asset discovery process
- Export job status polling
- Error details and stack traces

### Dry Run Mode

Test your configuration without making changes:

```bash
quicksight-backup --config config.yaml --dry-run
```

This will:
- Validate configuration file syntax
- Test AWS connectivity and permissions
- Verify DynamoDB and S3 access
- Validate bundle configuration (max_assets_per_bundle range)
- Report any issues without executing the backup

### Getting Help

If you encounter issues not covered here:

1. Enable debug logging and check the log file
2. Run in dry-run mode to validate configuration
3. Check AWS CloudTrail logs for API-level errors
4. Review the [GitHub Issues](https://github.com/quicksight-backup/quicksight-backup-tool/issues)
5. Create a new issue with:
   - Configuration file (remove sensitive data)
   - Complete error message
   - Debug log output
   - AWS region and QuickSight setup details

## Development

### Setting up Development Environment

```bash
git clone https://github.com/quicksight-backup/quicksight-backup-tool.git
cd quicksight-backup-tool
pip install -e ".[dev]"
```

### Code Quality

```bash
# Format code
black quicksight_backup/

# Sort imports
isort quicksight_backup/

# Lint code
flake8 quicksight_backup/

# Type checking
mypy quicksight_backup/
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for a list of changes and version history.

## Support

- **Documentation**: [GitHub Wiki](https://github.com/quicksight-backup/quicksight-backup-tool/wiki)
- **Issues**: [GitHub Issues](https://github.com/quicksight-backup/quicksight-backup-tool/issues)
- **Discussions**: [GitHub Discussions](https://github.com/quicksight-backup/quicksight-backup-tool/discussions)