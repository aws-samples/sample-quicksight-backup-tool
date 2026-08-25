# Amazon Quick Sight Backup Tool

A comprehensive backup solution for Amazon Quick Sight resources including users, groups, datasources, datasets, analyses, and dashboards.

## ⚠️ Important Disclaimer

**This code is provided as an example implementation for educational and inspirational purposes.** It demonstrates one approach to implementing a Quick Sight backup strategy using AWS APIs and services. Before deploying this solution in a production environment, you should:

- **Review and understand** all code components and their implications for your specific environment
- **Adapt the implementation** to meet your organization's security, compliance, and operational requirements
- **Conduct thorough testing** in a non-production environment that mirrors your production setup
- **Implement appropriate monitoring, alerting, and error handling** for your operational needs
- **Help support compliance** with your industry regulations and internal policies regarding data backup and retention
- **Consider additional security measures** such as encryption at rest, access controls, and audit logging
- **Validate backup integrity** and test restore procedures regularly
- **Review AWS service limits** and costs associated with your specific usage patterns

This tool serves as a starting point and reference implementation. Production deployments should be thoroughly reviewed by your security, compliance, and operations teams before implementation.

## Introduction

Amazon Quick Sight does not provide a native backup mechanism for BI assets such as dashboards, analyses, datasets, and data sources. The Amazon Quick Sight Backup Tool addresses this gap by providing automated backup capabilities, enabling disaster recovery and migration scenarios for your Amazon Quick Sight environment.

## Overview

The Quick Sight Backup Tool provides automated backup capabilities for your Amazon Quick Sight environment, helping you maintain disaster recovery capabilities and facilitate migration scenarios. The tool uses AWS APIs to export and backup all critical Quick Sight components to DynamoDB (for users/groups) and S3 (for asset bundles).

## Features

- **Comprehensive Resource Coverage**: Backup users, groups, datasources, datasets, analyses, and dashboards
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
- Amazon Quick Sight Enterprise edition or higher

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

### Local web interface (optional)

The Streamlit UI requires Python 3.10 or later and is installed separately so CLI-only environments do not receive web dependencies:

```bash
pip install -e ".[web]"
streamlit run streamlit_app/app.py --server.address 127.0.0.1
```

The UI is intended for a single trusted operator on the local machine. It uses named AWS profiles and preserves the same read-only preview plus explicit restore confirmation used by the CLI. A fresh browser session starts with a workspace chooser and does not create a workspace automatically. The chooser lists persistent workspaces from `~/QuickSightWorkspaces` (or `QUICKSIGHT_WORKSPACE_HOME`) and creates safely named direct-child folders. Only folders with a valid versioned `.quicksight-workspace.json` marker are listed. From the Workspace tab, a selected library workspace can be renamed, and any valid workspace can be removed only while it is empty.

A browser folder selector can also validate and copy an existing workspace—including nested configs, manifests, overrides, bounded backup outputs, and reports—into a new isolated session; browser security means this imported copy cannot write changes back to the originally selected folder. Generated plans, temporary files, and previous workspace archives are omitted. Arbitrary server-side paths remain available under **Advanced workspace locations**, and ZIP export/restore remains an optional portability workflow.

Workspace files and History survive Streamlit restarts; reopen the persistent library or external workspace from the chooser after a fresh browser session. Backup and restore configurations can be uploaded and edited inline after safe YAML/JSON normalization, loaded directly from the workspace folder, or started from validated JSON templates; identity mappings are edited in `restore.identity_mappings`, and optional API-native overrides have the same workflow. Edited files can be saved in JSON or YAML before execution. Backup manifests remain immutable because they are authoritative restore evidence. Do not bind the UI to a public interface without adding an authenticated deployment boundary.

## Quick Start

> **Prerequisites**: Before starting, complete all items in the [Prerequisites](#prerequisites) section above, including Python 3.8+, AWS CLI configuration, required IAM permissions, and Amazon Quick Sight Enterprise edition.

> **Cost information**: This tool creates Amazon DynamoDB tables and Amazon S3 objects that incur AWS charges based on storage and usage. For pricing details, see the [Amazon DynamoDB pricing](https://aws.amazon.com/dynamodb/pricing/) and [Amazon S3 pricing](https://aws.amazon.com/s3/pricing/) pages. To track costs, configure billing alerts in the AWS Billing console. See the [Cleanup](#cleanup-and-resource-deletion) section to delete resources when no longer needed.

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

2. **Create the S3 bucket**:

```bash
aws s3 mb s3://my-quicksight-backups --region us-east-1
```

Replace the bucket name and region to match your configuration file.

3. **Run the backup**:

```bash
quicksight-backup --config config.yaml
```

4. **Verify the backup**:

Check for successful completion in the console output. Verify S3 bundles and DynamoDB tables were created:

```bash
# Verify S3 asset bundles
aws s3 ls s3://my-quicksight-backups/quicksight-backups/$(date +%Y/%m/%d)/

# Verify DynamoDB backup tables
aws dynamodb list-tables --query 'TableNames[?contains(@, `quicksight`) && contains(@, `backup`)]'
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

| Bundle Size | Advantages | Disadvantages |
|-------------|------------|---------------|
| Smaller (1-25 assets) | Faster export job completion, Lower memory usage, Better error isolation | More API calls and S3 objects, Increased backup time for large environments |
| Larger (50-100 assets) | Fewer API calls and S3 objects, Faster overall backup for large environments | Longer export job times, Higher memory usage, Larger blast radius for failures |

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

### Quick Sight Permissions

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
      "Resource": "arn:aws:quicksight:*:123456789012:*"
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
        "arn:aws:dynamodb:us-east-1:123456789012:table/*-quicksight-users-backup",
        "arn:aws:dynamodb:us-east-1:123456789012:table/*-quicksight-groups-backup",
        "arn:aws:dynamodb:us-east-1:123456789012:table/*-quicksight-users-groups-backup"
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
      "Resource": "arn:aws:s3:::my-quicksight-backups/quicksight-backups/*"
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

The tool automatically creates date-prefixed DynamoDB table names to preserve historical backups. Each backup run creates new tables with the current date as a prefix, so that previous backups are never overwritten.

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

All formats are converted to `YYYY-MM-DD` for DynamoDB table names so they have:
- No special characters (like `/`) that are invalid in table names
- Consistent naming across all backup runs
- Proper sorting and organization of historical backups

#### Table Management

**Important**: Each backup run creates new DynamoDB tables and S3 objects. To manage storage costs, establish a retention policy that deletes backups older than your required retention period:

```bash
# Example: List all backup tables
aws dynamodb list-tables --query 'TableNames[?contains(@, `quicksight-`) && contains(@, `-backup`)]'

# Example: Delete old backup tables
aws dynamodb delete-table --table-name 2025-10-01-quicksight-users-backup
```

**Warning**: The delete-table command permanently deletes backup data and cannot be undone. Verify you no longer need this backup before proceeding.

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

## Cleanup and Resource Deletion

When you no longer need the backup infrastructure, follow these steps to delete resources and stop incurring charges.

### Delete DynamoDB Tables

```bash
# List all backup tables
aws dynamodb list-tables --query 'TableNames[?contains(@, `quicksight-`) && contains(@, `-backup`)]'

# Delete specific backup tables (replace YYYY-MM-DD with actual date)
aws dynamodb delete-table --table-name YYYY-MM-DD-quicksight-users-backup
aws dynamodb delete-table --table-name YYYY-MM-DD-quicksight-groups-backup
aws dynamodb delete-table --table-name YYYY-MM-DD-quicksight-users-groups-backup
```

**Warning**: This permanently deletes backup data and cannot be undone.

### Delete S3 Backup Bundles

```bash
# Empty the backup prefix
aws s3 rm s3://my-quicksight-backups/quicksight-backups/ --recursive

# Delete the bucket (if dedicated to backups)
aws s3 rb s3://my-quicksight-backups
```

**Warning**: This permanently deletes all backup bundles. Verify you no longer need these backups.

### Verify Deletion

```bash
aws dynamodb list-tables | grep quicksight
aws s3 ls | grep quicksight-backups
```

### Cost Impact

DynamoDB tables and S3 storage incur charges based on AWS pricing. Delete resources when backups are no longer needed.

## Troubleshooting

### Common Issues

#### 1. Configuration Errors

**Error**: `Configuration file does not exist`
```bash
quicksight-backup --config config.yaml
# Error: Configuration file does not exist: config.yaml
```

**Solution**: Verify the configuration file path. Use an absolute path or verify that the relative path is correct from your current working directory.

**Error**: `Configuration file must be YAML or JSON`
```bash
quicksight-backup --config config.txt
# Error: Configuration file must be YAML or JSON: config.txt
```

**Solution**: The configuration file must use a YAML or JSON format. Rename your file with a `.yaml`, `.yml`, or `.json` extension.

#### 2. AWS Credentials Issues

**Error**: `Access Denied`
```
AWS Credentials Error: An error occurred (AccessDenied) when calling the ListUsers operation
```

**Solution**: Verify that your AWS credentials have the required permissions (see [Permissions](#permissions) section).

#### 3. Quick Sight API Issues

**Error**: `User is not registered in Quick Sight`
```
Quick Sight Error: User: arn:aws:iam::123456789012:user/myuser is not registered in Quick Sight
```

**Solution**: Register the user in Quick Sight or use credentials for a registered Quick Sight user.

**Error**: `Rate exceeded`
```
Quick Sight Error: Rate exceeded for operation: ListUsers
```

**Solution**: The is designed to include automatic rate limiting handling with exponential backoff. If this persists, try running during off-peak hours.

#### 4. DynamoDB Issues

**Error**: `Table already exists`
```
DynamoDB Error: Table already exists: quicksight-users-backup
```

**Solution**: This is expected behavior. The tool will use the existing table. Check the table schema matches expectations.

**Error**: `Requested resource not found`
```
DynamoDB Error: Requested resource not found: Table: quicksight-users-backup not found
```

**Solution**: The tool will automatically create the table. Verify that you have `dynamodb:CreateTable` permissions.

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

**Solution**: Create the S3 bucket manually or check the bucket name is correct in your configuration.

**Error**: `Access Denied`
```
S3 Error: An error occurred (AccessDenied) when calling the PutObject operation
```

**Solution**: Verify that your AWS credentials have `s3:PutObject` permissions for the specified bucket.

#### 6. Asset Bundle Export Issues

**Error**: `Export job failed`
```
Asset Bundle Error: Export job failed with status: FAILED
```

**Solutions**:
- Check if the assets exist and are accessible
- Verify that FILE datasets are properly excluded
- Check assets don't have circular dependencies
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
   - AWS region and Quick Sight setup details

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


## Conclusion

The Amazon Quick Sight Backup Tool provides comprehensive backup capabilities for your Quick Sight environment, enabling disaster recovery and migration scenarios. With flexible backup modes, robust error handling, and automated organization of backups, the tool helps protect your BI assets and maintain business continuity. Get started by following the Quick Start guide above and adapt the configuration to meet your specific requirements.

## Restore (Part 2 P0)

Restore is exposed as a separate command, so the existing `quicksight-backup` CLI and configuration remain unchanged. For backups that include the generated restore manifest, the normal workflow validates and creates a digest-protected plan internally, shows one reviewed preview, asks for confirmation once, and removes the temporary plan after execution:

```text
# Read-only validation and preview
quicksight-restore --manifest backup_manifest_YYYYMMDD_HHMMSS.json --config target.yaml --dry-run

# Execute with one interactive confirmation
quicksight-restore --manifest backup_manifest_YYYYMMDD_HHMMSS.json --config target.yaml

# Non-interactive execution after reviewing the dry-run
quicksight-restore --manifest backup_manifest_YYYYMMDD_HHMMSS.json --config target.yaml --yes
```

The normal command reports preflight stages, identity results, bundle progress, import-job status changes, 15-second heartbeats, and per-resource completion totals. Operators who need to retain and separately approve the plan can still use the advanced workflow:

```text
quicksight-restore plan --config examples/restore-in-place.yaml --bundle-key S3_OBJECT_KEY_1 --bundle-key S3_OBJECT_KEY_2 --output restore-plan.json
quicksight-restore run --config examples/restore-in-place.yaml --plan restore-plan.json --bundle-key S3_OBJECT_KEY_1 --bundle-key S3_OBJECT_KEY_2
quicksight-restore status --config examples/restore-in-place.yaml --restore-id RESTORE_ID
quicksight-restore status --report-directory ./restore-reports --restore-id RESTORE_ID
```

For legacy backups that do not contain a restore manifest, replace the advanced-workflow key placeholders with full object keys as shown by S3 or the backup logs. When selectors are supplied on the command line, repeat the identical `--backup-date` and `--bundle-key` values for both `plan` and `run` so execution can reproduce the reviewed configuration snapshot; alternatively, persist them in `source_backup` and omit the CLI selector flags. Date-only discovery (`--backup-date`, or `source_backup.backup_date` without explicit keys) is a convenience for dates containing exactly one ZIP object. Normal Part 1 runs can produce several ZIPs; an ambiguous date is rejected rather than guessed, so use repeated `--bundle-key` options or `source_backup.bundle_keys` for those runs.

The config-backed status form resolves `restore.report_directory` relative to the configuration file. The direct form is local-only and resolves a relative report directory from the current working directory; it does not initialize AWS clients. Status exits are `0` for success, `1` for failed, `2` for partial, `3` for a valid running checkpoint, and `4` when the report cannot be read or verified.

`plan` performs source and target read-only discovery, validates exact S3 versions/sizes/SHA-256 checksums and ZIP structure, inventories and deduplicates members, detects target conflicts, validates principals and API-native overrides, and persists a digest-protected plan. `run` verifies that plan and every selected source object before the first target mutation, optionally restores supported identities, imports the original verified bundle bytes in dependency order, and writes an atomic JSON report under `restore.report_directory`.

### Restore security and limitations

- Use the default AWS credential chain, named profiles, or STS AssumeRole. Restore configuration does not accept access keys, session tokens, passwords, or credential pairs. Store data-source secrets in an approved target service such as AWS Secrets Manager and reference only supported API-native values.
- Asset restore accepts native `QUICKSIGHT_JSON` archives only. Each selected bundle must be no larger than 20 MiB because the original verified bytes are sent through `StartAssetBundleImportJob`'s inline body; this version does not rewrite, recompress, or stage bundles in target-owned S3.
- Quick Sight-managed users can be registered. IAM users require a reviewed target IAM user/role ARN (and a session name for roles). IAM Identity Center identities and assignments must be provisioned in their authoritative identity source; restore only maps and verifies them.

#### Quick-native user activation after restore

A successful restore of an `IdentityType=QUICKSIGHT` user means that the target user resource, role, group mappings, and memberships were recreated. It does **not** restore the user's password, prior sign-in session, or activated state. A newly registered native user is normally returned with `Active=false` and must complete registration, set a password, and sign in again before the user can access Quick.

This manual step is required by the AWS API contract: [`RegisterUser`](https://docs.aws.amazon.com/quicksight/latest/APIReference/API_RegisterUser.html) returns a one-time `UserInvitationUrl` for native Quick users, but API registration does **not** send an invitation email. This sample intentionally does not write that sensitive one-time URL into plans, logs, or restore reports. After restore, a Quick administrator should open **Manage Quick > Manage users** and choose **Resend invitation** for each restored native user. Integrators that call `RegisterUser` directly may instead deliver the URL from that immediate API response through an approved secure channel, but must not persist or log it. The user must finish the registration flow and sign in once; verify that `list-users` then reports `Active=true`.

This limitation applies to Quick-native users only. IAM users continue to authenticate through their mapped IAM principal, and IAM Identity Center users remain managed and activated in IAM Identity Center.

- Legacy Part 1 backups have no immutable run ID. Obvious same-day duplicate singleton/bundle indexes stop planning; select exact object keys explicitly rather than allowing the tool to guess. Their metadata also does not cryptographically bind a dependency to the exact provider bundle/version from one backup run. Planning derives provider edges from the complete selected inventory and rejects ambiguous providers, but operators must still select a coherent reviewed set and validate restored workloads.
- Plan/report SHA-256 seals detect accidental or out-of-band content changes; they are integrity checks, not signatures or proof of author identity. Keep the configuration directory, overrides, plans, and report directory under trusted operator control. Descriptor, reparse-point, ownership-token, and digest checks fail closed on observed changes, but the local filesystem protocol is not a security boundary against a malicious process racing namespace changes.
- `FailureAction=ROLLBACK` applies to one import job only. It is not an atomic rollback across all planned bundles. Only the `SUCCESSFUL` terminal state is counted as success; timeout, unknown states, and every failed/rollback-failed state are failures.
- Reports count planned members and jobs, not `DescribeAssetBundleImportJob` imported-asset lists, because that response is not an authoritative inventory of every restored asset.
- Asset bundles do not contain source data or credentials, and Part 2 cannot restore content that Part 1 excluded. Cross-account and cross-Region recovery requires reviewed principals, IAM roles, VPC/data-source settings, credentials, and post-import workload validation.
- This remains an educational sample. Run security, compliance, quota, and representative non-production recovery testing before production use.

Restore source access needs `s3:ListBucket`, `s3:GetObject`, `s3:GetObjectVersion`, `dynamodb:DescribeTable`, and `dynamodb:Scan`. Target access needs the applicable Quick Sight list/describe identity and asset APIs, `quicksight:CreateGroup`, `quicksight:RegisterUser`, `quicksight:CreateGroupMembership`, `quicksight:StartAssetBundleImportJob`, `quicksight:DescribeAssetBundleImportJob`, plus `iam:GetRole`/`iam:GetUser` for reviewed IAM mappings.
