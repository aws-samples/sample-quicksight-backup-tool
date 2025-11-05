# QuickSight Backup Tool - Configuration Examples

This directory contains example configuration files for different deployment scenarios. Choose the configuration that best matches your environment and customize it according to your needs.

## Available Configurations

### 1. Basic Configuration (`config-basic.yaml`)
**Use Case**: Getting started, small environments, testing
- Minimal required settings
- Default options for most parameters
- Simple logging configuration
- Good for learning and initial setup

### 2. Development Configuration (`config-development.yaml`)
**Use Case**: Development and testing environments
- Debug-level logging for troubleshooting
- Development-specific naming conventions
- Separate AWS account/region recommended
- Verbose output for learning

### 3. Production Configuration (`config-production.yaml`)
**Use Case**: Production environments, business-critical backups
- Production-optimized settings
- Enhanced error handling
- Structured logging paths
- Performance considerations included

### 4. Enterprise Configuration (`config-enterprise.yaml`)
**Use Case**: Large-scale enterprise deployments
- Compliance and security considerations
- Multi-region disaster recovery notes
- Monitoring and alerting guidance
- Advanced IAM policy examples

### 5. Cross-Account Configuration (`config-cross-account.yaml`)
**Use Case**: Backing up across AWS accounts
- Cross-account IAM role setup
- Security considerations for multi-account access
- Trust policy examples
- Network and permissions guidance

### 6. Lambda Configuration (`config-lambda.yaml`)
**Use Case**: Serverless execution in AWS Lambda
- Lambda-optimized settings
- CloudWatch Logs integration
- Packaging and deployment notes
- EventBridge scheduling examples

### 7. Minimal JSON Configuration (`config-minimal.json`)
**Use Case**: Programmatic configuration, CI/CD pipelines
- JSON format for easy parsing
- Only required parameters
- Suitable for automation scripts
- Template for dynamic configuration generation

## Quick Setup Guide

1. **Choose a configuration** that matches your deployment scenario
2. **Copy the file** to your working directory:
   ```bash
   cp examples/config-basic.yaml ./config.yaml
   ```
3. **Edit the configuration** with your specific values:
   - AWS region and account ID
   - DynamoDB table names
   - S3 bucket name
   - Logging preferences
4. **Validate the configuration**:
   ```bash
   quicksight-backup --config config.yaml --dry-run
   ```
5. **Run your first backup**:
   ```bash
   quicksight-backup --config config.yaml --verbose
   ```

## Configuration Parameters Reference

### Required Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `aws.region` | AWS region for QuickSight assets (datasources, datasets, analyses, dashboards) | `us-east-1` |
| `aws.account_id` | AWS account ID (12-digit string) | `"123456789012"` |
| `dynamodb.users_table_name` | DynamoDB table name for user data | `quicksight-users-backup` |
| `dynamodb.groups_table_name` | DynamoDB table name for group data | `quicksight-groups-backup` |
| `s3.bucket_name` | S3 bucket for asset bundle storage | `my-quicksight-backups` |

### Optional Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `aws.identity_region` | `aws.region` | AWS region for QuickSight users and groups |
| `s3.prefix_format` | `YYYY/MM/DD` | Date-based folder structure in S3 |
| `backup.include_dependencies` | `true` | Include asset dependencies in exports |
| `backup.include_permissions` | `true` | Include sharing permissions |
| `backup.include_tags` | `true` | Include resource tags |
| `backup.export_format` | `QUICKSIGHT_JSON` | Export format (only option currently) |
| `logging.level` | `INFO` | Log level (DEBUG, INFO, WARNING, ERROR) |
| `logging.file_path` | None | Log file path (optional) |

## Environment-Specific Considerations

### Multi-Region QuickSight Deployments
- **Identity Region**: QuickSight users and groups are managed in a single region (typically us-east-1)
- **Asset Regions**: QuickSight assets (datasources, datasets, analyses, dashboards) can exist in multiple regions
- **Configuration**: Use `identity_region` for users/groups and `region` for assets
- **Example**: Users managed in us-east-1, assets deployed in us-west-2

### Development Environment
- Use separate AWS account or region
- Enable DEBUG logging for troubleshooting
- Use smaller test datasets for faster iterations
- Test all backup modes (full, users-only, assets-only)

### Production Environment
- Use INFO logging level (DEBUG only for troubleshooting)
- Configure log rotation and monitoring
- Set up CloudWatch alarms for backup failures
- Enable S3 versioning and encryption
- Use IAM roles instead of access keys

### Enterprise Environment
- Implement compliance requirements (encryption, retention)
- Set up cross-region disaster recovery
- Configure monitoring and alerting
- Use least privilege IAM policies
- Document recovery procedures

## Security Best Practices

### Credentials Management
- Use IAM roles when possible (EC2, Lambda, ECS)
- Avoid hardcoding credentials in configuration files
- Use AWS Secrets Manager for sensitive data
- Rotate credentials regularly

### Access Control
- Follow least privilege principle for IAM policies
- Use resource-based policies for fine-grained control
- Enable CloudTrail for audit logging
- Restrict S3 bucket access with bucket policies

### Data Protection
- Enable S3 bucket encryption (SSE-S3 or SSE-KMS)
- Enable DynamoDB encryption at rest
- Use VPC endpoints for private network access
- Configure S3 bucket versioning and MFA delete

## Troubleshooting Configuration Issues

### Common Configuration Errors

1. **Invalid AWS Account ID Format**
   ```yaml
   # Wrong - numeric value
   account_id: 123456789012
   
   # Correct - string value
   account_id: "123456789012"
   ```

2. **Missing Required Parameters**
   ```bash
   # Error: Configuration validation failed: Missing required parameter: aws.region
   ```
   Solution: Ensure all required parameters are present

3. **Invalid S3 Bucket Name**
   ```bash
   # Error: The specified bucket does not exist
   ```
   Solution: Create the S3 bucket or verify the name is correct

4. **DynamoDB Table Permissions**
   ```bash
   # Error: User is not authorized to perform: dynamodb:CreateTable
   ```
   Solution: Add DynamoDB permissions to your IAM role/user

### Validation Commands

```bash
# Test configuration syntax and AWS connectivity
quicksight-backup --config config.yaml --dry-run

# Test with verbose logging for detailed troubleshooting
quicksight-backup --config config.yaml --dry-run --verbose

# Validate specific backup mode
quicksight-backup --config config.yaml --mode users-only --dry-run
```

## Migration Between Configurations

When migrating from one configuration to another:

1. **Backup existing data** before making changes
2. **Test new configuration** with `--dry-run` first
3. **Update table names** if changing naming conventions
4. **Verify permissions** for new resources
5. **Update monitoring** and alerting configurations

## Support and Documentation

- **Main Documentation**: See [README.md](../README.md) in the root directory
- **Troubleshooting**: Refer to the troubleshooting section in the main README
- **Issues**: Report configuration issues on [GitHub Issues](https://github.com/quicksight-backup/quicksight-backup-tool/issues)
- **Examples**: Additional examples available in the [GitHub Wiki](https://github.com/quicksight-backup/quicksight-backup-tool/wiki)