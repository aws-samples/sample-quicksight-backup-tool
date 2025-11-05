"""
Command-line interface for QuickSight Backup Tool.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

from .orchestrator import QuickSightBackupOrchestrator
from .models.exceptions import (
    ConfigurationError,
    AWSCredentialsError,
    QuickSightBackupError
)


def setup_logging(verbose: bool = False, log_file: Optional[str] = None) -> None:
    """
    Setup logging configuration based on verbosity level.
    
    Args:
        verbose: Enable verbose (DEBUG) logging
        log_file: Optional log file path
    """
    log_level = logging.DEBUG if verbose else logging.INFO
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Configure root logger
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[]
    )
    
    # Add console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_formatter = logging.Formatter(log_format)
    console_handler.setFormatter(console_formatter)
    logging.getLogger().addHandler(console_handler)
    
    # Add file handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_formatter = logging.Formatter(log_format)
        file_handler.setFormatter(file_formatter)
        logging.getLogger().addHandler(file_handler)


def validate_config_file(config_path: str) -> str:
    """
    Validate that the configuration file exists and is readable.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        str: Absolute path to configuration file
        
    Raises:
        argparse.ArgumentTypeError: If file doesn't exist or isn't readable
    """
    path = Path(config_path)
    
    if not path.exists():
        raise argparse.ArgumentTypeError(f"Configuration file does not exist: {config_path}")
    
    if not path.is_file():
        raise argparse.ArgumentTypeError(f"Configuration path is not a file: {config_path}")
    
    if not path.suffix.lower() in ['.yaml', '.yml', '.json']:
        raise argparse.ArgumentTypeError(f"Configuration file must be YAML or JSON: {config_path}")
    
    try:
        # Test if file is readable
        with open(path, 'r') as f:
            f.read(1)
    except PermissionError:
        raise argparse.ArgumentTypeError(f"Configuration file is not readable: {config_path}")
    except Exception as e:
        raise argparse.ArgumentTypeError(f"Error accessing configuration file: {e}")
    
    return str(path.absolute())


def create_argument_parser() -> argparse.ArgumentParser:
    """
    Create and configure the argument parser for the CLI.
    
    Returns:
        argparse.ArgumentParser: Configured argument parser
    """
    parser = argparse.ArgumentParser(
        prog='quicksight-backup',
        description='QuickSight Backup Tool - Backup QuickSight resources to DynamoDB and S3',
        epilog='''
Examples:
  %(prog)s --config config.yaml
  %(prog)s --config config.yaml --verbose
  %(prog)s --config config.yaml --mode users-only --output-dir ./backups
  %(prog)s --config config.yaml --dry-run --verbose
        ''',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Required arguments
    parser.add_argument(
        '--config', '-c',
        type=validate_config_file,
        required=True,
        help='Path to configuration file (YAML or JSON format)'
    )
    
    # Backup mode options
    parser.add_argument(
        '--mode', '-m',
        choices=['full', 'users-only', 'assets-only'],
        default='full',
        help='Backup mode: full (default), users-only, or assets-only'
    )
    
    # Output options
    parser.add_argument(
        '--output-dir', '-o',
        type=str,
        help='Output directory for backup reports and manifests (default: current directory)'
    )
    
    # Logging options
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose (DEBUG) logging'
    )
    
    parser.add_argument(
        '--log-file',
        type=str,
        help='Path to log file (logs to console only if not specified)'
    )
    
    # Execution options
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Perform a dry run without making any changes (validate configuration and connectivity only)'
    )
    
    parser.add_argument(
        '--no-progress',
        action='store_true',
        help='Disable progress indicators for batch/automated execution'
    )
    
    # Report options
    parser.add_argument(
        '--generate-manifest',
        action='store_true',
        help='Generate backup manifest file (enabled by default)'
    )
    
    parser.add_argument(
        '--generate-report',
        action='store_true',
        help='Generate human-readable backup report (enabled by default)'
    )
    
    # Version
    parser.add_argument(
        '--version',
        action='version',
        version='%(prog)s 1.0.0'
    )
    
    return parser


def execute_backup(args: argparse.Namespace) -> int:
    """
    Execute the backup operation based on CLI arguments.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        int: Exit code (0 for success, non-zero for failure)
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Initialize orchestrator
        logger.info(f"Initializing QuickSight Backup Tool with args: {args}")
        orchestrator = QuickSightBackupOrchestrator(args)
        
        # Initialize services
        logger.info("Initializing backup services...")
        if not orchestrator.initialize():
            logger.error("Failed to initialize backup orchestrator")
            return 1
        
        # Handle dry run mode
        if args.dry_run:
            logger.info("Dry run mode - configuration and connectivity validated successfully")
            print("✓ Configuration file is valid")
            print("✓ AWS connectivity validated")
            print("✓ All prerequisites met")
            print("Dry run completed successfully. Use --mode to execute actual backup.")
            return 0
        
        # Execute backup based on mode
        logger.info(f"Executing backup in '{args.mode}' mode")
        
        backup_report = orchestrator.execute_backup()
        
        # Generate output files
        output_dir = Path(args.output_dir) if args.output_dir else Path.cwd()
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate manifest (default enabled)
        if args.generate_manifest or args.mode == 'full':
            manifest_path = output_dir / f"backup_manifest_{orchestrator.backup_report.start_time.strftime('%Y%m%d_%H%M%S')}.json"
            orchestrator.generate_backup_manifest(str(manifest_path))
            logger.info(f"Backup manifest saved to: {manifest_path}")
        
        # Generate report (default enabled)
        if args.generate_report or args.mode == 'full':
            report_path = output_dir / f"backup_report_{orchestrator.backup_report.start_time.strftime('%Y%m%d_%H%M%S')}.txt"
            orchestrator.save_backup_report(str(report_path))
            logger.info(f"Backup report saved to: {report_path}")
        
        # Print summary to console
        if not args.no_progress:
            print("\n" + "="*60)
            print("BACKUP COMPLETED")
            print("="*60)
            
            if orchestrator.backup_report:
                print(f"Success Rate: {orchestrator.backup_report.success_rate:.1f}%")
                print(f"Total Resources: {orchestrator.backup_report.total_resources}")
                print(f"Execution Time: {orchestrator.backup_report.total_execution_time:.2f}s")
                
                if orchestrator.backup_report.failed_resources > 0:
                    print(f"⚠ {orchestrator.backup_report.failed_resources} resources failed")
                    return 2  # Partial success
                else:
                    print("✓ All resources backed up successfully")
        
        return 0
        
    except ConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        print(f"Configuration Error: {e}", file=sys.stderr)
        print("Please check your configuration file and try again.", file=sys.stderr)
        return 1
        
    except AWSCredentialsError as e:
        logger.error(f"AWS credentials error: {e}")
        print(f"AWS Credentials Error: {e}", file=sys.stderr)
        print("Please check your AWS credentials and permissions.", file=sys.stderr)
        return 1
        
    except QuickSightBackupError as e:
        logger.error(f"Backup error: {e}")
        print(f"Backup Error: {e}", file=sys.stderr)
        return 1
        
    except KeyboardInterrupt:
        logger.warning("Backup interrupted by user")
        print("\nBackup interrupted by user", file=sys.stderr)
        return 130  # Standard exit code for SIGINT
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        print(f"Unexpected Error: {e}", file=sys.stderr)
        print("Please check the logs for more details.", file=sys.stderr)
        return 1


def main() -> int:
    """
    Main entry point for the CLI application.
    
    Returns:
        int: Exit code
    """
    # Create argument parser
    parser = create_argument_parser()
    
    # Parse arguments
    try:
        args = parser.parse_args()
    except SystemExit as e:
        return e.code
    
    # Setup logging
    setup_logging(verbose=args.verbose, log_file=args.log_file)
    
    # Execute backup
    return execute_backup(args)


if __name__ == '__main__':
    sys.exit(main())