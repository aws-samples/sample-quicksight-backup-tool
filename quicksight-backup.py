"""
Command-line interface for QuickSight Backup Tool.

This script provides a direct entry point for the QuickSight Backup Tool.
It delegates to the main CLI module in the package.
"""

import sys
from quicksight_backup.cli import main

if __name__ == '__main__':
    sys.exit(main())