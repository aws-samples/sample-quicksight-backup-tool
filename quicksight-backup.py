"""
Command-line interface for Quick Sight Backup Tool.

This script provides a direct entry point for the Quick Sight Backup Tool.
It delegates to the main CLI module in the package.
"""

import sys
from quicksight_backup.cli import main

if __name__ == '__main__':
    sys.exit(main())