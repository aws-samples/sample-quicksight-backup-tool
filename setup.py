"""
Setup configuration for QuickSight Backup Tool.
"""

from setuptools import setup, find_packages
import os

# Read the README file for long description
def read_readme():
    readme_path = os.path.join(os.path.dirname(__file__), 'README.md')
    if os.path.exists(readme_path):
        with open(readme_path, 'r', encoding='utf-8') as f:
            return f.read()
    return "QuickSight Backup Tool - A comprehensive backup solution for Amazon QuickSight resources."

# Read version from package
def get_version():
    version_file = os.path.join('quicksight_backup', '__init__.py')
    with open(version_file, 'r') as f:
        for line in f:
            if line.startswith('__version__'):
                return line.split('=')[1].strip().strip('"').strip("'")
    return "1.0.0"

setup(
    name="quicksight-backup-tool",
    version=get_version(),
    author="QuickSight Backup Tool Team",
    author_email="support@quicksight-backup.com",
    description="A comprehensive backup solution for Amazon QuickSight resources including users, groups, and asset bundles",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/quicksight-backup/quicksight-backup-tool",
    packages=find_packages(exclude=["tests", "tests.*"]),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: System :: Archiving :: Backup",
        "Topic :: System :: Systems Administration",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Environment :: Console",
        "Natural Language :: English",
    ],
    python_requires=">=3.8",
    install_requires=[
        "boto3>=1.26.0,<2.0.0",
        "botocore>=1.29.0,<2.0.0",
        "PyYAML>=6.0,<7.0.0",
        "python-dateutil>=2.8.0,<3.0.0",
        "typing-extensions>=4.0.0,<5.0.0",
        "requests>=2.28.0,<3.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "moto>=4.0.0",
            "black>=22.0.0",
            "flake8>=5.0.0",
            "mypy>=1.0.0",
            "isort>=5.0.0",
        ],
        "test": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "moto>=4.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "quicksight-backup=quicksight_backup.cli:main",
        ],
    },
    include_package_data=True,
    package_data={
        "quicksight_backup": ["config/*.yaml", "config/*.json"],
    },
    keywords="aws quicksight backup automation boto3 dynamodb s3 asset-bundle disaster-recovery migration",
    project_urls={
        "Bug Reports": "https://github.com/quicksight-backup/quicksight-backup-tool/issues",
        "Source": "https://github.com/quicksight-backup/quicksight-backup-tool",
        "Documentation": "https://github.com/quicksight-backup/quicksight-backup-tool/wiki",
        "Changelog": "https://github.com/quicksight-backup/quicksight-backup-tool/blob/main/CHANGELOG.md",
        "Funding": "https://github.com/sponsors/quicksight-backup",
    },
    license="MIT",
    platforms=["any"],
)