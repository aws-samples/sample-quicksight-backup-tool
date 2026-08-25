"""
Data models for backup operation results.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Iterable, List, Tuple


RESOURCE_COUNT_FIELDS = ("successful", "failed", "skipped")


def aggregate_resource_counts(results: Iterable["BackupResult"]) -> Dict[str, Dict[str, int]]:
    """Aggregate per-resource counts from backup result metadata."""
    aggregated: Dict[str, Dict[str, int]] = {}

    for result in results:
        resource_counts = result.metadata.get("resource_counts", {})
        if not isinstance(resource_counts, dict):
            continue

        for resource_type, counts in resource_counts.items():
            if not isinstance(resource_type, str) or not isinstance(counts, dict):
                continue

            totals = aggregated.setdefault(
                resource_type,
                {field_name: 0 for field_name in RESOURCE_COUNT_FIELDS},
            )
            for field_name in RESOURCE_COUNT_FIELDS:
                totals[field_name] += int(counts.get(field_name, 0))

    return aggregated


def get_resource_totals(resource_counts: Dict[str, Dict[str, int]]) -> Dict[str, int]:
    """Sum successful, failed, and skipped counts across all resource types."""
    return {
        field_name: sum(counts.get(field_name, 0) for counts in resource_counts.values())
        for field_name in RESOURCE_COUNT_FIELDS
    }


def format_resource_counts_table(resource_counts: Dict[str, Dict[str, int]]) -> str:
    """Render a compact table of per-resource backup counts."""
    headers = ("Type", "Successful", "Failed", "Skipped")
    rows = []

    for resource_type, counts in resource_counts.items():
        rows.append(
            (
                resource_type.replace("_", " ").title(),
                str(counts.get("successful", 0)),
                str(counts.get("failed", 0)),
                str(counts.get("skipped", 0)),
            )
        )

    totals = get_resource_totals(resource_counts)
    rows.append(
        (
            "TOTAL",
            str(totals["successful"]),
            str(totals["failed"]),
            str(totals["skipped"]),
        )
    )

    widths = [
        max(len(headers[index]), *(len(row[index]) for row in rows))
        for index in range(len(headers))
    ]

    def format_row(row: Tuple[str, str, str, str]) -> str:
        return (
            f"{row[0]:<{widths[0]}}  "
            f"{row[1]:>{widths[1]}}  "
            f"{row[2]:>{widths[2]}}  "
            f"{row[3]:>{widths[3]}}"
        )

    separator = "  ".join("-" * width for width in widths)
    return "\n".join([format_row(headers), separator, *(format_row(row) for row in rows)])


class BackupStatus(Enum):
    """Status enumeration for backup operations."""

    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"
    IN_PROGRESS = "in_progress"


@dataclass
class BackupResult:
    """Result of a backup operation for a specific resource type."""

    resource_type: str
    success: bool
    items_processed: int
    items_failed: int
    error_messages: List[str] = field(default_factory=list)
    execution_time: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)
    status: BackupStatus = BackupStatus.SUCCESS
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def resource_counts(self) -> Dict[str, Dict[str, int]]:
        """Return this operation's optional per-resource counts."""
        resource_counts = self.metadata.get("resource_counts", {})
        return resource_counts if isinstance(resource_counts, dict) else {}

    def add_error(self, error_message: str) -> None:
        """Add an error message to the result."""
        self.error_messages.append(error_message)
        self.items_failed += 1
        self.success = False
        if self.items_processed > 0:
            self.status = BackupStatus.PARTIAL
        else:
            self.status = BackupStatus.FAILED


@dataclass
class BackupReport:
    """Comprehensive report of all backup operations."""

    total_resources: int
    successful_resources: int
    failed_resources: int
    partial_resources: int
    total_execution_time: float
    start_time: datetime
    end_time: datetime
    results: List[BackupResult] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        """Calculate the legacy operation success rate."""
        if self.total_resources == 0:
            return 0.0
        return (self.successful_resources / self.total_resources) * 100

    @property
    def resource_counts(self) -> Dict[str, Dict[str, int]]:
        """Aggregate successful, failed, and skipped counts by resource type."""
        return aggregate_resource_counts(self.results)

    @property
    def resource_totals(self) -> Dict[str, int]:
        """Return successful, failed, and skipped totals for actual resources."""
        return get_resource_totals(self.resource_counts)

    @property
    def resource_success_rate(self) -> float:
        """Calculate resource success rate, excluding skipped resources."""
        totals = self.resource_totals
        attempted = totals["successful"] + totals["failed"]
        if attempted == 0:
            return 0.0
        return (totals["successful"] / attempted) * 100

    @property
    def has_failures(self) -> bool:
        """Return whether an operation or actual resource failed."""
        return (
            any(
                result.status in (BackupStatus.FAILED, BackupStatus.PARTIAL)
                for result in self.results
            )
            or self.failed_resources > 0
            or self.partial_resources > 0
            or self.resource_totals["failed"] > 0
        )

    def format_resource_counts(self) -> str:
        """Render the report's compact resource count table."""
        return format_resource_counts_table(self.resource_counts)

    def add_result(self, result: BackupResult) -> None:
        """Add a backup result to the report."""
        self.results.append(result)
        if result.status == BackupStatus.SUCCESS:
            self.successful_resources += 1
        elif result.status == BackupStatus.FAILED:
            self.failed_resources += 1
        elif result.status == BackupStatus.PARTIAL:
            self.partial_resources += 1
