"""Command-line interface for planning, running, and inspecting restores."""

from typing import List, Optional
import argparse
import json
import sys

from .config.loader import RestoreConfigLoader
from .models.errors import RestoreError
from .orchestrator import RestoreOrchestrator
from .services.report import RestoreReportService

_STATUS_EXIT_CODES = {
    "success": 0,
    "failed": 1,
    "partial": 2,
    "running": 3,
}
_STATUS_READ_ERROR = 4


def _add_selection_arguments(command: argparse.ArgumentParser) -> None:
    command.add_argument("--backup-date", help="Explicit legacy backup date (YYYY-MM-DD)")
    command.add_argument(
        "--bundle-key",
        action="append",
        dest="bundle_keys",
        help="Explicit S3 object key; repeat for every selected bundle",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="quicksight-restore",
        description="Plan and execute restores from Quick Sight Backup Tool Part 1 artifacts.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan = subparsers.add_parser("plan", help="Discover artifacts and create a read-only plan")
    plan.add_argument("--config", required=True, help="Restore YAML or JSON configuration")
    _add_selection_arguments(plan)
    plan.add_argument(
        "--output", default="restore-plan.json", help="Path for the persisted restore plan"
    )

    run = subparsers.add_parser("run", help="Verify a persisted plan and execute it")
    run.add_argument("--config", required=True, help="Restore YAML or JSON configuration")
    run.add_argument("--plan", required=True, help="Persisted restore plan JSON")
    _add_selection_arguments(run)

    status = subparsers.add_parser("status", help="Read a persisted restore report")
    source = status.add_mutually_exclusive_group(required=True)
    source.add_argument("--config", help="Restore YAML or JSON configuration used by the run")
    source.add_argument(
        "--report-directory",
        help="Local report directory (relative paths use the current directory)",
    )
    status.add_argument("--restore-id", required=True, help="Restore ID from a run report")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        loader = RestoreConfigLoader()
        if args.command == "status":
            if args.config:
                report_directory = loader.load(args.config).restore.report_directory
            else:
                report_directory = args.report_directory
            report = RestoreReportService(report_directory).load(args.restore_id)
            print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
            return _STATUS_EXIT_CODES.get(report.overall_status, _STATUS_READ_ERROR)

        if args.command == "plan":
            config = loader.load(
                args.config,
                backup_date=args.backup_date,
                bundle_keys=args.bundle_keys,
            )
            orchestrator = RestoreOrchestrator(config)
            plan = orchestrator.plan(
                output_path=args.output,
                backup_date=args.backup_date,
                bundle_keys=args.bundle_keys,
            )
            print(
                json.dumps(
                    {
                        "plan_id": plan.plan_id,
                        "plan_digest": plan.plan_digest,
                        "planned_bundles": len(plan.bundles),
                        "output": args.output,
                        "read_only": True,
                    },
                    sort_keys=True,
                )
            )
            return 0

        config = loader.load(
            args.config,
            backup_date=args.backup_date,
            bundle_keys=args.bundle_keys,
        )
        orchestrator = RestoreOrchestrator(config)
        report = orchestrator.run(args.plan)
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
        if report.overall_status == "success":
            return 0
        if report.overall_status == "partial":
            return 2
        return 1
    except RestoreError as error:
        print("Restore error: {0}".format(error), file=sys.stderr)
        if args.command == "status":
            return _STATUS_READ_ERROR
        return 2 if args.command == "plan" else 1
    except KeyboardInterrupt:
        print("Restore interrupted", file=sys.stderr)
        return 130
    except Exception as error:
        print("Unexpected restore error: {0}".format(error), file=sys.stderr)
        if args.command == "status":
            return _STATUS_READ_ERROR
        return 2 if args.command == "plan" else 1


if __name__ == "__main__":
    sys.exit(main())
