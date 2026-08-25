"""Command-line interface for simple and advanced Quick Sight restores."""

from pathlib import Path
from typing import Any, Dict, List, Optional
import argparse
import json
import sys
import tempfile

from .config.loader import RestoreConfigLoader
from .models.errors import RestoreConfigurationError, RestoreError
from .orchestrator import RestoreOrchestrator
from .services.report import RestoreReportService

_STATUS_EXIT_CODES = {
    "success": 0,
    "failed": 1,
    "partial": 2,
    "running": 3,
}
_STATUS_READ_ERROR = 4
_RESOURCE_OUTCOMES = ("successful", "failed", "skipped", "not_attempted", "pending")


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
        description=(
            "Restore a Quick Sight backup manifest. The normal path plans and validates "
            "internally; advanced plan/run/status commands remain available."
        ),
        epilog=(
            "Normal: quicksight-restore --manifest backup_manifest.json --config target.yaml\n"
            "Preview: quicksight-restore --manifest backup_manifest.json --config target.yaml "
            "--dry-run"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--config",
        help="Target, mappings, and restore options YAML or JSON configuration",
    )
    parser.add_argument(
        "--backup-manifest",
        "--manifest",
        dest="backup_manifest",
        help="Backup manifest emitted by quicksight-backup",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Execute the reviewed restore without an interactive confirmation",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and validate the internal plan, print the preview, and make no changes",
    )
    parser.add_argument(
        "--plan-output",
        help="Optionally retain the internally generated plan at this path",
    )

    subparsers = parser.add_subparsers(dest="command")

    plan = subparsers.add_parser("plan", help="Advanced: create a persisted read-only plan")
    plan.add_argument("--config", required=True, help="Restore YAML or JSON configuration")
    _add_selection_arguments(plan)
    plan.add_argument(
        "--output", default="restore-plan.json", help="Path for the persisted restore plan"
    )

    run = subparsers.add_parser("run", help="Advanced: verify and execute a persisted plan")
    run.add_argument("--config", required=True, help="Restore YAML or JSON configuration")
    run.add_argument("--plan", required=True, help="Persisted restore plan JSON")
    _add_selection_arguments(run)

    status = subparsers.add_parser("status", help="Advanced: read a persisted restore report")
    source = status.add_mutually_exclusive_group(required=True)
    source.add_argument("--config", help="Restore YAML or JSON configuration used by the run")
    source.add_argument(
        "--report-directory",
        help="Local report directory (relative paths use the current directory)",
    )
    status.add_argument("--restore-id", required=True, help="Restore ID from a run report")
    return parser


def _resource_type_counts(plan: Any) -> Dict[str, int]:
    resources = {
        resource_key for bundle in plan.bundles for resource_key in bundle.selected_resources
    }
    counts: Dict[str, int] = {}
    for resource_key in resources:
        resource_type = resource_key.split("/", 1)[0]
        counts[resource_type] = counts.get(resource_type, 0) + 1
    return dict(sorted(counts.items()))


def _identity_counts(plan: Any) -> Dict[str, int]:
    snapshot = plan.manifest.identity_snapshot if plan.restore_identities else None
    if snapshot is None:
        return {}
    return {
        "user": snapshot.users.item_count,
        "group": snapshot.groups.item_count,
        "membership": snapshot.memberships.item_count,
    }


def _print_plan_preview(plan: Any, manifest_path: str, retained_plan: Optional[str]) -> None:
    action_counts: Dict[str, int] = {}
    existing_conflicts = 0
    for decision in plan.conflict_decisions:
        action_counts[decision.action] = action_counts.get(decision.action, 0) + 1
        if decision.target_exists:
            existing_conflicts += 1

    print("\n" + "=" * 60)
    print("RESTORE READY")
    print("=" * 60)
    print(f"Backup Manifest: {manifest_path}")
    print(f"Target Account: {plan.target.aws_account_id}")
    print(f"Target Asset Region: {plan.target.asset_region}")
    print(f"Planned Bundles: {len(plan.bundles)}")
    print(f"Existing Target Conflicts: {existing_conflicts}")
    if action_counts:
        rendered_actions = ", ".join(
            "{0}: {1}".format(action.replace("_", " ").title(), count)
            for action, count in sorted(action_counts.items())
        )
        print(f"Target Actions: {rendered_actions}")
    if retained_plan:
        print(f"Retained Plan: {retained_plan}")

    print("\nResources selected:")
    combined = _resource_type_counts(plan)
    for identity_type, count in _identity_counts(plan).items():
        combined[identity_type] = count
    if combined:
        for resource_type, count in combined.items():
            print(f"  {resource_type.replace('_', ' ').title()}: {count}")
    else:
        print("  None")

    if plan.warnings:
        print("\nWarnings reviewed:")
        for warning in plan.warnings:
            print(f"  - {warning}")


def _format_resource_results(resource_counts: Dict[str, Dict[str, int]]) -> str:
    headers = ("Type", "Successful", "Failed", "Skipped", "Not attempted", "Pending")
    rows = []
    for resource_type, counts in resource_counts.items():
        rows.append(
            (
                resource_type.replace("_", " ").title(),
                str(counts.get("successful", 0)),
                str(counts.get("failed", 0)),
                str(counts.get("skipped", 0)),
                str(counts.get("not_attempted", 0)),
                str(counts.get("pending", 0)),
            )
        )
    totals = {
        field: sum(counts.get(field, 0) for counts in resource_counts.values())
        for field in _RESOURCE_OUTCOMES
    }
    rows.append(
        (
            "TOTAL",
            str(totals["successful"]),
            str(totals["failed"]),
            str(totals["skipped"]),
            str(totals["not_attempted"]),
            str(totals["pending"]),
        )
    )
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in rows))
        for index in range(len(headers))
    ]

    def render(row: Any) -> str:
        return "  ".join(
            value.ljust(widths[index]) if index == 0 else value.rjust(widths[index])
            for index, value in enumerate(row)
        )

    return "\n".join(
        [render(headers), "  ".join("-" * width for width in widths)]
        + [render(row) for row in rows]
    )


def _print_restore_completion(report: Any, report_directory: str) -> None:
    print("\n" + "=" * 60)
    print("RESTORE COMPLETED: {0}".format(report.overall_status.upper()))
    print("=" * 60)
    print(f"Restore ID: {report.restore_id}")
    print(
        "Report: {0}".format(Path(report_directory) / "restore-{0}.json".format(report.restore_id))
    )
    resource_counts = report.summary.get("resource_counts", {})
    if resource_counts:
        print("\nResource results:")
        print(_format_resource_results(resource_counts))
        if any(
            counts.get("failed", 0)
            for resource_type, counts in resource_counts.items()
            if resource_type not in ("user", "group", "membership")
        ):
            print(
                "\n* Asset failures are canonical resources in unsuccessful bundle jobs; "
                "QuickSight does not return complete per-member terminal outcomes."
            )


def _report_exit_code(status: str) -> int:
    return _STATUS_EXIT_CODES.get(status, 1)


def _print_progress(message: str) -> None:
    print("[restore] {0}".format(message), flush=True)


def _run_normal_restore(args: argparse.Namespace, loader: RestoreConfigLoader) -> int:
    config = loader.load(args.config, backup_manifest=args.backup_manifest)
    orchestrator = RestoreOrchestrator(config, progress_callback=_print_progress)
    temporary_directory = None
    retained_plan = None
    if args.plan_output:
        raw_plan_path = Path(args.plan_output).expanduser()
        retained_plan = str(
            raw_plan_path.resolve()
            if raw_plan_path.is_absolute()
            else (Path(config.config_directory) / raw_plan_path).resolve()
        )
        plan_path = retained_plan
    else:
        temporary_directory = tempfile.TemporaryDirectory(
            prefix="quicksight-restore-",
            dir=config.config_directory,
        )
        plan_path = str(Path(temporary_directory.name) / "restore-plan.json")

    try:
        plan = orchestrator.plan(
            output_path=plan_path,
            backup_date=config.source_backup.backup_date,
            bundle_keys=config.source_backup.bundle_keys,
        )
        _print_plan_preview(plan, args.backup_manifest, retained_plan)
        if args.dry_run:
            print("\nDry run completed. The target account was not changed.")
            return 0

        if not args.yes:
            if not sys.stdin.isatty():
                raise RestoreConfigurationError(
                    "Interactive confirmation is unavailable; rerun with --yes after reviewing "
                    "the dry-run preview"
                )
            response = input("\nExecute this restore? [y/N]: ").strip().lower()
            if response not in ("y", "yes"):
                print("Restore cancelled. The target account was not changed.")
                return 0

        report = orchestrator.run(plan_path)
        _print_restore_completion(report, config.restore.report_directory)
        return _report_exit_code(report.overall_status)
    finally:
        if temporary_directory is not None:
            temporary_directory.cleanup()


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None and (not args.config or not args.backup_manifest):
        parser.error("the normal restore path requires --manifest and --config")

    try:
        loader = RestoreConfigLoader()
        if args.command is None:
            return _run_normal_restore(args, loader)

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
        return _report_exit_code(report.overall_status)
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
