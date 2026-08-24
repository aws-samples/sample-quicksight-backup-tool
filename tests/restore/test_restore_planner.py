import copy
import json

import pytest

from quicksight_restore.models.contracts import sha256_json
from quicksight_restore.models.errors import RestorePlanningError
from quicksight_restore.services.planner import QuickSightTargetInventory, RestorePlanner
from conftest import make_bundle, make_manifest, make_member


TARGET_PRINCIPAL = "arn:aws:quicksight:us-east-1:222222222222:user/default/reviewer"
SOURCE_PRINCIPAL = "arn:aws:quicksight:us-east-1:111111111111:user/default/source-user"


class Inventory:
    def __init__(self, resources=()):
        self.resources = set(resources)
        self.calls = []

    def list_resources(self, resource_types):
        self.calls.append(set(resource_types))
        return self.resources


class IdentityClient:
    def __init__(self):
        self.calls = []

    def describe_user(self, **request):
        self.calls.append(("describe_user", request))
        return {"User": {"Arn": TARGET_PRINCIPAL}}

    def __getattr__(self, name):
        if name.startswith(("create", "register", "start", "update", "delete")):
            raise AssertionError("planning attempted a mutating API: " + name)
        raise AttributeError(name)


def duplicate_manifest(conflicting=False):
    shared_a = make_member("dataset/shared.json", "dataset", "shared", {"value": 1})
    shared_b = make_member(
        "dataset/shared.json",
        "dataset",
        "shared",
        {"value": 2 if conflicting else 1},
    )
    dependency_bundle = make_bundle(
        "quicksight-backups/2026/08/17/datasources/datasources-010101.zip",
        "datasources",
        [
            make_member("datasource/source.json", "datasource", "source"),
            shared_a,
        ],
        body=b"source-bundle",
    )
    dedicated_bundle = make_bundle(
        "quicksight-backups/2026/08/17/datasets/datasets-010102.zip",
        "datasets",
        [shared_b],
        body=b"dataset-bundle",
    )
    return make_manifest([dedicated_bundle, dependency_bundle])


def bundle_by_root(plan, root_type):
    return next(item for item in plan.bundles if item.root_type == root_type)


def test_matching_duplicates_preserve_atomic_archives_deterministically(restore_config):
    inventory = Inventory()
    planner = RestorePlanner(restore_config, target_inventory=inventory)
    first = planner.build_plan(duplicate_manifest())
    second = planner.build_plan(duplicate_manifest())
    decision = first.duplicate_decisions[0]
    assert decision.resource_key == "dataset/shared"
    assert "/datasets/" in decision.selected_bundle_key
    assert "atomically" in decision.reason
    assert [item.root_type for item in first.bundles] == ["datasources", "datasets"]
    source_bundle = bundle_by_root(first, "datasources")
    assert set(source_bundle.selected_resources) == {
        "datasource/source",
        "dataset/shared",
    }
    assert source_bundle.omitted_member_names == []
    assert source_bundle.materialization_mode == "original"
    assert first.plan_digest == second.plan_digest
    assert first.verify_digest()
    first.bundles[0].selected_member_names.append("tampered.json")
    assert not first.verify_digest()


def test_conflicting_duplicate_hashes_are_a_planning_error(restore_config):
    planner = RestorePlanner(restore_config, target_inventory=Inventory())
    with pytest.raises(RestorePlanningError, match="Conflicting duplicate definitions"):
        planner.build_plan(duplicate_manifest(conflicting=True))


def test_target_conflict_policies_preserve_atomic_archives(restore_config):
    manifest = duplicate_manifest()
    restore_config.restore.conflict_policy = "skip"
    with pytest.raises(RestorePlanningError, match="member-level rewriting"):
        RestorePlanner(
            restore_config, target_inventory=Inventory({"datasource/source"})
        ).build_plan(manifest)

    plan = RestorePlanner(
        restore_config,
        target_inventory=Inventory({"datasource/source", "dataset/shared"}),
    ).build_plan(manifest)
    assert len(plan.bundles) == 2
    assert all(item.execution_action == "skip_policy" for item in plan.bundles)
    assert all(item.import_transport == "none" for item in plan.bundles)
    assert all(item.import_overrides == {} for item in plan.bundles)
    assert all(item.action == "skip" for item in plan.conflict_decisions)

    restore_config.restore.conflict_policy = "fail"
    with pytest.raises(RestorePlanningError, match="Target conflict"):
        RestorePlanner(
            restore_config, target_inventory=Inventory({"datasource/source"})
        ).build_plan(manifest)

    restore_config.restore.conflict_policy = "update"
    plan = RestorePlanner(
        restore_config, target_inventory=Inventory({"datasource/source"})
    ).build_plan(manifest)
    assert any(item.action == "update" for item in plan.conflict_decisions)

    restore_config.restore.conflict_policy = "prefix"
    restore_config.restore.conflict_prefix = "recovered-"
    plan = RestorePlanner(
        restore_config, target_inventory=Inventory({"datasource/source"})
    ).build_plan(manifest)
    assert all(item.action == "prefix" for item in plan.conflict_decisions)
    assert {item.destination_resource_key for item in plan.conflict_decisions} == {
        "datasource/recovered-source",
        "dataset/recovered-shared",
    }
    assert all(
        item.import_overrides["OverrideParameters"]["ResourceIdOverrideConfiguration"][
            "PrefixForAllResources"
        ]
        == "recovered-"
        for item in plan.bundles
    )


def test_target_principals_generate_bundle_scoped_owner_permissions(restore_config):
    restore_config.restore.target_principals = [TARGET_PRINCIPAL]
    plan = RestorePlanner(restore_config, target_inventory=Inventory()).build_plan(
        duplicate_manifest()
    )

    source_overrides = bundle_by_root(plan, "datasources").import_overrides["OverridePermissions"]
    assert set(source_overrides) == {"DataSources", "DataSets"}
    assert len(source_overrides["DataSources"]) == 1
    assert len(source_overrides["DataSets"]) == 1
    source_permissions = source_overrides["DataSources"][0]
    assert source_permissions["DataSourceIds"] == ["*"]
    assert source_permissions["Permissions"]["Principals"] == [TARGET_PRINCIPAL]
    assert "quicksight:UpdateDataSourcePermissions" in source_permissions["Permissions"]["Actions"]
    assert source_overrides["DataSets"][0]["DataSetIds"] == ["*"]

    dataset_overrides = bundle_by_root(plan, "datasets").import_overrides["OverridePermissions"]
    assert set(dataset_overrides) == {"DataSets"}
    assert len(dataset_overrides["DataSets"]) == 1
    dataset_permissions = dataset_overrides["DataSets"][0]
    assert dataset_permissions["DataSetIds"] == ["*"]
    assert dataset_permissions["Permissions"]["Principals"] == [TARGET_PRINCIPAL]


def test_prefix_target_principals_use_bundle_type_wildcards(restore_config):
    restore_config.restore.target_principals = [TARGET_PRINCIPAL]
    restore_config.restore.conflict_policy = "prefix"
    restore_config.restore.conflict_prefix = "reviewed-"

    plan = RestorePlanner(restore_config, target_inventory=Inventory()).build_plan(
        duplicate_manifest()
    )

    source_overrides = bundle_by_root(plan, "datasources").import_overrides
    assert (
        source_overrides["OverrideParameters"]["ResourceIdOverrideConfiguration"][
            "PrefixForAllResources"
        ]
        == "reviewed-"
    )
    assert set(source_overrides["OverridePermissions"]) == {
        "DataSources",
        "DataSets",
    }
    assert source_overrides["OverridePermissions"]["DataSources"][0]["DataSourceIds"] == ["*"]
    assert source_overrides["OverridePermissions"]["DataSets"][0]["DataSetIds"] == ["*"]

    dataset_overrides = bundle_by_root(plan, "datasets").import_overrides
    assert set(dataset_overrides["OverridePermissions"]) == {"DataSets"}
    assert dataset_overrides["OverridePermissions"]["DataSets"][0]["DataSetIds"] == ["*"]


def test_source_principals_and_ambiguous_permission_configuration_are_rejected(
    restore_config,
):
    explicit = {
        "OverridePermissions": {
            "DataSources": [
                {
                    "DataSourceIds": ["source"],
                    "Permissions": {
                        "Principals": [SOURCE_PRINCIPAL],
                        "Actions": ["quicksight:DescribeDataSource"],
                    },
                }
            ]
        }
    }
    planner = RestorePlanner(restore_config, target_inventory=Inventory())
    with pytest.raises(RestorePlanningError, match="different AWS account"):
        planner.build_plan(duplicate_manifest(), explicit)

    restore_config.restore.target_principals = [TARGET_PRINCIPAL]
    explicit["OverridePermissions"]["DataSources"][0]["Permissions"]["Principals"] = [
        TARGET_PRINCIPAL
    ]
    with pytest.raises(RestorePlanningError, match="either restore.target_principals"):
        RestorePlanner(restore_config, target_inventory=Inventory()).build_plan(
            duplicate_manifest(), explicit
        )


def test_native_overrides_are_scoped_to_each_complete_archive(restore_config):
    permissions = {
        "Principals": [TARGET_PRINCIPAL],
        "Actions": ["quicksight:DescribeDataSource"],
    }
    overrides = {
        "OverrideParameters": {
            "DataSources": [{"DataSourceId": "source", "Name": "restored-source"}],
            "DataSets": [{"DataSetId": "shared", "Name": "restored-data"}],
        },
        "OverridePermissions": {
            "DataSources": [{"DataSourceIds": ["source"], "Permissions": permissions}],
            "DataSets": [
                {
                    "DataSetIds": ["shared"],
                    "Permissions": {
                        "Principals": [TARGET_PRINCIPAL],
                        "Actions": ["quicksight:DescribeDataSet"],
                    },
                }
            ],
        },
        "OverrideTags": {
            "DataSources": [
                {"DataSourceIds": ["source"], "Tags": [{"Key": "kind", "Value": "source"}]}
            ],
            "DataSets": [{"DataSetIds": ["shared"], "Tags": [{"Key": "kind", "Value": "data"}]}],
        },
    }
    plan = RestorePlanner(restore_config, target_inventory=Inventory()).build_plan(
        duplicate_manifest(), overrides
    )
    source = bundle_by_root(plan, "datasources").import_overrides
    data = bundle_by_root(plan, "datasets").import_overrides
    assert set(source["OverrideParameters"]) == {"DataSources", "DataSets"}
    assert set(source["OverridePermissions"]) == {"DataSources", "DataSets"}
    assert set(source["OverrideTags"]) == {"DataSources", "DataSets"}
    assert set(data["OverrideParameters"]) == {"DataSets"}
    assert set(data["OverridePermissions"]) == {"DataSets"}
    assert set(data["OverrideTags"]) == {"DataSets"}


def test_prefix_and_exact_identifier_shapes_fail_during_planning(restore_config):
    planner = RestorePlanner(restore_config, target_inventory=Inventory())
    with pytest.raises(RestorePlanningError, match="requires restore.conflict_policy=prefix"):
        planner.build_plan(
            duplicate_manifest(),
            {
                "OverrideParameters": {
                    "ResourceIdOverrideConfiguration": {"PrefixForAllResources": "unsafe-"}
                }
            },
        )

    restore_config.restore.conflict_policy = "prefix"
    restore_config.restore.conflict_prefix = "reviewed-"
    with pytest.raises(RestorePlanningError, match="conflicts with restore.conflict_prefix"):
        RestorePlanner(restore_config, target_inventory=Inventory()).build_plan(
            duplicate_manifest(),
            {
                "OverrideParameters": {
                    "ResourceIdOverrideConfiguration": {"PrefixForAllResources": "different-"}
                }
            },
        )

    restore_config.restore.conflict_policy = "update"
    restore_config.restore.conflict_prefix = None
    with pytest.raises(RestorePlanningError, match="require AnalysisId"):
        planner.build_plan(
            duplicate_manifest(),
            {"OverrideParameters": {"Analyses": [{"AnalyseId": "bad"}]}},
        )
    with pytest.raises(RestorePlanningError, match="require ScheduleId"):
        planner.build_plan(
            duplicate_manifest(),
            {"OverrideParameters": {"RefreshSchedules": [{"DataSetId": "shared"}]}},
        )


def test_plan_is_read_only_and_validates_principals_and_override_references(restore_config):
    restore_config.restore.target_principals = [TARGET_PRINCIPAL]
    restore_config.restore.validate_target_principals = True
    identity = IdentityClient()
    inventory = Inventory()
    planner = RestorePlanner(
        restore_config,
        target_quicksight_client=object(),
        target_identity_quicksight_client=identity,
        target_inventory=inventory,
    )
    overrides = {
        "OverrideParameters": {
            "DataSources": [{"DataSourceId": "source", "Name": "restored-source"}]
        },
        "OverrideTags": {},
        "OverrideValidationStrategy": {"StrictModeForAllResources": True},
    }
    plan = planner.build_plan(duplicate_manifest(), overrides)
    assert plan.overrides == overrides
    assert identity.calls[0][0] == "describe_user"
    assert inventory.calls

    bad = copy.deepcopy(overrides)
    bad["OverrideParameters"]["DataSources"][0]["DataSourceId"] = "missing"
    with pytest.raises(RestorePlanningError, match="unselected resource"):
        planner.build_plan(duplicate_manifest(), bad)


def test_prefix_checks_effective_destination_not_source_id(restore_config):
    restore_config.restore.conflict_policy = "prefix"
    restore_config.restore.conflict_prefix = "recovered-"
    manifest = duplicate_manifest()

    with pytest.raises(RestorePlanningError, match="Prefixed target conflict"):
        RestorePlanner(
            restore_config,
            target_inventory=Inventory({"dataset/recovered-shared"}),
        ).build_plan(manifest)

    plan = RestorePlanner(
        restore_config,
        target_inventory=Inventory({"dataset/shared", "datasource/source"}),
    ).build_plan(manifest)
    assert all(not item.target_exists for item in plan.conflict_decisions)


def test_dependency_graph_orders_provider_before_dependent(restore_config):
    dependent = make_bundle(
        "quicksight-backups/2026/08/17/datasets/datasets_bundle_1-010101.zip",
        "datasets",
        [
            make_member(
                "dataset/report.json",
                "dataset",
                "report",
                dependencies=["datasource/source"],
            )
        ],
    )
    provider = make_bundle(
        "quicksight-backups/2026/08/17/datasets/datasets_bundle_2-010102.zip",
        "datasets",
        [make_member("datasource/source.json", "datasource", "source")],
    )
    manifest = make_manifest([dependent, provider])

    plan = RestorePlanner(restore_config, target_inventory=Inventory()).build_plan(manifest)

    assert [bundle.key for bundle in plan.bundles] == [provider.key, dependent.key]
    assert [bundle.order for bundle in plan.bundles] == [0, 1]
    assert plan.bundles[0].prerequisite_bundle_keys == []
    assert plan.bundles[1].prerequisite_bundle_keys == [provider.key]


def test_missing_dependency_requires_selected_or_existing_target(restore_config):
    dependent = make_bundle(
        "quicksight-backups/2026/08/17/datasets/datasets-010101.zip",
        "datasets",
        [
            make_member(
                "dataset/report.json",
                "dataset",
                "report",
                dependencies=["datasource/source"],
            )
        ],
    )
    manifest = make_manifest([dependent])

    with pytest.raises(RestorePlanningError, match="Missing dependency datasource/source"):
        RestorePlanner(restore_config, target_inventory=Inventory()).build_plan(manifest)

    plan = RestorePlanner(
        restore_config, target_inventory=Inventory({"datasource/source"})
    ).build_plan(manifest)
    assert [bundle.key for bundle in plan.bundles] == [dependent.key]


def test_dependency_cycles_fail_with_archive_diagnostics(restore_config):
    first = make_bundle(
        "quicksight-backups/2026/08/17/datasets/datasets_bundle_1-010101.zip",
        "datasets",
        [
            make_member(
                "dataset/one.json",
                "dataset",
                "one",
                dependencies=["dataset/two"],
            )
        ],
    )
    second = make_bundle(
        "quicksight-backups/2026/08/17/datasets/datasets_bundle_2-010102.zip",
        "datasets",
        [
            make_member(
                "dataset/two.json",
                "dataset",
                "two",
                dependencies=["dataset/one"],
            )
        ],
    )

    with pytest.raises(RestorePlanningError, match="Dependency cycle"):
        RestorePlanner(restore_config, target_inventory=Inventory()).build_plan(
            make_manifest([first, second])
        )


def test_planner_rejects_ancillary_members_and_invalid_prefixed_ids(restore_config):
    unsupported = make_bundle(
        "quicksight-backups/2026/08/17/datasets/datasets-010101.zip",
        "datasets",
        [make_member("README.md", "unknown", "README.md", known=False)],
    )
    with pytest.raises(RestorePlanningError, match="Unsupported ancillary"):
        RestorePlanner(restore_config, target_inventory=Inventory()).build_plan(
            make_manifest([unsupported])
        )

    restore_config.restore.conflict_policy = "prefix"
    restore_config.restore.conflict_prefix = "recovered-"
    too_long = "x" * 510
    member = make_member("analysis/{0}.json".format(too_long), "analysis", too_long)
    bundle = make_bundle(
        "quicksight-backups/2026/08/17/analyses/analyses-010101.zip",
        "analyses",
        [member],
    )
    with pytest.raises(RestorePlanningError, match="Prefixed resource ID is invalid"):
        RestorePlanner(restore_config, target_inventory=Inventory()).build_plan(
            make_manifest([bundle])
        )


class TargetInventoryClient:
    def __init__(self):
        self.refresh_calls = []

    def list_vpc_connections(self, **request):
        return {
            "VPCConnectionSummaries": [{"VPCConnectionId": "vpc-one"}],
        }

    def list_data_sets(self, **request):
        return {"DataSetSummaries": [{"DataSetId": "set-one"}]}

    def list_refresh_schedules(self, **request):
        self.refresh_calls.append(request)
        return {"RefreshSchedules": [{"ScheduleId": "daily"}]}


def test_target_inventory_includes_vpc_connections_and_scoped_refresh_schedules():
    client = TargetInventoryClient()
    inventory = QuickSightTargetInventory(client, "222222222222")

    resources = inventory.list_resources({"vpcconnection", "refreshschedule"})

    assert resources == {
        "dataset/set-one",
        "vpcconnection/vpc-one",
        "refreshschedule/set-one/daily",
    }
    assert client.refresh_calls == [{"AwsAccountId": "222222222222", "DataSetId": "set-one"}]


def test_dependencies_from_multiple_source_scopes_are_rejected(restore_config):
    first = make_member(
        "dataset/one.json",
        "dataset",
        "one",
        dependency_scopes=["aws:us-east-1:111111111111"],
    )
    second = make_member(
        "dataset/two.json",
        "dataset",
        "two",
        dependency_scopes=["aws:us-west-2:333333333333"],
    )
    bundle = make_bundle(
        "quicksight-backups/2026/08/17/datasets/datasets-010101.zip",
        "datasets",
        [first, second],
    )

    with pytest.raises(RestorePlanningError, match="multiple source scopes"):
        RestorePlanner(restore_config, target_inventory=Inventory()).build_plan(
            make_manifest([bundle])
        )


def test_planner_rejects_bundle_above_inline_import_limit_before_target_calls(
    restore_config,
):
    from quicksight_restore.limits import INLINE_IMPORT_MAX_BYTES

    bundle = make_bundle(
        "quicksight-backups/2026/08/17/datasets/datasets-010101.zip",
        "datasets",
        [make_member("dataset/large.json", "dataset", "large")],
    )
    bundle.size = INLINE_IMPORT_MAX_BYTES + 1
    inventory = Inventory()

    with pytest.raises(RestorePlanningError, match="target-owned S3 staging"):
        RestorePlanner(restore_config, target_inventory=inventory).build_plan(
            make_manifest([bundle])
        )

    assert inventory.calls == []


def test_plan_persistence_requires_a_current_valid_seal(restore_config, tmp_path):
    plan = RestorePlanner(restore_config, target_inventory=Inventory()).build_plan(
        duplicate_manifest()
    )
    plan.warnings.append("changed after review")

    with pytest.raises(RestorePlanningError, match="digest verification failed"):
        RestorePlanner.save_plan(plan, str(tmp_path / "stale-plan.json"))
    assert not (tmp_path / "stale-plan.json").exists()


def test_plan_persistence_rejects_mismatched_plan_id(restore_config, tmp_path):
    plan = RestorePlanner(restore_config, target_inventory=Inventory()).build_plan(
        duplicate_manifest()
    )
    plan.plan_id = "plan-" + "0" * 20

    with pytest.raises(RestorePlanningError, match="ID does not match"):
        RestorePlanner.save_plan(plan, str(tmp_path / "wrong-id.json"))


def test_plan_artifact_digest_covers_created_at(restore_config, tmp_path):
    plan = RestorePlanner(restore_config, target_inventory=Inventory()).build_plan(
        duplicate_manifest()
    )
    path = tmp_path / "created-at-plan.json"
    RestorePlanner.save_plan(plan, str(path))
    original_artifact_digest = plan.artifact_digest

    plan.created_at = "2030-01-01T00:00:00+00:00"
    assert plan.calculate_digest() == plan.plan_digest
    assert plan.calculate_artifact_digest() != original_artifact_digest

    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["created_at"] = plan.created_at
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(RestorePlanningError, match="artifact digest verification failed"):
        RestorePlanner.load_plan(str(path))


def test_plan_artifact_digest_covers_manifest_generated_at(restore_config, tmp_path):
    plan = RestorePlanner(restore_config, target_inventory=Inventory()).build_plan(
        duplicate_manifest()
    )
    path = tmp_path / "manifest-generated-at-plan.json"
    RestorePlanner.save_plan(plan, str(path))
    original_artifact_digest = plan.artifact_digest

    plan.manifest.generated_at = "2030-01-01T00:00:00+00:00"
    assert plan.calculate_digest() == plan.plan_digest
    assert plan.calculate_artifact_digest() != original_artifact_digest

    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["manifest"]["generated_at"] = plan.manifest.generated_at
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(RestorePlanningError, match="artifact digest verification failed"):
        RestorePlanner.load_plan(str(path))


def test_plan_load_verifies_raw_digest_before_defaults(restore_config, tmp_path):
    plan = RestorePlanner(restore_config, target_inventory=Inventory()).build_plan(
        duplicate_manifest()
    )
    path = tmp_path / "plan.json"
    RestorePlanner.save_plan(plan, str(path))
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw.pop("warnings")
    artifact_payload = copy.deepcopy(raw)
    artifact_payload.pop("artifact_digest")
    raw["artifact_digest"] = sha256_json(artifact_payload)
    path.write_text(json.dumps(raw), encoding="utf-8")

    with pytest.raises(RestorePlanningError, match="raw digest verification failed"):
        RestorePlanner.load_plan(str(path))


def test_plan_save_is_complete_no_clobber_and_round_trips(restore_config, tmp_path):
    plan = RestorePlanner(restore_config, target_inventory=Inventory()).build_plan(
        duplicate_manifest()
    )
    path = tmp_path / "plan.json"
    RestorePlanner.save_plan(plan, str(path))
    original = path.read_bytes()

    loaded = RestorePlanner.load_plan(str(path))
    assert loaded.to_dict() == plan.to_dict()
    with pytest.raises(RestorePlanningError, match="already exists"):
        RestorePlanner.save_plan(plan, str(path))
    assert path.read_bytes() == original
    assert not list(tmp_path.glob("*.tmp"))


def test_prefix_retargets_native_override_selectors_but_preserves_audit_input(
    restore_config,
):
    restore_config.restore.conflict_policy = "prefix"
    restore_config.restore.conflict_prefix = "reviewed-"
    permissions = {
        "Principals": [TARGET_PRINCIPAL],
        "Actions": ["quicksight:DescribeDataSource"],
    }
    overrides = {
        "OverrideParameters": {
            "DataSources": [
                {
                    "DataSourceId": "source",
                    "Name": "restored-source",
                    "DataSourceParameters": {"AthenaParameters": {"WorkGroup": "target-workgroup"}},
                }
            ]
        },
        "OverridePermissions": {
            "DataSources": [{"DataSourceIds": ["source"], "Permissions": permissions}]
        },
        "OverrideTags": {
            "DataSources": [
                {
                    "DataSourceIds": ["source"],
                    "Tags": [{"Key": "restore", "Value": "reviewed"}],
                }
            ]
        },
    }

    plan = RestorePlanner(restore_config, target_inventory=Inventory()).build_plan(
        duplicate_manifest(), overrides
    )

    assert plan.overrides == overrides
    assert plan.overrides["OverrideParameters"]["DataSources"][0]["DataSourceId"] == "source"
    compiled = bundle_by_root(plan, "datasources").import_overrides
    data_source = compiled["OverrideParameters"]["DataSources"][0]
    assert data_source["DataSourceId"] == "reviewed-source"
    assert data_source["Name"] == "restored-source"
    assert (
        data_source["DataSourceParameters"]["AthenaParameters"]["WorkGroup"] == "target-workgroup"
    )
    assert compiled["OverridePermissions"]["DataSources"][0]["DataSourceIds"] == ["reviewed-source"]
    assert compiled["OverrideTags"]["DataSources"][0]["DataSourceIds"] == ["reviewed-source"]
    assert compiled["OverrideParameters"]["ResourceIdOverrideConfiguration"] == {
        "PrefixForAllResources": "reviewed-"
    }


def test_singular_parameter_wildcard_is_rejected_but_plural_wildcards_expand(
    restore_config,
):
    restore_config.restore.conflict_policy = "prefix"
    restore_config.restore.conflict_prefix = "copy-"
    bundle = make_bundle(
        "quicksight-backups/2026/08/17/datasources/datasources-010101.zip",
        "datasources",
        [
            make_member("datasource/one.json", "datasource", "one"),
            make_member("datasource/two.json", "datasource", "two"),
        ],
    )
    manifest = make_manifest([bundle])
    planner = RestorePlanner(restore_config, target_inventory=Inventory())

    with pytest.raises(RestorePlanningError, match="does not support wildcard"):
        planner.build_plan(
            manifest,
            {"OverrideParameters": {"DataSources": [{"DataSourceId": "*"}]}},
        )

    overrides = {
        "OverridePermissions": {
            "DataSources": [
                {
                    "DataSourceIds": ["*"],
                    "Permissions": {
                        "Principals": [TARGET_PRINCIPAL],
                        "Actions": ["quicksight:DescribeDataSource"],
                    },
                }
            ]
        },
        "OverrideTags": {
            "DataSources": [
                {
                    "DataSourceIds": ["*"],
                    "Tags": [{"Key": "restore", "Value": "reviewed"}],
                }
            ]
        },
    }
    plan = planner.build_plan(manifest, overrides)
    compiled = plan.bundles[0].import_overrides

    assert compiled["OverridePermissions"]["DataSources"][0]["DataSourceIds"] == [
        "copy-one",
        "copy-two",
    ]
    assert compiled["OverrideTags"]["DataSources"][0]["DataSourceIds"] == [
        "copy-one",
        "copy-two",
    ]


def test_refresh_schedule_override_requires_exact_selected_composite(
    restore_config,
):
    bundle = make_bundle(
        "quicksight-backups/2026/08/17/datasets/datasets-010101.zip",
        "datasets",
        [
            make_member("dataset/set.json", "dataset", "set"),
            make_member(
                "refreshSchedule/set--refresh-schedule--daily.json",
                "refreshschedule",
                "set/daily",
                dependencies=["dataset/set"],
            ),
        ],
    )
    manifest = make_manifest([bundle])
    planner = RestorePlanner(restore_config, target_inventory=Inventory())
    overrides = {
        "OverrideParameters": {"RefreshSchedules": [{"DataSetId": "set", "ScheduleId": "daily"}]}
    }

    plan = planner.build_plan(manifest, overrides)

    assert plan.bundles[0].import_overrides["OverrideParameters"]["RefreshSchedules"] == [
        {"DataSetId": "set", "ScheduleId": "daily"}
    ]

    bad = copy.deepcopy(overrides)
    bad["OverrideParameters"]["RefreshSchedules"][0]["ScheduleId"] = "missing"
    with pytest.raises(
        RestorePlanningError,
        match="unselected resource: refreshschedule/set/missing",
    ):
        planner.build_plan(manifest, bad)
