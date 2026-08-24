"""Permission overrides generated from reviewed target Quick Sight principals."""

from typing import Dict, Iterable, List, Sequence, Set, Tuple

ANALYSIS_ACTIONS = [
    "quicksight:RestoreAnalysis",
    "quicksight:UpdateAnalysisPermissions",
    "quicksight:DeleteAnalysis",
    "quicksight:DescribeAnalysisPermissions",
    "quicksight:QueryAnalysis",
    "quicksight:DescribeAnalysis",
    "quicksight:UpdateAnalysis",
]

DASHBOARD_ACTIONS = [
    "quicksight:DescribeDashboard",
    "quicksight:ListDashboardVersions",
    "quicksight:UpdateDashboardPermissions",
    "quicksight:QueryDashboard",
    "quicksight:UpdateDashboard",
    "quicksight:DeleteDashboard",
    "quicksight:DescribeDashboardPermissions",
    "quicksight:UpdateDashboardPublishedVersion",
]

DATA_SET_ACTIONS = [
    "quicksight:DeleteDataSet",
    "quicksight:UpdateDataSetPermissions",
    "quicksight:PutDataSetRefreshProperties",
    "quicksight:CreateRefreshSchedule",
    "quicksight:CancelIngestion",
    "quicksight:PassDataSet",
    "quicksight:ListRefreshSchedules",
    "quicksight:UpdateRefreshSchedule",
    "quicksight:DeleteRefreshSchedule",
    "quicksight:DescribeDataSetRefreshProperties",
    "quicksight:DescribeDataSet",
    "quicksight:CreateIngestion",
    "quicksight:DescribeRefreshSchedule",
    "quicksight:ListIngestions",
    "quicksight:DescribeDataSetPermissions",
    "quicksight:UpdateDataSet",
    "quicksight:DeleteDataSetRefreshProperties",
    "quicksight:DescribeIngestion",
]

DATA_SOURCE_ACTIONS = [
    "quicksight:DescribeDataSource",
    "quicksight:DescribeDataSourcePermissions",
    "quicksight:PassDataSource",
    "quicksight:UpdateDataSource",
    "quicksight:DeleteDataSource",
    "quicksight:UpdateDataSourcePermissions",
]

THEME_ACTIONS = [
    "quicksight:DescribeTheme",
    "quicksight:DescribeThemePermissions",
    "quicksight:UpdateTheme",
    "quicksight:DeleteTheme",
    "quicksight:UpdateThemePermissions",
    "quicksight:ListThemeVersions",
    "quicksight:ListThemeAliases",
    "quicksight:DescribeThemeAlias",
    "quicksight:CreateThemeAlias",
    "quicksight:UpdateThemeAlias",
    "quicksight:DeleteThemeAlias",
]

FOLDER_ACTIONS = [
    "quicksight:DescribeFolder",
    "quicksight:DescribeFolderPermissions",
    "quicksight:UpdateFolderPermissions",
    "quicksight:DeleteFolder",
    "quicksight:UpdateFolder",
    "quicksight:CreateFolderMembership",
    "quicksight:DeleteFolderMembership",
    "quicksight:ListFolderMembers",
]

# resource type -> (OverridePermissions section, identifier field, owner actions)
PERMISSION_OVERRIDE_SPECS: Dict[str, Tuple[str, str, List[str]]] = {
    "analysis": ("Analyses", "AnalysisIds", ANALYSIS_ACTIONS),
    "dashboard": ("Dashboards", "DashboardIds", DASHBOARD_ACTIONS),
    "dataset": ("DataSets", "DataSetIds", DATA_SET_ACTIONS),
    "datasource": ("DataSources", "DataSourceIds", DATA_SOURCE_ACTIONS),
    "theme": ("Themes", "ThemeIds", THEME_ACTIONS),
    "folder": ("Folders", "FolderIds", FOLDER_ACTIONS),
}


def build_override_permissions(
    selected_resources: Iterable[str], target_principals: Sequence[str]
) -> Dict[str, List[dict]]:
    """Build bundle-scoped wildcard owner permissions for selected resource types."""

    principals = sorted(set(target_principals))
    if not principals:
        return {}
    resource_types: Set[str] = set()
    for resource_key in selected_resources:
        resource_type, separator, resource_id = resource_key.partition("/")
        if not separator or not resource_id or resource_type not in PERMISSION_OVERRIDE_SPECS:
            continue
        resource_types.add(resource_type)

    result: Dict[str, List[dict]] = {}
    for resource_type in sorted(resource_types):
        section, identifier_field, actions = PERMISSION_OVERRIDE_SPECS[resource_type]
        result[section] = [
            {
                identifier_field: ["*"],
                "Permissions": {
                    "Principals": principals,
                    "Actions": list(actions),
                },
            }
        ]
    return result
