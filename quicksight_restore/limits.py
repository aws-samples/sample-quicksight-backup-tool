"""Service limits and sealed transport identifiers for restore operations."""

# StartAssetBundleImportJob accepts at most 20 MiB when the bundle is supplied
# inline through AssetBundleImportSource.Body. Larger Part 1 artifacts must fail
# planning until a target-owned, version-pinned staging transport is implemented.
INLINE_IMPORT_MAX_BYTES = 20 * 1024 * 1024

IMPORT_TRANSPORT_INLINE = "inline_body"
IMPORT_TRANSPORT_NONE = "none"

IMPORT_ACTION = "import"
SKIP_POLICY_ACTION = "skip_policy"

MAX_API_ATTEMPTS = 5
RETRY_BASE_SECONDS = 1.0
RETRY_CAP_SECONDS = 8.0

# Local operator-authored and generated restore artifacts are read through a
# descriptor-bound bounded reader. These caps prevent metadata races from
# turning nominal preflight checks into unbounded parser reads.
MAX_CONFIG_BYTES = 1 * 1024 * 1024
MAX_OVERRIDES_BYTES = 16 * 1024 * 1024
MAX_PLAN_BYTES = 100 * 1024 * 1024
MAX_REPORT_BYTES = 50 * 1024 * 1024
