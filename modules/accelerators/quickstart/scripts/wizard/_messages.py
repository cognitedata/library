"""
User-facing prompt labels and static message strings for the Quickstart DP setup wizard.

Keeping all terminal text here makes it easy to review the wizard's "script" in one
place and update wording without touching logic code.
"""
from __future__ import annotations

# Section / banner titles

SEC_CDF_PROJECT      = "CDF Project"
SEC_APP_OWNER        = "File Annotation: ApplicationOwner"
SEC_GROUP_SOURCE_IDS = "Group Source IDs"
SEC_OPENID_SECRET    = "OpenID Client Secret"
SEC_POST_VERIFY      = "Post-write verification"
BANNER_PENDING       = "PENDING CHANGES — review before applying"

# Prompt labels

PROMPT_ENV           = "Target environment"
PROMPT_PROJECT       = "CDF project name"
PROMPT_APP_OWNER     = "ApplicationOwner email(s)"
PROMPT_SHARED_GROUP  = "Use the same GROUP_SOURCE_ID for all modules?"
PROMPT_APPLY         = "Apply all changes above?"

# Environment selection

ENV_SELECT_INTRO = (
    "\nThis wizard configures exactly one environment at a time.\n"
    "Valid choices: dev, prod, staging\n"
    "  (default: dev — press Enter to accept)"
)

# Group Source IDs section

# Use str.format(n=..., module_names=...) at call site.
GROUP_SOURCE_INTRO = (
    "  {n} modules in Quickstart Deployment Pack need a groupSourceId — the external ID\n"
    "  of the IdP group (e.g. Azure AD) that controls CDF access for that module.\n"
    "  Modules: {module_names}\n"
)

GROUP_SOURCE_PER_MODULE_HINT = (
    "\n  Prompting for each module (already-set values shown with keep/replace option).\n"
)

# Hints shown in main()

HINT_APP_OWNER_FORMAT = "  Accepts one or more comma-separated email addresses."
HINT_CURRENT_VALUE    = "  Current value : {value}"
HINT_SQL_PENDING      = "  FILE_ANNOTATION mode will be enabled (COMMON MODE block commented out)."
HINT_BACKUPS          = "  Backups : timestamped .bak files created for each modified file."

# Status / warning messages shown in main()

WARN_ABORTED = "Aborted — no files were modified."
MSG_DONE     = "\nDone."

# Post-write verification step labels

VERIFY_BUILD_START = "  [1/3] Verifying build ..."
VERIFY_BUILD_OK    = "  [1/3] Build succeeded."
VERIFY_BUILD_FAIL  = "  [1/3] Build FAILED."
VERIFY_DRY_START   = "\n  [2/3] Running dry-run deploy ..."
VERIFY_DRY_OK      = "  [2/3] Dry-run deploy succeeded."
VERIFY_DRY_FAIL    = "  [2/3] Dry-run deploy failed — not proceeding to live deploy."
VERIFY_LIVE_OK     = "  [3/3] Live deploy complete."
VERIFY_LIVE_SKIP   = "  [3/3] Live deploy skipped."
