import { createContext, useContext, useMemo, useState } from "react";

type Language = "en" | "ja";

type TranslationParams = Record<string, string | number>;

type I18nContextValue = {
  language: Language;
  setLanguage: (language: Language) => void;
  t: (key: string, params?: TranslationParams) => string;
};

const translations: Record<Language, Record<string, string>> = {
  en: {
    "language.english": "English",
    "language.japanese": "日本語",
    "app.language": "Language",
    "shared.project.label": "Project",
    "shared.help.button": "What does this mean?",
    "shared.modal.close": "Close",
    "shared.loader.title": "Loading",
    "shared.loader.description":
      "Working really hard pulling data from CDF. You can dismiss this message to watch partial data being loaded. Once all data is available this page will close by itself.",
    "shared.loader.dismissOnce": "Dismiss this time",
    "shared.loader.dismissForever": "Never show this loader again",
    "privateMode.badge": "Private Mode",
    "privateMode.clickToDisable": "Click to disable private mode",
    "nav.processing": "Processing",
    "nav.permissions": "Permissions",
    "nav.dataCatalog": "Data Catalog",
    "nav.healthChecks": "Health Checks",
    "nav.transformations": "Transformations",
    "apiError.showDetails": "Show details",
    "apiError.hideDetails": "Hide details",
    "apiError.section.api": "API",
    "apiError.section.request": "Request body",
    "apiError.section.details": "Details",
    "apiError.docsLink": "Open API documentation",
    "apiError.permissionsHint":
      "Permission requirements are listed at the top of each documentation page.",
    "apiError.networkHint":
      "This can happen if you are offline or the backend is not responding.",
    "processing.title": "Processing",
    "processing.subtitle": "Function execution concurrency for the last {hoursWindow} hours.",
    "processing.loading.functions": "Loading functions...",
    "processing.loading.stats": "Loading execution stats...",
    "processing.loading.runs": "Loading function executions...",
    "processing.loading.transformations": "Loading transformations...",
    "processing.loading.workflows": "Loading workflows...",
    "processing.loading.extractors": "Loading extraction pipelines...",
    "processing.progress.panelTitle": "Background requests",
    "processing.progress.functions.list":
      "Functions · POST /functions/list — {count} definitions loaded so far",
    "processing.progress.functions.runs":
      "Functions · POST /functions/…/calls/list — {current} of {total} functions queried ({remaining} remaining)",
    "processing.progress.transformations.list":
      "Transformations · GET /transformations — loading catalog",
    "processing.progress.transformations.jobs":
      "Transformations · GET /transformations/…/jobs — {current} of {total} transformations ({remaining} remaining)",
    "processing.progress.workflows.executions":
      "Workflows · POST /workflows/executions/list — {loaded} executions retrieved (paginating until complete)",
    "processing.progress.extractors.list":
      "Extraction pipelines · GET /extpipes — {loaded} configurations loaded",
    "processing.progress.extractors.runs":
      "Extraction pipelines · POST /extpipes/runs/list — {current} of {total} pipelines ({remaining} remaining)",
    "processing.progress.band.functions.list": "fn list · {count}",
    "processing.progress.band.functions.runs": "fn calls · {current}/{total}",
    "processing.progress.band.transformations.list": "tx catalog…",
    "processing.progress.band.transformations.jobs": "tx jobs · {current}/{total}",
    "processing.progress.band.workflows": "wf exec · {loaded}",
    "processing.progress.band.extractors.list": "ext list · {loaded}",
    "processing.progress.band.extractors.runs": "ext runs · {current}/{total}",
    "processing.error.runs": "Failed to load function executions.",
    "processing.function.defaultName": "Function {id}",
    "processing.error.transformations": "Failed to load transformations.",
    "processing.transformation.defaultName": "Transformation {id}",
    "processing.error.workflows": "Failed to load workflows.",
    "processing.error.extractors": "Failed to load extraction pipelines.",
    "processing.bubbles.loading": "Loading…",
    "processing.bubbles.empty": "No data in this window.",
    "processing.bubbles.ready": "Loaded",
    "processing.heatmap.title": "Scheduled starts heatmap",
    "processing.heatmap.description":
      "Cron-based scheduled starts across functions, transformations, and workflows for a full day.",
    "processing.heatmap.loading": "Loading schedules...",
    "processing.heatmap.error": "Failed to load schedules.",
    "processing.heatmap.empty": "No scheduled starts detected.",
    "processing.heatmap.legend.none": "0",
    "processing.heatmap.legend.one": "1",
    "processing.heatmap.legend.mid": "5",
    "processing.heatmap.legend.high": "10+",
    "processing.heatmap.legend.now": "Now (UTC)",
    "processing.heatmap.unknownFunction": "Unknown function",
    "processing.heatmap.unknownTransformation": "Unknown transformation",
    "processing.heatmap.hover.title": "{time} · {count}",
    "processing.heatmap.hover.none": "No scheduled starts",
    "processing.heatmap.pinned": "pinned",
    "processing.heatmap.unpin": "Unpin",
    "processing.heatmap.copyList": "Copy list",
    "processing.heatmap.help.title": "Scheduled starts heatmap",
    "processing.heatmap.help.subtitle": "Plan schedules evenly across the day.",
    "processing.heatmap.help.detailOne":
      "Spread out schedules as evenly as possible to avoid load spikes.",
    "processing.heatmap.help.detailTwo":
      "This heatmap shows a 24-hour overview of when work is scheduled, including empty times.",
    "processing.heatmap.help.detailThree":
      "Run duration is not included here, so factor it in when adjusting schedules.",
    "processing.extractor.seenEvents": "{count} seen events",
    "processing.extractor.started": "Extractor started",
    "processing.unavailable.functions": "Functions API is not available on this client.",
    "processing.time.utc": "UTC:",
    "processing.time.local": "{tzLabel}:",
    "processing.action.previous": "Previous",
    "processing.action.next": "Next",
    "processing.legend.functions": "Functions",
    "processing.legend.transformations": "Transformations",
    "processing.legend.workflows": "Workflows",
    "processing.legend.extractors": "Extraction pipelines",
    "processing.stats.executions": "{count} executions · peak parallelism {peak}",
    "processing.stats.peak": "peak {peak}",
    "processing.failed.title": "Failed or timed out duration",
    "processing.failed.description": "Total time spent on executions that failed or timed out.",
    "processing.failed.minutes": "{minutes} minutes",
    "processing.help.title": "Processing overview",
    "processing.help.subtitle": "What the concurrency and bubbles represent.",
    "processing.help.challenge.title": "Which challenges does this help solve?",
    "processing.help.challenge.one":
      "Identify failures in Functions, Transformations, Workflows, and Extraction Pipelines that are easy to miss in the CDF GUI.",
    "processing.help.challenge.two":
      "Reduce contention and throttling (HTTP 429, concurrent limits) by spotting bursty scheduling and smoothing run overlap.",
    "processing.help.band":
      "The line bands show how many executions ran in parallel over the selected time window for each data type.",
    "processing.help.bubbles":
      "The dots below show individual executions (functions, transformations, workflows, and extraction pipelines). Dot size reflects duration where available, and color reflects status.",
    "processing.help.inspect":
      "Click any dot to inspect metadata and logs/details. Use the hour navigation to explore different periods.",
    "processing.help.peaks":
      "Use the graphs to spot unnecessary peaks paired with idle gaps. Those patterns usually mean scheduling can be smoothed out.",
    "processing.help.conflicts":
      "Better scheduling reduces conflicts and exceptions by avoiding heavy overlap during periods of added strain.",
    "processing.legend.panel": "Bubble color legend",
    "processing.card.concurrency.title": "Concurrency diagram",
    "processing.card.concurrency.description":
      "Parallel function executions per {bucketSeconds}-second bucket. Dotted vertical lines and light bands mark five-minute boundaries (UTC).",
    "processing.status.error": "error",
    "processing.loader.title": "Loading processing data",
    "processing.unknown.transformation": "Unknown transformation",
    "processing.legend.functions.title": "Function bubbles",
    "processing.legend.transformations.title": "Transformation bubbles",
    "processing.legend.workflows.title": "Workflow bubbles",
    "processing.legend.extractors.title": "Extraction pipeline bubbles",
    "processing.legend.completed": "Completed",
    "processing.legend.running": "Running",
    "processing.legend.failed": "Failed",
    "processing.legend.timeout": "Timeout",
    "processing.legend.timedout": "Timed out",
    "processing.legend.success": "Success",
    "processing.legend.other": "Other",
    "processing.legend.seen": "Seen",
    "processing.legend.failed.default": "Failed (default)",
    "processing.legend.failed.oom": "Failed: out of memory",
    "processing.legend.failed.concurrent": "Failed: too many concurrent requests",
    "processing.legend.failed.internal": "Failed: internal server error",
    "processing.legend.failed.upstream": "Failed: upstream request timeout",
    "processing.unknown": "unknown",
    "processing.modal.logs.loading": "Loading logs...",
    "processing.modal.logs.error": "Failed to load logs.",
    "processing.modal.workflow.loading": "Loading workflow details...",
    "processing.modal.workflow.error": "Failed to load workflow execution.",
    "processing.modal.extractor.name": "Extraction pipeline",
    "processing.modal.transformations.name": "Transformation",
    "processing.modal.workflows.name": "Workflow",
    "processing.modal.functions.name": "Function",
    "processing.modal.function.title": "Function metadata",
    "processing.modal.function.section.function": "Function",
    "processing.modal.function.section.execution": "Execution",
    "processing.modal.function.section.logs": "Invocation logs",
    "processing.modal.function.logs.empty": "No logs found for this invocation.",
    "processing.modal.function.logs.noMessage": "No message",
    "processing.modal.transformation.title": "Transformation metadata",
    "processing.modal.transformation.viewDetailsLink": "Analyze!",
    "processing.modal.transformation.runHistoryLink": "View run history in Fusion",
    "processing.modal.transformation.section.transformation": "Transformation",
    "processing.modal.transformation.section.job": "Job",
    "processing.modal.workflow.title": "Workflow execution",
    "processing.modal.workflow.section.execution": "Execution summary",
    "processing.modal.workflow.section.details": "Workflow details",
    "processing.modal.extractor.title": "Extraction pipeline execution",
    "processing.modal.extractor.section.pipeline": "Pipeline",
    "processing.modal.extractor.section.run": "Run",
    "permissions.title": "Permissions Troubleshooting",
    "permissions.subtitle": "Capability overview for groups in this project.",
    "permissions.subNavAria": "Permissions sections",
    "permissions.subnav.groups": "Group capabilities",
    "permissions.subnav.compare": "User comparison",
    "permissions.subnav.spaces": "Space access",
    "permissions.subnav.datasets": "Data set access",
    "permissions.subnav.crossProject": "Cross-project checks",
    "permissions.crossProject.title": "Cross-project membership",
    "permissions.crossProject.description":
      "Compare security group memberships across CDF projects, using shared source IDs or names. Expand the access block to upload JSON or view as another user.",
    "permissions.crossProject.viewAs": "Matrix for",
    "permissions.crossProject.viewerCollapsedPrefix": "Viewing",
    "permissions.crossProject.viewerCurrentUser": "Current user",
    "permissions.crossProject.accessCollapsedHint":
      "Expand to upload or paste access JSON, or switch to another user.",
    "permissions.crossProject.accessCollapsedUsers":
      "{n} access file(s) on record — expand to edit or switch.",
    "permissions.crossProject.accessBlockExpand": "Expand",
    "permissions.crossProject.accessBlockCollapse": "Hide",
    "permissions.crossProject.viewerHint":
      "Uploaded users use the projects listed in their JSON. Current user uses every project from your live session token.",
    "permissions.crossProject.loading": "Loading memberships and group definitions…",
    "permissions.crossProject.noProjects": "Your token has no project list from access info.",
    "permissions.crossProject.noMemberships": "You have no security group memberships in these projects.",
    "permissions.crossProject.summaryEmpty": "No group memberships to compare across projects.",
    "permissions.crossProject.summaryMatch":
      "The same logical groups appear in every project (matched by source ID, or name when source ID is missing).",
    "permissions.crossProject.summaryMismatch":
      "Membership differs across projects: some groups are missing in one or more projects (see highlighted cells).",
    "permissions.crossProject.idOnlyNote":
      "Rows tagged “ID” align only within a single project (no shared source ID or name). They are not linked across projects.",
    "permissions.crossProject.metricLabel": "Cell display:",
    "permissions.crossProject.metricStatus": "Member",
    "permissions.crossProject.metricName": "Name",
    "permissions.crossProject.metricSourceId": "Source ID",
    "permissions.crossProject.metricId": "Numeric ID",
    "permissions.crossProject.colGroup": "Group",
    "permissions.crossProject.memberCount": "{n} groups",
    "permissions.crossProject.columnCountTitleMatch":
      "Number of checkmarks in this column ({n}) — same as the number of numeric group IDs for this project in your token.",
    "permissions.crossProject.columnCountTitleMerged":
      "Number of checkmarks in this column ({logical}). The token lists {token} numeric group IDs in this project; {merged} of them share a source ID or name with another membership, so they map to one row each.",
    "permissions.crossProject.idOnlyBadge": "ID",
    "permissions.crossProject.idOnlyBadgeTitle":
      "This row is keyed by numeric ID in one project only; cross-project matching is not applied.",
    "permissions.crossProject.cellGapTitle": "Not a member of this logical group in this project",
    "permissions.crossProject.cellUnknown": "(unnamed)",
    "permissions.crossProject.legendMember": "Member of this group in this project",
    "permissions.crossProject.legendGap": "Missing here but member in another project",
    "permissions.crossProject.legendOther": "Other / neutral cell",
    "permissions.crossProject.capabilitiesTitle": "Resolved capabilities",
    "permissions.crossProject.capabilitiesDescription":
      "Capabilities granted by your security group memberships in each project. A green check means at least one member group includes that capability. An orange dot means actions or scope differ in a way beyond read-vs-write tier. A small R or W next to the check means this project is read-tier or write-tier while scope matches other environments (often intentional).",
    "permissions.crossProject.capabilitiesNone":
      "No capabilities were found on your member groups in these projects.",
    "permissions.crossProject.colCapability": "Capability",
    "permissions.crossProject.capCellPresentTitle": "Granted via member group(s) in this project",
    "permissions.crossProject.capCellDriftTitle":
      "Granted here, but actions or scope differ from another project (or multiple groups disagree)",
    "permissions.crossProject.capCellReadWriteDriftTitle":
      "Same scope; only read vs write tier differs — often intentional across environments",
    "permissions.crossProject.readWriteDriftBadgeTitle":
      "Read vs write tier only (same scope). Click to compare JSON.",
    "permissions.crossProject.legendReadWriteDrift":
      "R or W: read vs write tier only in this project; scope matches elsewhere",
    "permissions.crossProject.capCellGapTitle": "Granted in another project but not via your groups here",
    "permissions.crossProject.scopeDriftDotTitle": "Scope / actions differ across environments or groups",
    "permissions.crossProject.legendCapPresent": "Granted in this project",
    "permissions.crossProject.legendCapGap": "Missing here but granted elsewhere",
    "permissions.crossProject.legendScopeDrift": "Different scoping or actions (orange dot)",
    "permissions.crossProject.groupDefinitionsForbiddenSummary":
      "Could not load group definitions for: {projects} (access forbidden for your user). Membership from the viewed access data is still shown; highlighted columns lack full detail.",
    "permissions.crossProject.columnDefinitionsForbiddenTitle":
      "Group definitions not available — your token cannot list groups in this project",
    "permissions.crossProject.membershipForbiddenCellTitle":
      "Member in this project, but group details are not visible to your user",
    "permissions.crossProject.capCellDefinitionsForbiddenTitle":
      "Capabilities unknown — group list is not accessible for this project",
    "permissions.crossProject.legendDefinitionsForbidden":
      "Column: definitions not loaded (forbidden)",
    "permissions.crossProject.legendCapDefinitionsForbidden":
      "Cell: capabilities unknown for that project",
    "permissions.crossProject.driftModalTitle": "{capability} · {project}",
    "permissions.crossProject.driftModalColThis": "This project: {project}",
    "permissions.crossProject.driftModalColOther": "Compare: {label}",
    "permissions.help.title": "Permissions overview",
    "permissions.help.subtitle": "How to read capabilities, scopes, and group membership.",
    "permissions.help.challenge.title": "Which challenges does this help solve?",
    "permissions.help.challenge.one":
      "Troubleshoot why different users see different content in CDF.",
    "permissions.help.challenge.two":
      "Identify which security groups grant a specific set of permissions.",
    "permissions.help.challenge.three": "Spot drift in security scopes across groups.",
    "permissions.help.matrix":
      "The capability matrix shows each security group’s allowed actions per capability. Colors correspond to the legend below the table.",
    "permissions.help.scopes":
      "The space and data set tables list which groups have explicit scope entries. These are often the source of access mismatches.",
    "permissions.help.compare":
      "The user comparison table lets you compare uploaded users against the current project’s groups.",
    "permissions.groups.title": "Group capabilities",
    "permissions.groups.description": "Capability actions and scopes aggregated by security group.",
    "permissions.groups.none": "No groups found.",
    "permissions.loading": "Loading permissions...",
    "permissions.loadingDetail.groups": "Fetching security groups for this project…",
    "permissions.loadingDetail.datasets": "Fetching data sets…",
    "permissions.loadingDetail.spacesStarting": "Loading space definitions (paginated requests)…",
    "permissions.loadingDetail.spaces": "{count} spaces loaded · {page} request(s) so far",
    "permissions.loadingDetail.analyzing": "Mapping capabilities and scopes… group {current} of {total}",
    "permissions.error": "Failed to load permissions.",
    "permissions.currentUser": "Current user",
    "permissions.currentSuffix": "(current)",
    "permissions.group.fallback": "Group {id}",
    "permissions.upload.label": "Upload access info JSON files",
    "permissions.upload.uploading": "Uploading...",
    "permissions.upload.invalid": "Invalid access info in {fileName}",
    "permissions.upload.empty":
      "No users available yet. Upload JSON files, paste access info, or drop files onto the paste area below.",
    "permissions.paste.label": "Paste access info (JSON)",
    "permissions.paste.placeholder": "Paste JSON here…",
    "permissions.paste.displayName": "Display name (optional)",
    "permissions.paste.add": "Add to comparison",
    "permissions.paste.dropHint": "You can also drop JSON files onto this dashed area.",
    "permissions.paste.invalid": "Invalid JSON or missing subject and projects array.",
    "permissions.spaces.none": "No spaces found.",
    "permissions.datasets.none": "No data sets found.",
    "permissions.dataset.unnamed": "Unnamed data set",
    "permissions.space.unnamed": "Unnamed space",
    "permissions.legend.label": "Legend:",
    "permissions.legend.space": "Group has explicit space scope",
    "permissions.legend.dataset": "Group has explicit data set scope",
    "permissions.compare.membership": "Compare group membership by uploaded users.",
    "permissions.compare.help.title": "How user comparison works",
    "permissions.compare.help.subtitle": "Compare group membership with uploaded access files.",
    "permissions.compare.help.stepOne":
      "Upload multiple access info JSON files to compare group membership across users.",
    "permissions.compare.help.stepTwo":
      "Users download these JSON files from the CDF GUI and share them with you.",
    "permissions.compare.help.stepThree":
      "Group memberships are stored outside CDF, so this tool cannot fetch them directly.",
    "permissions.compare.help.stepFour":
      "In the CDF GUI: click your name in the bottom-left corner, select “Access info”, then copy the JSON into a file named your_name.json.",
    "permissions.scopes.space.title": "Space access",
    "permissions.scopes.space.description": "Space scope entries per security group.",
    "permissions.scopes.dataset.title": "Data set access",
    "permissions.scopes.dataset.description": "Dataset scope entries per security group.",
    "permissions.compare.title": "User comparison",
    "permissions.compare.searchLabel": "Search",
    "permissions.compare.searchPlaceholder": "Search groups by name, ID, or source…",
    "permissions.compare.utilizedOnly": "Only show groups with a member among listed users",
    "permissions.compare.truncatedSummary":
      "Showing {shown} of {total} groups (all {pinned} groups your users belong to are included). {hidden} more hidden.",
    "permissions.compare.showAll": "Show all {total} groups",
    "permissions.compare.collapseList": "Show summary only",
    "permissions.compare.noMatches": "No groups match the search or filters.",
    "permissions.compare.includeOtherProjects":
      "Include memberships from other CDF projects (from access info)",
    "permissions.compare.otherProjectLoading": "Loading group name…",
    "permissions.compare.otherProjectNameError":
      "Could not load group definitions for one or more projects (check access).",
    "permissions.compare.otherProjectGroupsForbiddenSummary":
      "Group definitions are not visible to your user for: {projects}. Rows still show numeric group IDs from the uploaded access info.",
    "permissions.compare.groupDefinitionForbiddenFallback": "Group {id}",
    "permissions.compare.description": "Compare user access to project groups.",
    "permissions.compare.empty": "No uploaded users. Upload JSON files to compare.",
    "permissions.compare.upload": "Choose files",
    "permissions.compare.error": "Failed to load user file.",
    "permissions.compare.clear": "Clear all",
    "permissions.compare.remove": "Remove",
    "permissions.legend.title": "Legend",
    "permissions.legend.read": "Read",
    "permissions.legend.write": "Read and Write",
    "permissions.legend.readplus": "Advanced Read",
    "permissions.legend.writeplus": "Advanced Write",
    "permissions.legend.owner": "Owner/Member",
    "permissions.legend.all": "All",
    "permissions.legend.multi": "Multi-scope",
    "permissions.legend.unknown": "Unknown",
    "permissions.legend.custom": "Custom actions",
    "permissions.scope.all": "All",
    "permissions.scope.datasets": "Datasets",
    "permissions.scope.ids": "IDs",
    "permissions.scope.spaces": "Spaces",
    "permissions.scope.tables": "Tables",
    "permissions.scope.apps": "Apps",
    "permissions.scope.multi": "Multi",
    "permissions.scope.unknown": "Unknown",
    "permissions.table.group": "Group",
    "permissions.table.space": "Space",
    "permissions.table.name": "Name",
    "permissions.table.id": "ID",
    "permissions.table.groups": "Groups",
    "permissions.table.actions": "Actions",
    "permissions.table.scope": "Scope",
    "permissions.table.user": "User",
    "permissions.table.capability": "Capability",
    "permissions.table.dataset": "Dataset",
    "permissions.table.status": "Status",
    "dataCatalog.title": "Data Catalog",
    "dataCatalog.sectionSubtitle":
      "Browse models and fields, explore properties, and compare published versions.",
    "dataCatalog.subNavAria": "Data catalog sections",
    "dataCatalog.overview.title": "Overview",
    "dataCatalog.subnav.overview": "Overview",
    "dataCatalog.subnav.propertyExplorer": "Property Explorer",
    "dataCatalog.propertyExplorer.showAllFilters": "Show all filters",
    "dataCatalog.propertyExplorer.hideExtraFilters": "Hide extra filters",
    "dataCatalog.subnav.dataModelVersions": "Data Model Versions",
    "dataCatalog.subnav.viewVersions": "View Versions",
    "dataCatalog.versionMatrix.showChecksumVersions":
      "Show checksum-style version columns",
    "dataCatalog.versionMatrix.onlyChecksumColumns":
      "No regular version columns in this scope; only checksum-style identifiers remain. Enable below to show the grid.",
    "dataCatalog.versionHistory.backToGrid": "Back to version matrix",
    "dataCatalog.versionHistory.open": "Version history",
    "dataCatalog.versionHistory.openPinned": "Version history",
    "dataCatalog.versionHistory.title": "Data model version history",
    "dataCatalog.versionHistory.versions": "versions",
    "dataCatalog.versionHistory.hint":
      "Expand a row for Fusion links and timestamps for each side, then the change summary. Steps compare consecutive published versions (newest first): views added or removed, reference version bumps, and—when CDF returns full inline view definitions—property and metadata changes between those view versions.",
    "dataCatalog.versionHistory.stepFrom": "From",
    "dataCatalog.versionHistory.stepTo": "To",
    "dataCatalog.versionHistory.stepSingle": "Version",
    "dataCatalog.versionHistory.fieldHeatmapCaption":
      "View field presence by data model version. Each column is one property identifier (no headers)—the same name is merged across views using DMS `implements` precedence (later array entries override earlier). Dark blue = present; light blue = present in this version and the adjacent older/newer row too, but which view wins after `implements` resolution changed between those two; white = absent. Details below—hover a cell or click to pin.",
    "dataCatalog.versionHistory.fieldHeatmapLegendLightBlue":
      "Light blue cells: property still present vs the neighboring version row, but the resolved supplying view (per root) changed—often because `implements` order or parent view definitions changed.",
    "dataCatalog.versionHistory.fieldHeatmapLegendOrange":
      "Orange row border: a transformation uses this data model version as a write destination and it is not the latest published version.",
    "dataCatalog.versionHistory.fieldHeatmapLegendAddedFieldHover":
      "Hovering version \"{version}\": orange cells (in rows from that version up to the one before latest) mark fields absent there but present in a newer revision.",
    "dataCatalog.versionHistory.fieldHeatmapLegendTxSql":
      "This data model is referenced in at least one transformation query.",
    "dataCatalog.versionHistory.fieldHeatmapRowLatest": "Latest",
    "dataCatalog.versionHistory.fieldHeatmapRowCatalog": "In catalog",
    "dataCatalog.versionHistory.fieldHeatmapRowTxRefs": "In transformation SQL",
    "dataCatalog.versionHistory.fieldHeatmapRowWriteDest": "Write destination",
    "dataCatalog.versionHistory.fieldHeatmapRowWriteDestOlder": "Write destination (older)",
    "dataCatalog.versionHistory.fieldHeatmapRowTooltipTx": "Transformations: {names}",
    "dataCatalog.versionHistory.fieldHeatmapEmpty":
      "No view properties found for this heat map. Published versions may only include view references without inline property lists.",
    "dataCatalog.versionHistory.fieldHeatmapTooltip": 'Version "{version}" · field "{field}"',
    "dataCatalog.versionHistory.fieldHeatmapDetailResolution": "Inheritance / resolution",
    "dataCatalog.versionHistory.fieldHeatmapResolutionRuleShort":
      "Per root: later `implements` entries override earlier. Tags show root, any shadowed suppliers (struck through), then the resolved supplier for this version.",
    "dataCatalog.versionHistory.fieldHeatmapResolutionTrivialManyRoots":
      "This heatmap column is only the property name \"{field}\" merged across all member views in the model. In this version it appears on {count} views, each with a trivial chain (that view supplies the name itself—no `implements` shadowing). There is not one shared inheritance path for the whole column.",
    "dataCatalog.versionHistory.fieldHeatmapResolutionOmittedTrivialRoots":
      "{count} other member view(s) also supply this name only from themselves (not shown).",
    "dataCatalog.versionHistory.fieldHeatmapDriftVsOlder": "vs older row ({version})",
    "dataCatalog.versionHistory.fieldHeatmapDriftVsNewer": "vs newer row ({version})",
    "dataCatalog.versionHistory.fieldHeatmapDriftWinnerAbsentInVersion": "Absent / {version}",
    "dataCatalog.versionHistory.fieldHeatmapDriftWinnerMalformedSig": "Empty supplier (see console)",
    "dataCatalog.versionHistory.fieldHeatmapDriftRootAdded":
      "Added in row \"{version}\": member view {root} now supplies this property (resolved to {supplier}).",
    "dataCatalog.versionHistory.fieldHeatmapDriftRootRemoved":
      "Removed in row \"{version}\": member view {root} no longer contributes this property (had been {supplier}).",
    "dataCatalog.versionHistory.fieldHeatmapDriftDebugTitle": "Drift debug (raw)",
    "dataCatalog.versionHistory.fieldHeatmapDriftDebugDismiss": "Dismiss",
    "dataCatalog.versionHistory.fieldHeatmapDetailWinnerDrift": "Supplier vs adjacent version",
    "dataCatalog.versionHistory.fieldHeatmapResolutionRule":
      "DMS resolves the same property identifier from multiple inherited views by walking each view's `implements` chain: later entries in the `implements` array override earlier ones (with topological ordering for the full graph). You get one effective property per name.",
    "dataCatalog.versionHistory.fieldHeatmapResolutionMultiRoot":
      "This property name is also exposed by other views listed in this data model (separate roots):",
    "dataCatalog.versionHistory.fieldHeatmapResolutionModelMember":
      "Model view {root}: utilized definition from {utilized}.",
    "dataCatalog.versionHistory.fieldHeatmapResolutionShadowed":
      "Shadowed definitions (same identifier on superseded views in the chain): {views}.",
    "dataCatalog.versionHistory.fieldHeatmapDetailTitle": "Details",
    "dataCatalog.versionHistory.fieldHeatmapDetailEmpty":
      "Hover a cell to see version, view, field, and presence here. Click a cell to pin; click again or use Clear to release.",
    "dataCatalog.versionHistory.fieldHeatmapDetailHover": "Hover",
    "dataCatalog.versionHistory.fieldHeatmapDetailPinned": "Pinned",
    "dataCatalog.versionHistory.fieldHeatmapDetailClearPin": "Clear pin",
    "dataCatalog.versionHistory.fieldHeatmapDetailVersion": "Version",
    "dataCatalog.versionHistory.fieldHeatmapDetailSpace": "Space",
    "dataCatalog.versionHistory.fieldHeatmapDetailView": "View",
    "dataCatalog.versionHistory.fieldHeatmapDetailField": "Field",
    "dataCatalog.versionHistory.fieldHeatmapDetailPresence": "In this version",
    "dataCatalog.versionHistory.fieldHeatmapDetailYes": "Yes",
    "dataCatalog.versionHistory.fieldHeatmapDetailNo": "No",
    "dataCatalog.versionHistory.created": "Created",
    "dataCatalog.versionHistory.updated": "Updated",
    "dataCatalog.versionHistory.noTransitions": "No consecutive transitions to compare.",
    "dataCatalog.versionHistory.transitionLabel": "{from} → {to}",
    "dataCatalog.versionHistory.hasChanges": "Changes",
    "dataCatalog.versionHistory.noStructural": "No list change",
    "dataCatalog.versionHistory.modelFields": "Data model name / description",
    "dataCatalog.versionHistory.viewsRemoved": "Views removed",
    "dataCatalog.versionHistory.viewsAdded": "Views added",
    "dataCatalog.versionHistory.viewVersionBumps": "View reference version changed",
    "dataCatalog.versionHistory.filterChanged": "View filter definition changed.",
    "dataCatalog.versionHistory.inlineViewMissing":
      "CDF did not return full inline definitions for this view pair; only the referenced version change is shown.",
    "dataCatalog.versionHistory.viewSchemaUnchanged":
      "No differences in inline view metadata, filter, or properties between these versions.",
    "dataCatalog.versionHistory.identicalFingerprint":
      "No differences in model metadata or view membership for this step.",
    "dataCatalog.versionHistory.viewPrevCreated": "Previous view created",
    "dataCatalog.versionHistory.viewNextCreated": "New view created",
    "dataCatalog.versionHistory.viewPrevUpdated": "Previous view updated",
    "dataCatalog.versionHistory.viewNextUpdated": "New view updated",
    "dataCatalog.dataModelVersions.rowLabelsHint":
      "Underlined names open version history when a model has more than one version. ↗ opens the latest version in Cognite Fusion.",
    "dataCatalog.dataModelVersions.searchLabel": "Search rows",
    "dataCatalog.dataModelVersions.searchPlaceholder":
      "Filter by model name, space, external id, or view ref…",
    "dataCatalog.dataModelVersions.noSearchResults":
      "No data models match this search. Try another substring or clear the box.",
    "dataCatalog.viewVersions.searchLabel": "Search rows",
    "dataCatalog.viewVersions.searchPlaceholder":
      "Filter by view name, space, external id, version, property, or implements ref…",
    "dataCatalog.viewVersions.noSearchResults":
      "No views match this search. Try another substring or clear the box.",
    "dataCatalog.dataModelVersions.tooltipVersionHistory": "Click to open version history",
    "dataCatalog.dataModelVersions.tooltipFusion": "Open latest version in Cognite Fusion",
    "dataCatalog.subtitle": "Columns: Data models → Views → Fields.",
    "dataCatalog.filter.placeholder": "Substring…",
    "dataCatalog.filter.exclude": "Exclude",
    "dataCatalog.filter.excludeAria": "Exclude matches for {column}",
    "dataCatalog.filter.summary":
      "{mShown} / {mTotal} models · {vShown} / {vTotal} views · {fShown} / {fTotal} fields",
    "dataCatalog.filter.clear": "Clear filters",
    "dataCatalog.filter.noMatch":
      "No nodes match the current filters. Clear or loosen the search text.",
    "dataCatalog.help.title": "Data catalog overview",
    "dataCatalog.help.subtitle": "How to interpret the model/view/field graph.",
    "dataCatalog.help.challenge.title": "Which challenges does this help solve?",
    "dataCatalog.help.challenge.one":
      "Explain how data models, views, and fields relate in your project.",
    "dataCatalog.help.challenge.two":
      "Identify which views expose a field and where it is reused.",
    "dataCatalog.help.challenge.three":
      "Validate sample rows quickly without switching tools.",
    "dataCatalog.help.graph":
      "The graph shows data models (left), views (middle), and fields (right), with lines indicating how they connect.",
    "dataCatalog.help.hover":
      "Hover a node to highlight its connections. Click a node to open sample rows aligned to the selected view and fields.",
    "dataCatalog.help.order":
      "The ordering attempts to reduce line crossings so related items appear closer.",
    "transformations.title": "Transformations",
    "transformations.subNavLabel": "Transformations sub-views",
    "transformations.subView.list": "List",
    "transformations.subView.overlap": "Transformation overlap",
    "transformations.subView.dataModelUsage": "Data model usage",
    "transformations.help.title": "Transformations overview",
    "transformations.help.listTitle": "List view",
    "transformations.help.columnTableTitle": "Column explanations",
    "transformations.help.columnTableHeader.col": "Column",
    "transformations.help.columnTableHeader.explanation": "Explanation",
    "transformations.help.overlapTitle": "Transformation overlap",
    "transformations.help.dataModelUsageTitle": "Data model usage",
    "transformations.help.subtitle":
      "Analyze SQL transformations, spot overlap, and track data model usage.",
    "transformations.help.challenge.title": "Which challenges does this help solve?",
    "transformations.help.challenge.one":
      "Validate transformation SQL and catch parse errors before deployment.",
    "transformations.help.challenge.two":
      "Identify overlapping transformations that may cause conflicts or redundant work.",
    "transformations.help.challenge.three":
      "See which data models and views each transformation references.",
    "transformations.help.list":
      "The list view shows all transformations with run stats, parse insights, and CTE previews. Click a row to inspect query structure and data model references.",
    "transformations.help.overlap":
      "The overlap view highlights transformations that share tables or data model references and may run concurrently.",
    "transformations.help.dataModelUsage":
      "The data model usage view maps transformations to the data models and views they reference.",
    "transformations.list.loading": "Loading transformations...",
    "transformations.list.error": "Failed to load transformations.",
    "transformations.list.empty": "No transformations found.",
    "transformations.list.searchPlaceholder": "Search (name, id, query)...",
    "transformations.list.backToList": "Back to list",
    "transformations.list.name": "Name",
    "transformations.list.runs24h": "Runs (24h)",
    "transformations.list.lastRun": "Last run",
    "transformations.list.totalTime": "Total time",
    "transformations.list.reads": "Reads",
    "transformations.list.writes": "Writes",
    "transformations.list.noops": "No-ops",
    "transformations.list.rateLimit429": "429s",
    "transformations.list.cte": "CTEs (Common Table Expressions)",
    "transformations.list.columnHelp.name": "Transformation name or ID.",
    "transformations.list.columnHelp.runs24h": "Number of runs in the last 24 hours.",
    "transformations.list.columnHelp.lastRun": "Timestamp of the most recent run.",
    "transformations.list.columnHelp.totalTime": "Total execution time across all runs in the last 24 hours.",
    "transformations.list.columnHelp.reads": "Rows read from raw tables in the latest job.",
    "transformations.list.columnHelp.writes": "Instances upserted (written) in the latest job.",
    "transformations.list.columnHelp.noops": "No-op upserts (unchanged instances) in the latest job.",
    "transformations.list.columnHelp.rateLimit429": "429 rate limit responses in the latest job.",
    "transformations.list.columnHelp.err": "Parse errors: SQL syntax issues detected by the parser.",
    "transformations.list.columnHelp.stmt": "Statements: number of SQL statements in the query.",
    "transformations.list.columnHelp.tok": "Tokens: approximate token count in the query.",
    "transformations.list.columnHelp.tbl": "Tables: number of table references in the query.",
    "transformations.list.columnHelp.cte": "CTEs: number of Common Table Expressions (WITH clauses) in the query.",
    "transformations.list.columnHelp.dm": "Data models: number of cdf_data_models() references.",
    "transformations.list.columnHelp.node": "Node references: number of node_reference() calls.",
    "transformations.list.columnHelp.unit": "Unit lookups: number of try_get_unit() calls.",
    "transformations.list.columnHelp.like": "LIKE: count of LIKE operator usage (pattern matching).",
    "transformations.list.columnHelp.rlike": "RLIKE: count of RLIKE operator usage (regex).",
    "transformations.list.columnHelp.reg": "REGEXP: count of REGEXP operator usage.",
    "transformations.list.columnHelp.nest": "Nested calls: nested function calls that may impact performance.",
    "transformations.list.selected": "Selected:",
    "transformations.list.query": "Query",
    "transformations.list.openInFusion": "Open in Fusion",
    "transformations.list.run": "Run",
    "transformations.list.preview": "Preview",
    "transformations.cte.awaitingPreviews":
      "Awaiting earlier previews to complete before this one is run",
    "transformations.cte.loadingPreview": "Loading preview…",
    "transformations.cte.timelineLoading": "Loading…",
    "transformations.cte.noRowsReturned": "No rows returned.",
    "dataCatalog.loading": "Loading metadata...",
    "dataCatalog.error": "Failed to load metadata.",
    "dataCatalog.empty": "No data models available.",
    "dataCatalog.error.noRelatedView": "No related view found for this selection.",
    "dataCatalog.error.viewUnavailable": "View metadata is not available.",
    "dataCatalog.error.viewMissingVersion": "Selected view is missing a version.",
    "dataCatalog.sample.title": "Sample Rows",
    "dataCatalog.sample.selected": "Selected: {label} · {column}",
    "dataCatalog.column.dataModels": "Data models",
    "dataCatalog.column.views": "Views",
    "dataCatalog.column.fields": "Fields",
    "dataCatalog.sample.loading": "Loading sample rows...",
    "dataCatalog.sample.error": "Failed to load sample rows.",
    "dataCatalog.sample.empty": "No rows available.",
    "healthChecks.title": "Health Checks",
    "healthChecks.subtitle": "Snapshot of key CDF data quality signals.",
    "healthChecks.internal.title": "Internal Health Checks",
    "healthChecks.internal.description":
      "Checks not yet production ready. Feedback welcome.",
    "healthChecks.circuitBreaker.title": "Stopped after repeated API errors",
    "healthChecks.circuitBreaker.description":
      "Several API calls failed in a row (e.g. proxy authentication). Refresh the page to retry after fixing the issue.",
    "healthChecks.loader.title": "Loading health checks",
    "healthChecks.loading": "Loading health checks...",
    "healthChecks.errors.dataModelsAndViews": "Failed to load data models and views.",
    "healthChecks.errors.viewDetails": "Failed to load view details.",
    "healthChecks.errors.containers": "Failed to load containers.",
    "healthChecks.errors.spaces": "Failed to load spaces.",
    "healthChecks.errors.rawMetadata": "Failed to load raw metadata.",
    "healthChecks.errors.functions": "Failed to load functions.",
    "healthChecks.errors.permissions": "Failed to load permissions.",
    "healthChecks.raw.unavailable": "Raw API is not available on this client.",
    "healthChecks.raw.unknownDate": "unknown",
    "healthChecks.modeling.unusedViews.title": "Views not in any data model",
    "healthChecks.modeling.unusedViews.description":
      "Views that are not referenced by any data model view list.",
    "healthChecks.modeling.unusedViews.count": "{count} views are not used in any data model.",
    "healthChecks.modeling.unusedViews.none":
      "All views are referenced by at least one data model.",
    "healthChecks.modeling.viewsWithoutContainers.title": "Views without containers",
    "healthChecks.modeling.viewsWithoutContainers.description":
      "Views that do not map any properties to containers.",
    "healthChecks.modeling.viewsWithoutContainers.count":
      "{count} views do not map to containers.",
    "healthChecks.modeling.viewsWithoutContainers.none":
      "All views map properties to containers.",
    "healthChecks.modeling.unusedContainers.title": "Containers unused by any view",
    "healthChecks.modeling.unusedContainers.description":
      "Containers that are not referenced by any view properties.",
    "healthChecks.modeling.unusedContainers.count":
      "{count} containers are not referenced by any view.",
    "healthChecks.modeling.unusedContainers.none":
      "All containers are referenced by at least one view.",
    "healthChecks.modeling.unusedSpaces.title": "Spaces without models, views, or containers",
    "healthChecks.modeling.unusedSpaces.description":
      "Spaces that have no models, views, or containers in this project.",
    "healthChecks.modeling.unusedSpaces.count":
      "{count} spaces have no models, views, or containers.",
    "healthChecks.modeling.unusedSpaces.none": "All spaces contain models, views, or containers.",
    "healthChecks.modeling.viewsProcessed": "Views processed {processed} / {total}",
    "healthChecks.modeling.spacesLoading": "Loading spaces list",
    "healthChecks.transformations.loading": "Loading transformations…",
    "healthChecks.transformations.progress.listing":
      "Fetching transformations from CDF (up to {limit} per page)…",
    "healthChecks.transformations.progress.remaining": "Fetching remaining transformation pages…",
    "healthChecks.transformations.progress.queries":
      "Resolving SQL for transformations that omit inline query…",
    "healthChecks.transformations.progress.dmv":
      "Scanning queries for data model version references…",
    "healthChecks.transformations.progress.noop":
      "Checking latest job write metrics per transformation…",
    "healthChecks.transformations.progress.noopCount": "{current} / {total} transformations",
    "healthChecks.transformations.sampleLimit":
      "Only the first {count} transformations are loaded; the project list may be longer.",
    "healthChecks.transformations.loadAll": "Load all transformations",
    "healthChecks.transformations.partialDisclaimer":
      "These results are based on a subset of transformations. Load all to include every transformation in the project.",
    "healthChecks.transformations.noops.title": "Transformation writes vs no-ops",
    "healthChecks.transformations.noops.description":
      "Flags transformations where every write in the last job was a no-op, meaning no data was actually changed. This often indicates redundant runs wasting resources.",
    "healthChecks.transformations.noops.error": "Failed to load transformation metrics.",
    "healthChecks.transformations.noops.count":
      "{count} of {total} transformation(s) had all writes equal to no-ops in their last run.",
    "healthChecks.transformations.noops.detail": "{writes} writes, all no-ops",
    "healthChecks.transformations.noops.allGood":
      "All {total} transformations with recent jobs produced effective writes.",
    "healthChecks.dataModelVersioning.title": "Data model version consistency",
    "healthChecks.dataModelVersioning.description":
      "Transformations referencing the same data model should use the same version. Inconsistencies can cause unexpected behavior.",
    "healthChecks.dataModelVersioning.error": "Failed to load transformation data model usage.",
    "healthChecks.dataModelVersioning.inconsistenciesCount":
      "{count} data model(s) have version inconsistencies across transformations.",
    "healthChecks.dataModelVersioning.allConsistent":
      "All data models have consistent versioning across transformations.",
    "healthChecks.versioning.title": "Versioning",
    "healthChecks.versioning.description":
      "Data model and view versions not referenced by transformations, and view versions that would become orphaned if unused model versions were removed.",
    "healthChecks.versioning.error": "Failed to load versioning checks.",
    "healthChecks.versioning.summary.category": "Category",
    "healthChecks.versioning.summary.total": "Total",
    "healthChecks.versioning.summary.used": "Used",
    "healthChecks.versioning.summary.unused": "Unused",
    "healthChecks.versioning.summary.coverage": "Coverage",
    "healthChecks.versioning.summary.coverageValue": "{pct}%",
    "healthChecks.versioning.summary.dataModelVersions": "Data model versions",
    "healthChecks.versioning.summary.viewVersions": "View versions",
    "healthChecks.versioning.summary.totals": "Totals",
    "healthChecks.versioning.summary.orphanedNote":
      "{count} view version(s) would be orphaned if unused model versions were removed ({pct}% of unused views).",
    "healthChecks.versioning.implicitViewRefs.title": "Data models with implicit view references",
    "healthChecks.versioning.implicitViewRefs.description":
      "These data model versions include at least one inline view without an explicit version. Pin a view version (for example v1) on the model to avoid opaque auto-generated view versions.",
    "healthChecks.versioning.implicitViewRefs.none":
      "Every inline view on every data model version specifies an explicit version.",
    "healthChecks.versioning.implicitViewRefs.missingVersionTag": "no explicit version on model",
    "healthChecks.versioning.modelVersionsNotInUse.title": "Data model versions not in use",
    "healthChecks.versioning.modelVersionsNotInUse.description":
      "Model versions that no transformation references (explicitly or as latest).",
    "healthChecks.versioning.modelVersionsNotInUse.none":
      "All data model versions are in use.",
    "healthChecks.versioning.completelyUnusedHint":
      "Orange = not referenced by any transformation or data model.",
    "healthChecks.versioning.viewVersionsNotInUse.title": "View versions not in use",
    "healthChecks.versioning.viewVersionsNotInUse.description":
      "View versions not referenced by any in-use data model version.",
    "healthChecks.versioning.viewVersionsNotInUse.none":
      "All view versions are in use.",
    "healthChecks.versioning.viewVersionsWouldBeOrphaned.title":
      "View versions that would become unused",
    "healthChecks.versioning.viewVersionsWouldBeOrphaned.description":
      "View versions only referenced by data model versions not in use. Removing those model versions would orphan these view versions.",
    "healthChecks.versioning.viewVersionsWouldBeOrphaned.referencedBy": "only in",
    "healthChecks.versioning.viewVersionsWouldBeOrphaned.none":
      "No view versions would become orphaned.",
    "healthChecks.raw.overview.title": "Raw tables overview",
    "healthChecks.raw.overview.description":
      "Database/table scan to identify empty or stale raw tables.",
    "healthChecks.raw.overview.counts": "{databases} databases · {tables} tables scanned",
    "healthChecks.raw.overview.databasesProcessed":
      "Databases processed {processed} / {total}",
    "healthChecks.raw.overview.tablesScanned": "Tables scanned {count}",
    "healthChecks.raw.overview.sampleNote":
      "Samples up to 10 rows when row counts are missing.",
    "healthChecks.raw.overview.sampleLimit":
      "This is a small sample (10 databases, up to 100 tables per database).",
    "healthChecks.raw.overview.loadAll": "Load All",
    "healthChecks.raw.emptyTables.title": "Empty raw tables",
    "healthChecks.raw.emptyTables.description": "Raw tables with zero rows.",
    "healthChecks.raw.emptyTables.count": "{count} raw tables have zero rows.",
    "healthChecks.raw.emptyTables.created": "created {date}",
    "healthChecks.raw.emptyTables.alert": "ALERT",
    "healthChecks.raw.emptyTables.none": "No empty raw tables detected.",
    "healthChecks.raw.emptyTables.sampling": "Sampling rows {processed} / {total}",
    "healthChecks.functions.loading": "Loading functions...",
    "healthChecks.functions.runtime.title": "Function runtime consistency",
    "healthChecks.functions.runtime.description":
      "All functions should use the same Python runtime version.",
    "healthChecks.functions.runtime.count":
      "Found {count} runtime versions across functions.",
    "healthChecks.functions.runtime.functions": "functions",
    "healthChecks.functions.runtime.none": "All functions use {runtime}.",
    "healthChecks.functions.runtime.defaultRuntime": "the same runtime",
    "healthChecks.functions.runtime.unknown": "unknown runtime",
    "healthChecks.functions.lowPython.title": "Functions below Python 3.12",
    "healthChecks.functions.lowPython.description":
      "Warn if any function uses a Python runtime older than 3.12.",
    "healthChecks.functions.lowPython.note":
      "Python 3.11 is approaching end of life. Staying on fully supported versions reduces risk and keeps security updates available.",
    "healthChecks.functions.lowPython.docs.python": "Python version status",
    "healthChecks.functions.lowPython.docs.azure": "Azure Functions Python support",
    "healthChecks.functions.lowPython.count": "{count} functions use Python < 3.12.",
    "healthChecks.functions.lowPython.none": "No functions are using Python < 3.12.",
    "healthChecks.scheduling.title": "Scheduling overlaps",
    "healthChecks.scheduling.description":
      "Look for cron schedules that start at the same time across functions, transformations, and workflows.",
    "healthChecks.scheduling.loading": "Loading schedules...",
    "healthChecks.scheduling.error": "Failed to load schedules.",
    "healthChecks.scheduling.counts.function": "{count} function schedules.",
    "healthChecks.scheduling.counts.transformation": "{count} transformation schedules.",
    "healthChecks.scheduling.counts.workflow": "{count} workflow schedules.",
    "healthChecks.scheduling.overlaps.count":
      "{count} cron patterns start at the same time.",
    "healthChecks.scheduling.overlaps.note":
      "Consider staggering starts to reduce contention and avoid bursts on shared resources.",
    "healthChecks.scheduling.overlaps.cron": "Minute/Hour: {cron}",
    "healthChecks.scheduling.overlaps.offsetTitle": "Example offsets",
    "healthChecks.scheduling.overlaps.none":
      "No overlapping cron schedules detected.",
    "healthChecks.scheduling.offsetExample.title": "Offset example",
    "healthChecks.scheduling.offsetExample.body":
      "Every 5 minutes: change 0/5 * * * * to 2/5 * * * * to start at minute 2, 7, 12, 17, etc.",
    "healthChecks.scheduling.types.function": "Function",
    "healthChecks.scheduling.types.transformation": "Transformation",
    "healthChecks.scheduling.types.workflow": "Workflow",
    "healthChecks.permissions.loading": "Loading permissions…",
    "healthChecks.permissions.progress.listing": "Fetching security groups…",
    "healthChecks.permissions.progress.analyzing":
      "Comparing capability scopes across groups…",
    "healthChecks.permissions.stats.title": "Overview",
    "healthChecks.permissions.stats.groups": "Security groups",
    "healthChecks.permissions.stats.uniqueScopeLists":
      "Unique explicit scope lists (by capability + scope type + entries)",
    "healthChecks.permissions.stats.totalCapabilities": "Capability rows (all groups)",
    "healthChecks.permissions.stats.distinctTypes": "Distinct capability types",
    "healthChecks.permissions.stats.groupsNoCapabilities": "Groups with no capabilities",
    "healthChecks.permissions.stats.allScopeRows": "Capability rows using unrestricted (all) scope",
    "healthChecks.permissions.stats.explicitScopeRows": "Capability rows with dataset / space / table / ID scoping",
    "healthChecks.permissions.stats.driftFindings": "Scope drift findings",
    "healthChecks.permissions.drift.title": "Permission scope drift",
    "healthChecks.permissions.drift.description":
      "Capabilities with nearly identical scopes that differ slightly between groups.",
    "healthChecks.permissions.drift.count":
      "{count} capability scopes look almost the same.",
    "healthChecks.permissions.drift.none": "No near-duplicate capability scopes detected.",
    "healthChecks.permissions.drift.emptyList": "None.",
    "healthChecks.permissions.drift.noActions": "no actions",
    "healthChecks.permissions.drift.explain.show": "Explain",
    "healthChecks.permissions.drift.explain.hide": "Hide",
    "healthChecks.permissions.drift.common": "Common entries",
    "healthChecks.permissions.drift.uniqueLeft": "Unique to {group}",
    "healthChecks.permissions.drift.uniqueRight": "Unique to {group}",
    "healthChecks.permissions.drift.uniqueToPrefix": "Unique to ",
    "healthChecks.permissions.drift.uniqueToSuffix": "",
    "healthChecks.permissions.drift.itemDiff":
      "{capability} ({actions}) · {scopeType} differs by a few items: {left} vs {right}",
    "healthChecks.permissions.drift.unknownCapability": "Unknown",
    "dataCatalog.tooltip.empty": "No connections.",
  },
  ja: {
    "language.english": "English",
    "language.japanese": "日本語",
    "app.language": "言語",
    "shared.project.label": "プロジェクト",
    "shared.help.button": "これは何を意味しますか？",
    "shared.modal.close": "閉じる",
    "shared.loader.title": "読み込み中",
    "shared.loader.description":
      "CDF からデータを取得しています。このメッセージは一時的に閉じて、部分的に読み込まれた内容を確認できます。すべてのデータが利用可能になると、この画面は自動的に閉じます。",
    "shared.loader.dismissOnce": "今回は閉じる",
    "shared.loader.dismissForever": "今後このローダーを表示しない",
    "privateMode.badge": "プライベートモード",
    "privateMode.clickToDisable": "クリックしてプライベートモードを無効にする",
    "nav.processing": "処理",
    "nav.permissions": "権限",
    "nav.dataCatalog": "データカタログ",
    "nav.healthChecks": "ヘルスチェック",
    "nav.transformations": "変換",
    "apiError.showDetails": "詳細を表示",
    "apiError.hideDetails": "詳細を非表示",
    "apiError.section.api": "API",
    "apiError.section.request": "リクエスト本文",
    "apiError.section.details": "詳細",
    "apiError.docsLink": "API ドキュメントを開く",
    "apiError.permissionsHint":
      "必要な権限は各ドキュメントページの冒頭に記載されています。",
    "apiError.networkHint":
      "オフライン、またはバックエンドが応答していない可能性があります。",
    "processing.title": "処理状況",
    "processing.subtitle": "直近 {hoursWindow} 時間の実行並列数を表示します。",
    "processing.loading.functions": "関数を読み込み中...",
    "processing.loading.stats": "実行統計を読み込み中...",
    "processing.loading.runs": "関数実行を読み込み中...",
    "processing.loading.transformations": "変換を読み込み中...",
    "processing.loading.workflows": "ワークフローを読み込み中...",
    "processing.loading.extractors": "抽出パイプラインを読み込み中...",
    "processing.progress.panelTitle": "バックグラウンドのリクエスト",
    "processing.progress.functions.list":
      "関数 · POST /functions/list — これまでに {count} 件の定義を取得",
    "processing.progress.functions.runs":
      "関数 · POST /functions/…/calls/list — {total} 件中 {current} 件を取得済み（残り {remaining} 件）",
    "processing.progress.transformations.list":
      "変換 · GET /transformations — カタログを読み込み中",
    "processing.progress.transformations.jobs":
      "変換 · GET /transformations/…/jobs — {total} 件中 {current} 件を取得済み（残り {remaining} 件）",
    "processing.progress.workflows.executions":
      "ワークフロー · POST /workflows/executions/list — {loaded} 件の実行を取得（ページング継続中）",
    "processing.progress.extractors.list":
      "抽出パイプライン · GET /extpipes — {loaded} 件の構成を取得",
    "processing.progress.extractors.runs":
      "抽出パイプライン · POST /extpipes/runs/list — {total} 件中 {current} 件を取得済み（残り {remaining} 件）",
    "processing.progress.band.functions.list": "fn 一覧 · {count}",
    "processing.progress.band.functions.runs": "fn 呼出 · {current}/{total}",
    "processing.progress.band.transformations.list": "tx 一覧…",
    "processing.progress.band.transformations.jobs": "tx ジョブ · {current}/{total}",
    "processing.progress.band.workflows": "wf 実行 · {loaded}",
    "processing.progress.band.extractors.list": "ext 一覧 · {loaded}",
    "processing.progress.band.extractors.runs": "ext 実行 · {current}/{total}",
    "processing.error.runs": "関数実行の読み込みに失敗しました。",
    "processing.function.defaultName": "関数 {id}",
    "processing.error.transformations": "変換の読み込みに失敗しました。",
    "processing.transformation.defaultName": "変換 {id}",
    "processing.error.workflows": "ワークフローの読み込みに失敗しました。",
    "processing.error.extractors": "抽出パイプラインの読み込みに失敗しました。",
    "processing.bubbles.loading": "読み込み中…",
    "processing.bubbles.empty": "この時間帯のデータはありません。",
    "processing.bubbles.ready": "読み込み完了",
    "processing.heatmap.title": "スケジュール開始のヒートマップ",
    "processing.heatmap.description":
      "関数・変換・ワークフローの cron スケジュールによる 1 日分の開始時刻です。",
    "processing.heatmap.loading": "スケジュールを読み込み中...",
    "processing.heatmap.error": "スケジュールの読み込みに失敗しました。",
    "processing.heatmap.empty": "スケジュール開始は見つかりませんでした。",
    "processing.heatmap.legend.none": "0",
    "processing.heatmap.legend.one": "1",
    "processing.heatmap.legend.mid": "5",
    "processing.heatmap.legend.high": "10+",
    "processing.heatmap.legend.now": "現在 (UTC)",
    "processing.heatmap.unknownFunction": "不明な関数",
    "processing.heatmap.unknownTransformation": "不明な変換",
    "processing.heatmap.hover.title": "{time} · {count}",
    "processing.heatmap.hover.none": "スケジュール開始なし",
    "processing.heatmap.pinned": "固定中",
    "processing.heatmap.unpin": "固定解除",
    "processing.heatmap.copyList": "リストをコピー",
    "processing.heatmap.help.title": "スケジュール開始のヒートマップ",
    "processing.heatmap.help.subtitle": "1 日を通じて均等に配置しましょう。",
    "processing.heatmap.help.detailOne":
      "負荷が集中しないよう、スケジュールはできるだけ均等に散らしてください。",
    "processing.heatmap.help.detailTwo":
      "このヒートマップは 24 時間のスケジュール分布と空き時間を示します。",
    "processing.heatmap.help.detailThree":
      "実行時間は考慮していないため、調整時に別途考慮してください。",
    "processing.extractor.seenEvents": "検知イベント {count} 件",
    "processing.extractor.started": "抽出が開始されました",
    "processing.unavailable.functions": "このクライアントでは Functions API を利用できません。",
    "processing.time.utc": "UTC:",
    "processing.time.local": "{tzLabel}:",
    "processing.action.previous": "前へ",
    "processing.action.next": "次へ",
    "processing.legend.functions": "Functions",
    "processing.legend.transformations": "Transformations",
    "processing.legend.workflows": "Workflows",
    "processing.legend.extractors": "Extraction pipelines",
    "processing.stats.executions": "{count} 件の実行 · 並列最大 {peak}",
    "processing.stats.peak": "最大 {peak}",
    "processing.failed.title": "失敗またはタイムアウトの合計時間",
    "processing.failed.description": "失敗またはタイムアウトした実行に費やした合計時間。",
    "processing.failed.minutes": "{minutes} 分",
    "processing.help.title": "処理の概要",
    "processing.help.subtitle": "並列数とバブルの意味。",
    "processing.help.challenge.title": "このヘルプで解決できる課題は？",
    "processing.help.challenge.one":
      "CDF UI では見逃しやすい Functions / Transformations / Workflows / Extraction Pipelines の断続的・ノイズの多い失敗を可視化します。",
    "processing.help.challenge.two":
      "バースト的なスケジューリングと実行重なりを見つけ、HTTP 429 や同時実行上限などの競合・スロットルを抑えます。",
    "processing.help.band":
      "線のバンドは、選択した期間で各データ種別が並列に動いていた数を示します。",
    "processing.help.bubbles":
      "下のドットは個々の実行（関数、変換、ワークフロー、抽出パイプライン）を表します。サイズは期間、色はステータスです。",
    "processing.help.inspect":
      "ドットをクリックしてメタデータやログを確認できます。時間ナビゲーションで期間を切り替えられます。",
    "processing.help.peaks":
      "不要なピークとアイドルの谷を見つけることで、スケジューリングの平準化ポイントが分かります。",
    "processing.help.conflicts":
      "スケジュールを平準化すると、負荷が高い時間帯の競合や例外を減らせます。",
    "processing.legend.panel": "バブル色の凡例",
    "processing.card.concurrency.title": "並列実行の可視化",
    "processing.card.concurrency.description":
      "{bucketSeconds} 秒ごとの関数実行の並列数。点線の縦線と淡い帯は 5 分境界（UTC）です。",
    "processing.status.error": "エラー",
    "processing.loader.title": "処理データを読み込み中",
    "processing.unknown.transformation": "不明な変換",
    "processing.legend.functions.title": "関数バブル",
    "processing.legend.transformations.title": "変換バブル",
    "processing.legend.workflows.title": "ワークフローバブル",
    "processing.legend.extractors.title": "抽出パイプラインバブル",
    "processing.legend.completed": "完了",
    "processing.legend.running": "実行中",
    "processing.legend.failed": "失敗",
    "processing.legend.timeout": "タイムアウト",
    "processing.legend.timedout": "タイムアウト",
    "processing.legend.success": "成功",
    "processing.legend.other": "その他",
    "processing.legend.seen": "検知",
    "processing.legend.failed.default": "失敗（既定）",
    "processing.legend.failed.oom": "失敗：メモリ不足",
    "processing.legend.failed.concurrent": "失敗：同時実行数が上限",
    "processing.legend.failed.internal": "失敗：内部サーバーエラー",
    "processing.legend.failed.upstream": "失敗：上流リクエストのタイムアウト",
    "processing.unknown": "不明",
    "processing.modal.logs.loading": "ログを読み込み中...",
    "processing.modal.logs.error": "ログの読み込みに失敗しました。",
    "processing.modal.workflow.loading": "ワークフロー詳細を読み込み中...",
    "processing.modal.workflow.error": "ワークフロー実行の読み込みに失敗しました。",
    "processing.modal.extractor.name": "抽出パイプライン",
    "processing.modal.transformations.name": "変換",
    "processing.modal.workflows.name": "ワークフロー",
    "processing.modal.functions.name": "関数",
    "processing.modal.function.title": "関数メタデータ",
    "processing.modal.function.section.function": "関数",
    "processing.modal.function.section.execution": "実行",
    "processing.modal.function.section.logs": "呼び出しログ",
    "processing.modal.function.logs.empty": "この実行のログはありません。",
    "processing.modal.function.logs.noMessage": "メッセージなし",
    "processing.modal.transformation.title": "変換メタデータ",
    "processing.modal.transformation.viewDetailsLink": "Analyze!",
    "processing.modal.transformation.runHistoryLink": "Fusionで実行履歴を表示",
    "processing.modal.transformation.section.transformation": "変換",
    "processing.modal.transformation.section.job": "ジョブ",
    "processing.modal.workflow.title": "ワークフロー実行",
    "processing.modal.workflow.section.execution": "実行サマリー",
    "processing.modal.workflow.section.details": "ワークフロー詳細",
    "processing.modal.extractor.title": "抽出パイプライン実行",
    "processing.modal.extractor.section.pipeline": "パイプライン",
    "processing.modal.extractor.section.run": "実行",
    "permissions.title": "権限トラブルシューティング",
    "permissions.subtitle": "このプロジェクトのグループ権限概要。",
    "permissions.subNavAria": "権限のセクション",
    "permissions.subnav.groups": "グループ権限",
    "permissions.subnav.compare": "ユーザー比較",
    "permissions.subnav.spaces": "スペースアクセス",
    "permissions.subnav.datasets": "データセットアクセス",
    "permissions.subnav.crossProject": "クロスプロジェクト確認",
    "permissions.crossProject.title": "クロスプロジェクトの所属",
    "permissions.crossProject.description":
      "CDF プロジェクト間でセキュリティグループの所属を比較します。共通のソース ID、またはソース ID がない場合は名前で対応付けます。アクセス情報ブロックを開いて JSON を追加するか、別ユーザーとして表示できます。",
    "permissions.crossProject.viewAs": "表示するユーザー",
    "permissions.crossProject.viewerCollapsedPrefix": "表示中",
    "permissions.crossProject.viewerCurrentUser": "現在のユーザー",
    "permissions.crossProject.accessCollapsedHint":
      "開くとアクセス情報の JSON をアップロード／貼り付け、または別ユーザーを選べます。",
    "permissions.crossProject.accessCollapsedUsers":
      "アクセス情報 {n} 件 — 開いて編集または切り替え",
    "permissions.crossProject.accessBlockExpand": "開く",
    "permissions.crossProject.accessBlockCollapse": "閉じる",
    "permissions.crossProject.viewerHint":
      "アップロードしたユーザーは JSON に列挙されたプロジェクトのみを使います。現在のユーザーはセッションのトークンに含まれるすべてのプロジェクトを使います。",
    "permissions.crossProject.loading": "所属とグループ定義を読み込み中…",
    "permissions.crossProject.noProjects": "トークンにプロジェクト一覧がありません。",
    "permissions.crossProject.noMemberships": "これらのプロジェクトにセキュリティグループの所属がありません。",
    "permissions.crossProject.summaryEmpty": "プロジェクト間で比較できるグループ所属がありません。",
    "permissions.crossProject.summaryMatch":
      "すべてのプロジェクトで論理的に同じグループが一致しています（ソース ID、またはソース ID がない場合は名前で対応）。",
    "permissions.crossProject.summaryMismatch":
      "プロジェクト間で所属が異なります。一部のプロジェクトでグループが欠けている行はセルで強調表示されます。",
    "permissions.crossProject.idOnlyNote":
      "「ID」タグの行は単一プロジェクト内の数値 ID のみで対応付けており、プロジェクト間ではリンクしません。",
    "permissions.crossProject.metricLabel": "セルの表示:",
    "permissions.crossProject.metricStatus": "所属",
    "permissions.crossProject.metricName": "名前",
    "permissions.crossProject.metricSourceId": "ソース ID",
    "permissions.crossProject.metricId": "数値 ID",
    "permissions.crossProject.colGroup": "グループ",
    "permissions.crossProject.memberCount": "{n} 件",
    "permissions.crossProject.columnCountTitleMatch":
      "この列のチェック数（{n}）。トークンに含まれるこのプロジェクトの数値グループ ID の件数と一致します。",
    "permissions.crossProject.columnCountTitleMerged":
      "この列のチェック数（{logical}）。トークンではこのプロジェクトに {token} 件の数値グループ ID がありますが、そのうち {merged} 件は別の所属とソース ID または名前が共通するため、行がまとめられています。",
    "permissions.crossProject.idOnlyBadge": "ID",
    "permissions.crossProject.idOnlyBadgeTitle":
      "この行はあるプロジェクト内の数値 ID のみをキーにしており、プロジェクト間の対応付けは行いません。",
    "permissions.crossProject.cellGapTitle": "このプロジェクトではこの論理グループに未所属",
    "permissions.crossProject.cellUnknown": "（名前なし）",
    "permissions.crossProject.legendMember": "このプロジェクトでこのグループに所属",
    "permissions.crossProject.legendGap": "他プロジェクトでは所属だがここでは未所属",
    "permissions.crossProject.legendOther": "その他 / 中立のセル",
    "permissions.crossProject.capabilitiesTitle": "解決済みケイパビリティ",
    "permissions.crossProject.capabilitiesDescription":
      "各プロジェクトで、所属セキュリティグループから付与されるケイパビリティです。緑のチェックは少なくとも 1 つの所属グループにそのケイパビリティがあることを示します。オレンジの点は、読み取り／書き込みの階層以外でもアクションやスコープが異なる場合です。チェック横の R または W は、このプロジェクトが読み取り階層か書き込み階層かを示し、スコープは他環境と一致している場合（環境間では意図的なことが多い）です。",
    "permissions.crossProject.capabilitiesNone":
      "これらのプロジェクトの所属グループにケイパビリティが見つかりませんでした。",
    "permissions.crossProject.colCapability": "ケイパビリティ",
    "permissions.crossProject.capCellPresentTitle": "このプロジェクトの所属グループから付与",
    "permissions.crossProject.capCellDriftTitle":
      "このプロジェクトでは付与されていますが、他プロジェクトまたは複数グループ間でアクション／スコープが一致しません",
    "permissions.crossProject.capCellReadWriteDriftTitle":
      "スコープは同一で、読み取り階層と書き込み階層の違いのみ — 環境間では意図的なことが多いです",
    "permissions.crossProject.readWriteDriftBadgeTitle":
      "読み取り／書き込みの階層のみが異なります（スコープは同一）。クリックで JSON を比較。",
    "permissions.crossProject.legendReadWriteDrift":
      "R または W: このプロジェクトは読み取り／書き込み階層のみが異なる。スコープは他環境と一致",
    "permissions.crossProject.capCellGapTitle": "他プロジェクトでは付与されているが、このプロジェクトの所属では付与されない",
    "permissions.crossProject.scopeDriftDotTitle": "環境間またはグループ間でスコープ／アクションが異なる",
    "permissions.crossProject.legendCapPresent": "このプロジェクトで付与",
    "permissions.crossProject.legendCapGap": "他では付与があるがここではなし",
    "permissions.crossProject.legendScopeDrift": "スコープやアクションの差異（オレンジの点）",
    "permissions.crossProject.groupDefinitionsForbiddenSummary":
      "次のプロジェクトではグループ定義を読み込めませんでした: {projects}（お使いのユーザーではアクセスが拒否されました）。表示中のアクセス情報の所属はそのまま表示しますが、強調した列は詳細がありません。",
    "permissions.crossProject.columnDefinitionsForbiddenTitle":
      "グループ定義を取得できません — このプロジェクトでグループ一覧にアクセスできません",
    "permissions.crossProject.membershipForbiddenCellTitle":
      "このプロジェクトでは所属していますが、お使いのユーザーからはグループ詳細を参照できません",
    "permissions.crossProject.capCellDefinitionsForbiddenTitle":
      "ケイパビリティは不明 — このプロジェクトではグループ一覧にアクセスできません",
    "permissions.crossProject.legendDefinitionsForbidden":
      "列: 定義を読み込めず（禁止）",
    "permissions.crossProject.legendCapDefinitionsForbidden":
      "セル: そのプロジェクトのケイパビリティは不明",
    "permissions.crossProject.driftModalTitle": "{capability} · {project}",
    "permissions.crossProject.driftModalColThis": "このプロジェクト: {project}",
    "permissions.crossProject.driftModalColOther": "比較: {label}",
    "permissions.help.title": "権限の概要",
    "permissions.help.subtitle": "ケイパビリティ、スコープ、グループ所属の読み方。",
    "permissions.help.challenge.title": "このヘルプで解決できる課題は？",
    "permissions.help.challenge.one":
      "CDF でユーザーごとに見える内容が異なる理由の切り分けに役立ちます。",
    "permissions.help.challenge.two":
      "特定の権限セットを付与しているセキュリティグループを特定できます。",
    "permissions.help.challenge.three": "グループ間のセキュリティスコープの乖離を見つけます。",
    "permissions.help.matrix":
      "ケイパビリティ行列は、セキュリティグループごとの許可アクションを示します。色は表の凡例に対応します。",
    "permissions.help.scopes":
      "スペースとデータセットの表は、明示的なスコープ設定を持つグループを示します。権限差異の原因になりやすい箇所です。",
    "permissions.help.compare":
      "ユーザー比較表で、アップロードしたユーザーと現在のプロジェクトグループを比較できます。",
    "permissions.groups.title": "グループ権限",
    "permissions.groups.description": "セキュリティグループ別の権限アクションとスコープ。",
    "permissions.groups.none": "グループが見つかりません。",
    "permissions.loading": "権限を読み込み中...",
    "permissions.loadingDetail.groups": "このプロジェクトのセキュリティグループを取得中…",
    "permissions.loadingDetail.datasets": "データセットを取得中…",
    "permissions.loadingDetail.spacesStarting": "スペース定義を読み込み中（ページ分割）…",
    "permissions.loadingDetail.spaces": "{count} 件のスペースを取得 · ここまで {page} 回のリクエスト",
    "permissions.loadingDetail.analyzing": "ケイパビリティとスコープを集計中… {total} 件中 {current} 件目のグループ",
    "permissions.error": "権限の読み込みに失敗しました。",
    "permissions.currentUser": "現在のユーザー",
    "permissions.currentSuffix": "（現在）",
    "permissions.group.fallback": "グループ {id}",
    "permissions.upload.label": "アクセス情報の JSON ファイルをアップロード",
    "permissions.upload.uploading": "アップロード中...",
    "permissions.upload.invalid": "{fileName} のアクセス情報が不正です",
    "permissions.upload.empty":
      "ユーザーがありません。JSON ファイルをアップロードするか、アクセス情報を貼り付けるか、下の点線枠にファイルをドロップしてください。",
    "permissions.paste.label": "アクセス情報を貼り付け（JSON）",
    "permissions.paste.placeholder": "ここに JSON を貼り付け…",
    "permissions.paste.displayName": "表示名（任意）",
    "permissions.paste.add": "比較に追加",
    "permissions.paste.dropHint": "点線の枠内に JSON ファイルをドロップすることもできます。",
    "permissions.paste.invalid": "JSON が不正か、subject と projects 配列がありません。",
    "permissions.spaces.none": "スペースが見つかりません。",
    "permissions.datasets.none": "データセットが見つかりません。",
    "permissions.dataset.unnamed": "無名のデータセット",
    "permissions.space.unnamed": "無名のスペース",
    "permissions.legend.label": "凡例:",
    "permissions.legend.space": "グループに明示的なスペーススコープがある",
    "permissions.legend.dataset": "グループに明示的なデータセットスコープがある",
    "permissions.compare.membership": "アップロードしたユーザーのグループ所属を比較します。",
    "permissions.compare.help.title": "ユーザー比較の仕組み",
    "permissions.compare.help.subtitle":
      "アクセス情報の JSON を使ってグループ所属を比較します。",
    "permissions.compare.help.stepOne":
      "複数のアクセス情報 JSON をアップロードしてユーザー間のグループ所属を比較します。",
    "permissions.compare.help.stepTwo":
      "各ユーザーは CDF GUI から JSON をダウンロードし、共有する必要があります。",
    "permissions.compare.help.stepThree":
      "グループ所属は CDF の外部に保管されているため、このツールから直接取得できません。",
    "permissions.compare.help.stepFour":
      "CDF GUI では、左下の自分の名前をクリックし、「Access info」を選択して JSON をコピーし、your_name.json として保存します。",
    "permissions.scopes.space.title": "スペースアクセス",
    "permissions.scopes.space.description": "セキュリティグループごとのスペーススコープ。",
    "permissions.scopes.dataset.title": "データセットアクセス",
    "permissions.scopes.dataset.description": "セキュリティグループごとのデータセットスコープ。",
    "permissions.compare.title": "ユーザー比較",
    "permissions.compare.searchLabel": "検索",
    "permissions.compare.searchPlaceholder": "グループ名、ID、ソースで検索…",
    "permissions.compare.utilizedOnly": "一覧ユーザーの誰かが所属しているグループのみ表示",
    "permissions.compare.truncatedSummary":
      "{total} 件中 {shown} 件を表示（ユーザーが所属する {pinned} 件のグループはすべて含みます）。あと {hidden} 件は非表示です。",
    "permissions.compare.showAll": "すべて表示（{total} 件）",
    "permissions.compare.collapseList": "要約表示に戻す",
    "permissions.compare.noMatches": "検索・フィルターに一致するグループがありません。",
    "permissions.compare.includeOtherProjects":
      "他の CDF プロジェクトの所属も表示（アクセス情報から）",
    "permissions.compare.otherProjectLoading": "グループ名を読み込み中…",
    "permissions.compare.otherProjectNameError":
      "一部のプロジェクトでグループ定義を取得できませんでした（アクセスを確認してください）。",
    "permissions.compare.otherProjectGroupsForbiddenSummary":
      "次のプロジェクトでは、お使いのユーザーからグループ定義を参照できません: {projects}。アップロードしたアクセス情報のグループ ID は表示されます。",
    "permissions.compare.groupDefinitionForbiddenFallback": "グループ {id}",
    "permissions.compare.description": "ユーザーのアクセス権をプロジェクトグループと比較します。",
    "permissions.compare.empty": "アップロード済みのユーザーがありません。JSON をアップロードしてください。",
    "permissions.compare.upload": "ファイルを選択",
    "permissions.compare.error": "ユーザーファイルの読み込みに失敗しました。",
    "permissions.compare.clear": "すべて削除",
    "permissions.compare.remove": "削除",
    "permissions.legend.title": "凡例",
    "permissions.legend.read": "読み取り",
    "permissions.legend.write": "読み取り＋書き込み",
    "permissions.legend.readplus": "高度な読み取り",
    "permissions.legend.writeplus": "高度な書き込み",
    "permissions.legend.owner": "所有者/メンバー",
    "permissions.legend.all": "全体",
    "permissions.legend.multi": "複数スコープ",
    "permissions.legend.unknown": "不明",
    "permissions.legend.custom": "カスタムアクション",
    "permissions.scope.all": "全体",
    "permissions.scope.datasets": "データセット",
    "permissions.scope.ids": "ID",
    "permissions.scope.spaces": "スペース",
    "permissions.scope.tables": "テーブル",
    "permissions.scope.apps": "アプリ",
    "permissions.scope.multi": "複数",
    "permissions.scope.unknown": "不明",
    "permissions.table.group": "グループ",
    "permissions.table.space": "スペース",
    "permissions.table.name": "名前",
    "permissions.table.id": "ID",
    "permissions.table.groups": "グループ",
    "permissions.table.actions": "アクション",
    "permissions.table.scope": "スコープ",
    "permissions.table.user": "ユーザー",
    "permissions.table.capability": "ケイパビリティ",
    "permissions.table.dataset": "データセット",
    "permissions.table.status": "ステータス",
    "dataCatalog.title": "データカタログ",
    "dataCatalog.sectionSubtitle":
      "モデルとフィールドの確認、プロパティの探索、公開バージョンの比較。",
    "dataCatalog.subNavAria": "データカタログのセクション",
    "dataCatalog.overview.title": "概要",
    "dataCatalog.subnav.overview": "概要",
    "dataCatalog.subnav.propertyExplorer": "プロパティエクスプローラー",
    "dataCatalog.propertyExplorer.showAllFilters": "すべてのフィルターを表示",
    "dataCatalog.propertyExplorer.hideExtraFilters": "追加フィルターを隠す",
    "dataCatalog.subnav.dataModelVersions": "データモデルバージョン",
    "dataCatalog.subnav.viewVersions": "ビューバージョン",
    "dataCatalog.versionMatrix.showChecksumVersions":
      "チェックサム形式のバージョン列を表示",
    "dataCatalog.versionMatrix.onlyChecksumColumns":
      "この範囲では通常のバージョン列がなく、チェックサム形式の識別子のみです。下で有効にするとグリッドに表示されます。",
    "dataCatalog.versionHistory.backToGrid": "バージョン一覧に戻る",
    "dataCatalog.versionHistory.open": "バージョン履歴",
    "dataCatalog.versionHistory.openPinned": "バージョン履歴",
    "dataCatalog.versionHistory.title": "データモデルのバージョン履歴",
    "dataCatalog.versionHistory.versions": "バージョン",
    "dataCatalog.versionHistory.hint":
      "行を開くと、それぞれの側の Fusion リンクと作成・更新日時、その後に変更内容が表示されます。ステップは連続する公開バージョン同士を比較します（新しい順）。追加・削除されたビュー、参照バージョンの更新、およびCDFが完全なインラインビュー定義を返した場合はそのビュー間のプロパティとメタデータの差分を表示します。",
    "dataCatalog.versionHistory.stepFrom": "変更前",
    "dataCatalog.versionHistory.stepTo": "変更後",
    "dataCatalog.versionHistory.stepSingle": "バージョン",
    "dataCatalog.versionHistory.fieldHeatmapCaption":
      "データモデル各バージョンにおけるビューフィールドの有無。列はプロパティ識別子ごと（見出しなし）。同一の名前は DMS の `implements` の優先（配列の後ろのエントリが前に勝つ）でビュー間でまとめます。濃い青＝存在、薄い青＝隣の行（より新しい／古い公開バージョン）でも存在するが、その2版の間で `implements` 解決後の供給元ビューが変わった、白＝なし。下のパネルに詳細—セルにホバーまたはクリックで固定。",
    "dataCatalog.versionHistory.fieldHeatmapLegendLightBlue":
      "薄い青のセル：隣のバージョン行ともプロパティはあるが、ルートごとの解決後の供給元ビューが変わっている（`implements` の順や親ビュー定義の変更など）。",
    "dataCatalog.versionHistory.fieldHeatmapLegendOrange":
      "オレンジの行枠：変換の書き込み先がこのデータモデルバージョンで、かつ最新の公開バージョンではありません。",
    "dataCatalog.versionHistory.fieldHeatmapLegendAddedFieldHover":
      "バージョン \"{version}\" にホバー中：オレンジのセルは、その行から「最新の一つ手前」の行までで、当該リビジョンでは無いがより新しいリビジョンで現れるフィールドです。",
    "dataCatalog.versionHistory.fieldHeatmapLegendTxSql":
      "このデータモデルは少なくとも1件の変換クエリで参照されています。",
    "dataCatalog.versionHistory.fieldHeatmapRowLatest": "最新",
    "dataCatalog.versionHistory.fieldHeatmapRowCatalog": "カタログ内",
    "dataCatalog.versionHistory.fieldHeatmapRowTxRefs": "変換SQLで参照",
    "dataCatalog.versionHistory.fieldHeatmapRowWriteDest": "書き込み先",
    "dataCatalog.versionHistory.fieldHeatmapRowWriteDestOlder": "書き込み先（旧バージョン）",
    "dataCatalog.versionHistory.fieldHeatmapRowTooltipTx": "変換: {names}",
    "dataCatalog.versionHistory.fieldHeatmapEmpty":
      "ヒートマップ用のビュープロパティが見つかりません。公開バージョンにインラインのプロパティ一覧が含まれていない可能性があります。",
    "dataCatalog.versionHistory.fieldHeatmapTooltip": 'バージョン "{version}" · フィールド "{field}"',
    "dataCatalog.versionHistory.fieldHeatmapDetailResolution": "継承 / 解決",
    "dataCatalog.versionHistory.fieldHeatmapResolutionRuleShort":
      "ルートごと: `implements` では配列の後ろのエントリが前に優先。タグはルート →（打ち消しはシャドウ）→ このバージョンで採用される供給元ビュー。",
    "dataCatalog.versionHistory.fieldHeatmapResolutionTrivialManyRoots":
      "ヒートマップの列はプロパティ名「{field}」だけをモデル内の全メンバービューにまたがってまとめています。このバージョンでは {count} ビューに存在し、いずれも自ビューがその名前を供給するだけの自明なチェーンです（`implements` によるシャドウはありません）。列全体で共有される単一の継承パスはありません。",
    "dataCatalog.versionHistory.fieldHeatmapResolutionOmittedTrivialRoots":
      "他に {count} 件のメンバービューも、この名前を自ビューからのみ供給しています（省略）。",
    "dataCatalog.versionHistory.fieldHeatmapDriftVsOlder": "より古い行（{version}）との差",
    "dataCatalog.versionHistory.fieldHeatmapDriftVsNewer": "より新しい行（{version}）との差",
    "dataCatalog.versionHistory.fieldHeatmapDriftWinnerAbsentInVersion": "なし / {version}",
    "dataCatalog.versionHistory.fieldHeatmapDriftWinnerMalformedSig": "空の供給元（コンソール参照）",
    "dataCatalog.versionHistory.fieldHeatmapDriftRootAdded":
      "行「{version}」で追加: メンバービュー {root} がこのプロパティを供給（解決先 {supplier}）。",
    "dataCatalog.versionHistory.fieldHeatmapDriftRootRemoved":
      "行「{version}」で削除: メンバービュー {root} がこのプロパティに含まれなくなりました（以前は {supplier}）。",
    "dataCatalog.versionHistory.fieldHeatmapDriftDebugTitle": "ドリフトデバッグ（生データ）",
    "dataCatalog.versionHistory.fieldHeatmapDriftDebugDismiss": "閉じる",
    "dataCatalog.versionHistory.fieldHeatmapDetailWinnerDrift": "隣接バージョンとの供給元の差",
    "dataCatalog.versionHistory.fieldHeatmapResolutionRule":
      "DMS では、複数の継承ビューに同じプロパティ識別子がある場合、各ビューの `implements` を辿って解決します。`implements` 配列では後のエントリが先に優先され（グラフ全体はトポロジカル順）、名前ごとに一つの有効なプロパティになります。",
    "dataCatalog.versionHistory.fieldHeatmapResolutionMultiRoot":
      "このプロパティ名は、このデータモデルに列挙されている他のビュー（別ルート）からも公開されています:",
    "dataCatalog.versionHistory.fieldHeatmapResolutionModelMember":
      "モデルビュー {root}: 採用される定義は {utilized}。",
    "dataCatalog.versionHistory.fieldHeatmapResolutionShadowed":
      "シャドウされた定義（チェーン上で優先されなかったビュー上の同一識別子）: {views}。",
    "dataCatalog.versionHistory.fieldHeatmapDetailTitle": "詳細",
    "dataCatalog.versionHistory.fieldHeatmapDetailEmpty":
      "セルにホバーするとバージョン・ビュー・フィールド・有無がここに表示されます。クリックで固定、もう一度クリックかクリアで解除します。",
    "dataCatalog.versionHistory.fieldHeatmapDetailHover": "ホバー中",
    "dataCatalog.versionHistory.fieldHeatmapDetailPinned": "固定",
    "dataCatalog.versionHistory.fieldHeatmapDetailClearPin": "固定を解除",
    "dataCatalog.versionHistory.fieldHeatmapDetailVersion": "バージョン",
    "dataCatalog.versionHistory.fieldHeatmapDetailSpace": "スペース",
    "dataCatalog.versionHistory.fieldHeatmapDetailView": "ビュー",
    "dataCatalog.versionHistory.fieldHeatmapDetailField": "フィールド",
    "dataCatalog.versionHistory.fieldHeatmapDetailPresence": "このバージョンに存在",
    "dataCatalog.versionHistory.fieldHeatmapDetailYes": "あり",
    "dataCatalog.versionHistory.fieldHeatmapDetailNo": "なし",
    "dataCatalog.versionHistory.created": "作成",
    "dataCatalog.versionHistory.updated": "更新",
    "dataCatalog.versionHistory.noTransitions": "比較できる連続バージョンがありません。",
    "dataCatalog.versionHistory.transitionLabel": "{from} → {to}",
    "dataCatalog.versionHistory.hasChanges": "変更あり",
    "dataCatalog.versionHistory.noStructural": "リスト変更なし",
    "dataCatalog.versionHistory.modelFields": "データモデル名 / 説明",
    "dataCatalog.versionHistory.viewsRemoved": "削除されたビュー",
    "dataCatalog.versionHistory.viewsAdded": "追加されたビュー",
    "dataCatalog.versionHistory.viewVersionBumps": "ビュー参照バージョンの変更",
    "dataCatalog.versionHistory.filterChanged": "ビューのフィルター定義が変更されました。",
    "dataCatalog.versionHistory.inlineViewMissing":
      "CDFはこのビューペアの完全なインライン定義を返していません。参照バージョンの変更のみ表示します。",
    "dataCatalog.versionHistory.viewSchemaUnchanged":
      "これらのバージョン間でインラインビューのメタデータ、フィルター、プロパティに差分はありません。",
    "dataCatalog.versionHistory.identicalFingerprint":
      "このステップではモデルメタデータまたはビュー構成に差分はありません。",
    "dataCatalog.versionHistory.viewPrevCreated": "以前のビュー作成",
    "dataCatalog.versionHistory.viewNextCreated": "新しいビュー作成",
    "dataCatalog.versionHistory.viewPrevUpdated": "以前のビュー更新",
    "dataCatalog.versionHistory.viewNextUpdated": "新しいビュー更新",
    "dataCatalog.dataModelVersions.rowLabelsHint":
      "下線のある名前は複数バージョンがあるモデルでバージョン履歴を開きます。↗ は Cognite Fusion で最新バージョンを開きます。",
    "dataCatalog.dataModelVersions.searchLabel": "行を検索",
    "dataCatalog.dataModelVersions.searchPlaceholder":
      "モデル名・スペース・外部ID・ビュー参照で絞り込み…",
    "dataCatalog.dataModelVersions.noSearchResults":
      "検索に一致するデータモデルはありません。別の文字列を試すか検索をクリアしてください。",
    "dataCatalog.viewVersions.searchLabel": "行を検索",
    "dataCatalog.viewVersions.searchPlaceholder":
      "ビュー名・スペース・外部ID・バージョン・プロパティ・implements参照で絞り込み…",
    "dataCatalog.viewVersions.noSearchResults":
      "検索に一致するビューはありません。別の文字列を試すか検索をクリアしてください。",
    "dataCatalog.dataModelVersions.tooltipVersionHistory": "クリックでバージョン履歴を開く",
    "dataCatalog.dataModelVersions.tooltipFusion": "Cognite Fusion で最新バージョンを開く",
    "dataCatalog.subtitle": "列: データモデル → ビュー → フィールド。",
    "dataCatalog.filter.placeholder": "部分文字列…",
    "dataCatalog.filter.exclude": "除外",
    "dataCatalog.filter.excludeAria": "{column}で一致した項目を除外",
    "dataCatalog.filter.summary":
      "データモデル {mShown} / {mTotal} · ビュー {vShown} / {vTotal} · フィールド {fShown} / {fTotal}",
    "dataCatalog.filter.clear": "フィルターをクリア",
    "dataCatalog.filter.noMatch":
      "条件に一致するノードがありません。フィルターをクリアするか検索を緩めてください。",
    "dataCatalog.help.title": "データカタログの概要",
    "dataCatalog.help.subtitle": "モデル/ビュー/フィールドグラフの読み方。",
    "dataCatalog.help.challenge.title": "このヘルプで解決できる課題は？",
    "dataCatalog.help.challenge.one":
      "プロジェクト内のデータモデル、ビュー、フィールドの関係を把握できます。",
    "dataCatalog.help.challenge.two":
      "どのビューが特定のフィールドを公開し、再利用されているかを特定できます。",
    "dataCatalog.help.challenge.three":
      "ツールを切り替えずにサンプル行を素早く確認できます。",
    "dataCatalog.help.graph":
      "グラフはデータモデル（左）、ビュー（中央）、フィールド（右）を示し、線で関係を表します。",
    "dataCatalog.help.hover":
      "ノードにホバーすると接続が強調されます。クリックで該当ビューとフィールドのサンプル行を表示します。",
    "dataCatalog.help.order":
      "線の交差が少なくなるように順序を調整し、関連要素が近くに配置されます。",
    "transformations.title": "変換",
    "transformations.subNavLabel": "変換サブビュー",
    "transformations.subView.list": "一覧",
    "transformations.subView.overlap": "変換の重複",
    "transformations.subView.dataModelUsage": "データモデル使用状況",
    "transformations.help.title": "変換の概要",
    "transformations.help.listTitle": "一覧ビュー",
    "transformations.help.columnTableTitle": "列の説明",
    "transformations.help.columnTableHeader.col": "列",
    "transformations.help.columnTableHeader.explanation": "説明",
    "transformations.help.overlapTitle": "変換の重複",
    "transformations.help.dataModelUsageTitle": "データモデル使用状況",
    "transformations.help.subtitle":
      "SQL変換を分析し、重複を特定し、データモデルの使用状況を追跡します。",
    "transformations.help.challenge.title": "このヘルプで解決できる課題は？",
    "transformations.help.challenge.one":
      "デプロイ前に変換SQLを検証し、パースエラーを検出します。",
    "transformations.help.challenge.two":
      "競合や重複作業を引き起こす可能性のある重複変換を特定します。",
    "transformations.help.challenge.three":
      "各変換が参照するデータモデルとビューを確認します。",
    "transformations.help.list":
      "一覧ビューでは、実行統計、パースインサイト、CTEプレビュー付きの全変換を表示します。行をクリックしてクエリ構造とデータモデル参照を確認できます。",
    "transformations.help.overlap":
      "重複ビューでは、テーブルやデータモデル参照を共有し、同時実行される可能性のある変換を強調表示します。",
    "transformations.help.dataModelUsage":
      "データモデル使用状況ビューでは、変換と参照するデータモデル・ビューの対応を示します。",
    "transformations.list.loading": "変換を読み込み中...",
    "transformations.list.error": "変換の読み込みに失敗しました。",
    "transformations.list.empty": "変換が見つかりません。",
    "transformations.list.searchPlaceholder": "検索 (名前、ID、クエリ)...",
    "transformations.list.backToList": "一覧に戻る",
    "transformations.list.name": "名前",
    "transformations.list.runs24h": "実行 (24h)",
    "transformations.list.lastRun": "最終実行",
    "transformations.list.totalTime": "合計時間",
    "transformations.list.reads": "読み取り",
    "transformations.list.writes": "書き込み",
    "transformations.list.noops": "No-op",
    "transformations.list.rateLimit429": "429",
    "transformations.list.cte": "CTE（共通テーブル式）",
    "transformations.list.columnHelp.name": "変換の名前またはID。",
    "transformations.list.columnHelp.runs24h": "過去24時間の実行回数。",
    "transformations.list.columnHelp.lastRun": "直近の実行日時。",
    "transformations.list.columnHelp.totalTime": "過去24時間の全実行の合計時間。",
    "transformations.list.columnHelp.reads": "最新ジョブで読み取った生テーブルの行数。",
    "transformations.list.columnHelp.writes": "最新ジョブでアップサート（書き込み）したインスタンス数。",
    "transformations.list.columnHelp.noops": "最新ジョブのNo-opアップサート（変更なしインスタンス）数。",
    "transformations.list.columnHelp.rateLimit429": "最新ジョブの429レート制限応答数。",
    "transformations.list.columnHelp.err": "パースエラー：パーサーが検出したSQL構文の問題。",
    "transformations.list.columnHelp.stmt": "ステートメント：クエリ内のSQL文の数。",
    "transformations.list.columnHelp.tok": "トークン：クエリの概算トークン数。",
    "transformations.list.columnHelp.tbl": "テーブル：クエリ内のテーブル参照の数。",
    "transformations.list.columnHelp.cte": "CTE：クエリ内の共通テーブル式（WITH句）の数。",
    "transformations.list.columnHelp.dm": "データモデル：cdf_data_models()参照の数。",
    "transformations.list.columnHelp.node": "ノード参照：node_reference()呼び出しの数。",
    "transformations.list.columnHelp.unit": "単位参照：try_get_unit()呼び出しの数。",
    "transformations.list.columnHelp.like": "LIKE：LIKE演算子の使用回数（パターンマッチング）。",
    "transformations.list.columnHelp.rlike": "RLIKE：RLIKE演算子の使用回数（正規表現）。",
    "transformations.list.columnHelp.reg": "REGEXP：REGEXP演算子の使用回数。",
    "transformations.list.columnHelp.nest": "ネスト呼び出し：パフォーマンスに影響する可能性のあるネストされた関数呼び出し。",
    "transformations.list.selected": "選択:",
    "transformations.list.query": "クエリ",
    "transformations.list.openInFusion": "Fusionで開く",
    "transformations.list.run": "実行",
    "transformations.list.preview": "プレビュー",
    "transformations.cte.awaitingPreviews":
      "このプレビューを実行する前に、先のプレビューの完了を待っています",
    "transformations.cte.loadingPreview": "プレビューを読み込み中…",
    "transformations.cte.timelineLoading": "読み込み中…",
    "transformations.cte.noRowsReturned": "行が返されませんでした。",
    "dataCatalog.loading": "メタデータを読み込み中...",
    "dataCatalog.error": "メタデータの読み込みに失敗しました。",
    "dataCatalog.empty": "データモデルがありません。",
    "dataCatalog.error.noRelatedView": "選択に関連するビューが見つかりません。",
    "dataCatalog.error.viewUnavailable": "ビューのメタデータが利用できません。",
    "dataCatalog.error.viewMissingVersion": "選択したビューのバージョンがありません。",
    "dataCatalog.sample.title": "サンプル行",
    "dataCatalog.sample.selected": "選択: {label} · {column}",
    "dataCatalog.column.dataModels": "データモデル",
    "dataCatalog.column.views": "ビュー",
    "dataCatalog.column.fields": "フィールド",
    "dataCatalog.sample.loading": "サンプル行を読み込み中...",
    "dataCatalog.sample.error": "サンプル行の読み込みに失敗しました。",
    "dataCatalog.sample.empty": "行がありません。",
    "healthChecks.title": "ヘルスチェック",
    "healthChecks.subtitle": "CDF の主要なデータ品質シグナルをまとめて表示します。",
    "healthChecks.internal.title": "内部ヘルスチェック",
    "healthChecks.internal.description":
      "本番対応前のチェックです。フィードバック歓迎。",
    "healthChecks.circuitBreaker.title": "API エラーが続いたため停止しました",
    "healthChecks.circuitBreaker.description":
      "複数の API 呼び出しが連続して失敗しました（例：プロキシ認証）。問題を修正してからページを更新して再試行してください。",
    "healthChecks.loader.title": "ヘルスチェックを読み込み中",
    "healthChecks.loading": "ヘルスチェックを読み込み中...",
    "healthChecks.errors.dataModelsAndViews": "データモデルとビューの読み込みに失敗しました。",
    "healthChecks.errors.viewDetails": "ビュー詳細の読み込みに失敗しました。",
    "healthChecks.errors.containers": "コンテナの読み込みに失敗しました。",
    "healthChecks.errors.spaces": "スペースの読み込みに失敗しました。",
    "healthChecks.errors.rawMetadata": "Raw メタデータの読み込みに失敗しました。",
    "healthChecks.errors.functions": "関数の読み込みに失敗しました。",
    "healthChecks.errors.permissions": "権限の読み込みに失敗しました。",
    "healthChecks.raw.unavailable": "このクライアントでは Raw API を利用できません。",
    "healthChecks.raw.unknownDate": "不明",
    "healthChecks.modeling.unusedViews.title": "データモデル未参照のビュー",
    "healthChecks.modeling.unusedViews.description":
      "どのデータモデルのビュー一覧にも含まれないビュー。",
    "healthChecks.modeling.unusedViews.count":
      "{count} 件のビューがデータモデルに含まれていません。",
    "healthChecks.modeling.unusedViews.none":
      "すべてのビューは少なくとも 1 つのデータモデルに含まれています。",
    "healthChecks.modeling.viewsWithoutContainers.title": "コンテナ未接続のビュー",
    "healthChecks.modeling.viewsWithoutContainers.description":
      "コンテナにプロパティがマッピングされていないビュー。",
    "healthChecks.modeling.viewsWithoutContainers.count":
      "{count} 件のビューがコンテナにマッピングされていません。",
    "healthChecks.modeling.viewsWithoutContainers.none":
      "すべてのビューはコンテナにマッピングされています。",
    "healthChecks.modeling.unusedContainers.title": "未参照のコンテナ",
    "healthChecks.modeling.unusedContainers.description":
      "ビューのプロパティから参照されていないコンテナ。",
    "healthChecks.modeling.unusedContainers.count":
      "{count} 件のコンテナがどのビューからも参照されていません。",
    "healthChecks.modeling.unusedContainers.none":
      "すべてのコンテナは少なくとも 1 つのビューから参照されています。",
    "healthChecks.modeling.unusedSpaces.title": "モデル・ビュー・コンテナがないスペース",
    "healthChecks.modeling.unusedSpaces.description":
      "このプロジェクトでモデル、ビュー、コンテナが存在しないスペース。",
    "healthChecks.modeling.unusedSpaces.count":
      "{count} 件のスペースにモデル・ビュー・コンテナがありません。",
    "healthChecks.modeling.unusedSpaces.none":
      "すべてのスペースにモデル・ビュー・コンテナがあります。",
    "healthChecks.modeling.viewsProcessed": "ビュー処理 {processed} / {total}",
    "healthChecks.modeling.spacesLoading": "スペース一覧を読み込み中",
    "healthChecks.transformations.loading": "変換を読み込み中…",
    "healthChecks.transformations.progress.listing":
      "CDFから変換を取得中（1ページあたり最大{limit}件）…",
    "healthChecks.transformations.progress.remaining": "残りの変換ページを取得中…",
    "healthChecks.transformations.progress.queries":
      "インラインクエリがない変換のSQLを解決中…",
    "healthChecks.transformations.progress.dmv":
      "クエリ内のデータモデルバージョン参照をスキャン中…",
    "healthChecks.transformations.progress.noop":
      "変換ごとに最新ジョブの書き込みメトリクスを確認中…",
    "healthChecks.transformations.progress.noopCount": "{current} / {total} 件の変換",
    "healthChecks.transformations.sampleLimit":
      "最初の {count} 件の変換のみ読み込み済みです。プロジェクトにはさらに存在する場合があります。",
    "healthChecks.transformations.loadAll": "すべての変換を読み込む",
    "healthChecks.transformations.partialDisclaimer":
      "結果は変換のサブセットに基づきます。プロジェクト内のすべての変換に含めるにはすべて読み込んでください。",
    "healthChecks.transformations.noops.title": "変換の書き込み vs No-op",
    "healthChecks.transformations.noops.description":
      "最後のジョブですべての書き込みがNo-opだった変換をフラグします。これはデータが実際に変更されなかったことを意味し、冗長な実行によるリソースの浪費を示唆します。",
    "healthChecks.transformations.noops.error": "変換メトリクスの読み込みに失敗しました。",
    "healthChecks.transformations.noops.count":
      "{total} 件中 {count} 件の変換で、最後の実行ですべての書き込みがNo-opでした。",
    "healthChecks.transformations.noops.detail": "{writes} 件の書き込み、すべてNo-op",
    "healthChecks.transformations.noops.allGood":
      "最近のジョブがある {total} 件すべての変換で実効的な書き込みがありました。",
    "healthChecks.dataModelVersioning.title": "データモデルバージョンの一貫性",
    "healthChecks.dataModelVersioning.description":
      "同じデータモデルを参照する変換は同じバージョンを使用する必要があります。不整合があると予期しない動作の原因になります。",
    "healthChecks.dataModelVersioning.error": "変換のデータモデル使用状況の読み込みに失敗しました。",
    "healthChecks.dataModelVersioning.inconsistenciesCount":
      "{count} 件のデータモデルで、変換間のバージョン不整合があります。",
    "healthChecks.dataModelVersioning.allConsistent":
      "すべてのデータモデルで変換間のバージョンが一貫しています。",
    "healthChecks.versioning.title": "バージョニング",
    "healthChecks.versioning.description":
      "変換で参照されていないデータモデルおよびビューのバージョンと、未使用のモデルバージョンを削除した場合に孤立するビューバージョン。",
    "healthChecks.versioning.error": "バージョニングチェックの読み込みに失敗しました。",
    "healthChecks.versioning.summary.category": "カテゴリ",
    "healthChecks.versioning.summary.total": "合計",
    "healthChecks.versioning.summary.used": "使用中",
    "healthChecks.versioning.summary.unused": "未使用",
    "healthChecks.versioning.summary.coverage": "カバレッジ",
    "healthChecks.versioning.summary.coverageValue": "{pct}%",
    "healthChecks.versioning.summary.dataModelVersions": "データモデルバージョン",
    "healthChecks.versioning.summary.viewVersions": "ビューバージョン",
    "healthChecks.versioning.summary.totals": "合計",
    "healthChecks.versioning.summary.orphanedNote":
      "未使用のモデルバージョンを削除すると、{count} のビューバージョンが孤立します（未使用ビューの{pct}%）。",
    "healthChecks.versioning.implicitViewRefs.title": "暗黙のビュー参照があるデータモデル",
    "healthChecks.versioning.implicitViewRefs.description":
      "これらのデータモデルバージョンのインラインビューに、明示的なバージョンが付いていないものが含まれています。不透明な自動生成ビューバージョンを避けるため、モデルでビューバージョン（例: v1）を固定してください。",
    "healthChecks.versioning.implicitViewRefs.none":
      "すべてのデータモデルバージョンで、インラインビューに明示的なバージョンが指定されています。",
    "healthChecks.versioning.implicitViewRefs.missingVersionTag": "モデル上で明示バージョンなし",
    "healthChecks.versioning.modelVersionsNotInUse.title": "使用されていないデータモデルバージョン",
    "healthChecks.versioning.modelVersionsNotInUse.description":
      "変換で参照されていないモデルバージョン（明示的または最新として）。",
    "healthChecks.versioning.modelVersionsNotInUse.none":
      "すべてのデータモデルバージョンが使用されています。",
    "healthChecks.versioning.completelyUnusedHint":
      "オレンジ = 変換またはデータモデルで参照されていません。",
    "healthChecks.versioning.viewVersionsNotInUse.title": "使用されていないビューバージョン",
    "healthChecks.versioning.viewVersionsNotInUse.description":
      "使用中のデータモデルバージョンで参照されていないビューバージョン。",
    "healthChecks.versioning.viewVersionsNotInUse.none":
      "すべてのビューバージョンが使用されています。",
    "healthChecks.versioning.viewVersionsWouldBeOrphaned.title":
      "未使用になるビューバージョン",
    "healthChecks.versioning.viewVersionsWouldBeOrphaned.description":
      "使用されていないデータモデルバージョンのみで参照されているビューバージョン。それらのモデルバージョンを削除すると、これらのビューバージョンが孤立します。",
    "healthChecks.versioning.viewVersionsWouldBeOrphaned.referencedBy": "のみで参照",
    "healthChecks.versioning.viewVersionsWouldBeOrphaned.none":
      "孤立するビューバージョンはありません。",
    "healthChecks.raw.overview.title": "Raw テーブル概要",
    "healthChecks.raw.overview.description":
      "空の Raw テーブルを特定するためのデータベース/テーブル走査。",
    "healthChecks.raw.overview.counts": "{databases} DB · {tables} テーブルを走査",
    "healthChecks.raw.overview.databasesProcessed": "DB 処理 {processed} / {total}",
    "healthChecks.raw.overview.tablesScanned": "走査テーブル数 {count}",
    "healthChecks.raw.overview.sampleNote":
      "行数が不明な場合は最大 10 行をサンプルします。",
    "healthChecks.raw.overview.sampleLimit":
      "小さいサンプルです（データベース 10 件、DB あたり最大 100 テーブル）。",
    "healthChecks.raw.overview.loadAll": "すべて読み込む",
    "healthChecks.raw.emptyTables.title": "空の Raw テーブル",
    "healthChecks.raw.emptyTables.description": "行数が 0 の Raw テーブル。",
    "healthChecks.raw.emptyTables.count": "{count} 件の Raw テーブルが空です。",
    "healthChecks.raw.emptyTables.created": "作成日 {date}",
    "healthChecks.raw.emptyTables.alert": "警告",
    "healthChecks.raw.emptyTables.none": "空の Raw テーブルはありません。",
    "healthChecks.raw.emptyTables.sampling": "行サンプリング {processed} / {total}",
    "healthChecks.functions.loading": "関数を読み込み中...",
    "healthChecks.functions.runtime.title": "関数ランタイムの一貫性",
    "healthChecks.functions.runtime.description":
      "すべての関数は同じ Python ランタイムを使用するべきです。",
    "healthChecks.functions.runtime.count":
      "関数のランタイムが {count} 種類あります。",
    "healthChecks.functions.runtime.functions": "関数",
    "healthChecks.functions.runtime.none": "すべての関数は {runtime} を使用しています。",
    "healthChecks.functions.runtime.defaultRuntime": "同一ランタイム",
    "healthChecks.functions.runtime.unknown": "不明なランタイム",
    "healthChecks.functions.lowPython.title": "Python 3.12 未満の関数",
    "healthChecks.functions.lowPython.description":
      "Python 3.12 より古いランタイムを使っている関数を警告します。",
    "healthChecks.functions.lowPython.note":
      "Python 3.11 はサポート終了が近づいています。完全サポート対象のバージョンを使うことで、リスクを抑え、セキュリティ更新を確実に受けられます。",
    "healthChecks.functions.lowPython.docs.python": "Python バージョン状況",
    "healthChecks.functions.lowPython.docs.azure": "Azure Functions の Python サポート",
    "healthChecks.functions.lowPython.count":
      "{count} 件の関数が Python 3.12 未満を使用しています。",
    "healthChecks.functions.lowPython.none":
      "Python 3.12 未満を使用している関数はありません。",
    "healthChecks.scheduling.title": "スケジュールの同時起動",
    "healthChecks.scheduling.description":
      "関数・変換・ワークフローの cron が同時に開始する設定を検出します。",
    "healthChecks.scheduling.loading": "スケジュールを読み込んでいます...",
    "healthChecks.scheduling.error": "スケジュールの読み込みに失敗しました。",
    "healthChecks.scheduling.counts.function": "関数のスケジュール {count} 件。",
    "healthChecks.scheduling.counts.transformation": "変換のスケジュール {count} 件。",
    "healthChecks.scheduling.counts.workflow": "ワークフローのスケジュール {count} 件。",
    "healthChecks.scheduling.overlaps.count":
      "{count} 件の cron が同時に開始します。",
    "healthChecks.scheduling.overlaps.note":
      "開始時刻をずらして、同時実行による負荷集中を避けてください。",
    "healthChecks.scheduling.overlaps.cron": "分/時: {cron}",
    "healthChecks.scheduling.overlaps.offsetTitle": "オフセット例",
    "healthChecks.scheduling.overlaps.none":
      "同時起動する cron は見つかりませんでした。",
    "healthChecks.scheduling.offsetExample.title": "オフセット例",
    "healthChecks.scheduling.offsetExample.body":
      "5 分ごと: 0/5 * * * * を 2/5 * * * * に変更し、2, 7, 12, 17 分に開始します。",
    "healthChecks.scheduling.types.function": "関数",
    "healthChecks.scheduling.types.transformation": "変換",
    "healthChecks.scheduling.types.workflow": "ワークフロー",
    "healthChecks.permissions.loading": "権限を読み込み中…",
    "healthChecks.permissions.progress.listing": "セキュリティグループを取得中…",
    "healthChecks.permissions.progress.analyzing":
      "グループ間でケイパビリティスコープを比較中…",
    "healthChecks.permissions.stats.title": "概要",
    "healthChecks.permissions.stats.groups": "セキュリティグループ",
    "healthChecks.permissions.stats.uniqueScopeLists":
      "一意の明示スコープリスト（ケイパビリティ＋スコープ種別＋エントリ）",
    "healthChecks.permissions.stats.totalCapabilities": "ケイパビリティ行（全グループ）",
    "healthChecks.permissions.stats.distinctTypes": "異なるケイパビリティ種別数",
    "healthChecks.permissions.stats.groupsNoCapabilities": "ケイパビリティがないグループ",
    "healthChecks.permissions.stats.allScopeRows": "無制限（all）スコープのケイパビリティ行",
    "healthChecks.permissions.stats.explicitScopeRows":
      "データセット／スペース／テーブル／ID スコープのあるケイパビリティ行",
    "healthChecks.permissions.stats.driftFindings": "スコープドリフトの検出件数",
    "healthChecks.permissions.drift.title": "権限スコープのドリフト",
    "healthChecks.permissions.drift.description":
      "ほぼ同一だが僅かに異なるスコープを持つケイパビリティを検出します。",
    "healthChecks.permissions.drift.count":
      "{count} 件のケイパビリティでスコープがほぼ同じです。",
    "healthChecks.permissions.drift.none": "近似したスコープ差分は検出されませんでした。",
    "healthChecks.permissions.drift.emptyList": "なし。",
    "healthChecks.permissions.drift.noActions": "アクションなし",
    "healthChecks.permissions.drift.explain.show": "説明",
    "healthChecks.permissions.drift.explain.hide": "非表示",
    "healthChecks.permissions.drift.common": "共通のエントリ",
    "healthChecks.permissions.drift.uniqueLeft": "{group} のみに存在",
    "healthChecks.permissions.drift.uniqueRight": "{group} のみに存在",
    "healthChecks.permissions.drift.uniqueToPrefix": "",
    "healthChecks.permissions.drift.uniqueToSuffix": " のみに存在",
    "healthChecks.permissions.drift.itemDiff":
      "{capability} ({actions}) · {scopeType} に小さな差分: {left} と {right}",
    "healthChecks.permissions.drift.unknownCapability": "不明",
    "dataCatalog.tooltip.empty": "接続がありません。",
  },
};

const I18nContext = createContext<I18nContextValue | null>(null);

export function I18nProvider({ children }: { children: React.ReactNode }) {
  const [language, setLanguage] = useState<Language>("en");

  const value = useMemo<I18nContextValue>(() => {
    const t = (key: string, params?: TranslationParams) => {
      const template = translations[language]?.[key] ?? translations.en[key] ?? key;
      if (!params) return template;
      return Object.entries(params).reduce(
        (result, [paramKey, value]) =>
          result.replace(new RegExp(`\\{${paramKey}\\}`, "g"), String(value)),
        template
      );
    };
    return { language, setLanguage, t };
  }, [language]);

  return <I18nContext.Provider value={value}>{children}</I18nContext.Provider>;
}

export function useI18n() {
  const ctx = useContext(I18nContext);
  if (!ctx) {
    throw new Error("useI18n must be used within I18nProvider");
  }
  return ctx;
}
