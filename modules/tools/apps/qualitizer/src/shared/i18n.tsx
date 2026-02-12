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
    "nav.processing": "Processing",
    "nav.permissions": "Permissions",
    "nav.dataCatalog": "Data Catalog",
    "nav.healthChecks": "Health Checks",
    "apiError.showDetails": "Show details",
    "apiError.hideDetails": "Hide details",
    "apiError.section.api": "API",
    "apiError.section.request": "Request body",
    "apiError.section.details": "Details",
    "apiError.docsLink": "Open API documentation",
    "apiError.permissionsHint":
      "Permission requirements are listed at the top of each documentation page.",
    "processing.title": "Processing",
    "processing.subtitle": "Function execution concurrency for the last {hoursWindow} hours.",
    "processing.loading.functions": "Loading functions...",
    "processing.loading.stats": "Loading execution stats...",
    "processing.loading.runs": "Loading function executions...",
    "processing.loading.transformations": "Loading transformations...",
    "processing.loading.workflows": "Loading workflows...",
    "processing.loading.extractors": "Loading extraction pipelines...",
    "processing.error.runs": "Failed to load function executions.",
    "processing.function.defaultName": "Function {id}",
    "processing.error.transformations": "Failed to load transformations.",
    "processing.transformation.defaultName": "Transformation {id}",
    "processing.error.workflows": "Failed to load workflows.",
    "processing.error.extractors": "Failed to load extraction pipelines.",
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
      "Parallel function executions per {bucketSeconds}-second bucket.",
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
    "permissions.error": "Failed to load permissions.",
    "permissions.currentUser": "Current user",
    "permissions.currentSuffix": "(current)",
    "permissions.group.fallback": "Group {id}",
    "permissions.upload.label": "Upload access info JSON files",
    "permissions.upload.uploading": "Uploading...",
    "permissions.upload.invalid": "Invalid access info in {fileName}",
    "permissions.upload.empty": "No users available yet. Upload permission export JSON files to compare memberships.",
    "permissions.spaces.none": "No spaces found.",
    "permissions.datasets.none": "No data sets found.",
    "permissions.dataset.unnamed": "Unnamed data set",
    "permissions.space.unnamed": "Unnamed space",
    "permissions.legend.label": "Legend:",
    "permissions.legend.space": "Group has explicit space scope",
    "permissions.legend.dataset": "Group has explicit data set scope",
    "permissions.compare.membership": "Compare group membership by uploaded users.",
    "permissions.scopes.space.title": "Space access",
    "permissions.scopes.space.description": "Space scope entries per security group.",
    "permissions.scopes.dataset.title": "Data set access",
    "permissions.scopes.dataset.description": "Dataset scope entries per security group.",
    "permissions.compare.title": "User comparison",
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
    "dataCatalog.subtitle": "Columns: Data models → Views → Fields.",
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
    "healthChecks.raw.overview.title": "Raw tables overview",
    "healthChecks.raw.overview.description":
      "Database/table scan to identify empty or stale raw tables.",
    "healthChecks.raw.overview.counts": "{databases} databases · {tables} tables scanned",
    "healthChecks.raw.overview.databasesProcessed":
      "Databases processed {processed} / {total}",
    "healthChecks.raw.overview.tablesScanned": "Tables scanned {count}",
    "healthChecks.raw.overview.sampleNote":
      "Samples up to 10 rows when row counts are missing.",
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
    "healthChecks.permissions.loading": "Loading permissions...",
    "healthChecks.permissions.drift.title": "Permission scope drift",
    "healthChecks.permissions.drift.description":
      "Capabilities with nearly identical scopes that differ slightly between groups.",
    "healthChecks.permissions.drift.count":
      "{count} capability scopes look almost the same.",
    "healthChecks.permissions.drift.none": "No near-duplicate capability scopes detected.",
    "healthChecks.permissions.drift.noActions": "no actions",
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
    "nav.processing": "処理",
    "nav.permissions": "権限",
    "nav.dataCatalog": "データカタログ",
    "nav.healthChecks": "ヘルスチェック",
    "apiError.showDetails": "詳細を表示",
    "apiError.hideDetails": "詳細を非表示",
    "apiError.section.api": "API",
    "apiError.section.request": "リクエスト本文",
    "apiError.section.details": "詳細",
    "apiError.docsLink": "API ドキュメントを開く",
    "apiError.permissionsHint":
      "必要な権限は各ドキュメントページの冒頭に記載されています。",
    "processing.title": "処理状況",
    "processing.subtitle": "直近 {hoursWindow} 時間の実行並列数を表示します。",
    "processing.loading.functions": "関数を読み込み中...",
    "processing.loading.stats": "実行統計を読み込み中...",
    "processing.loading.runs": "関数実行を読み込み中...",
    "processing.loading.transformations": "変換を読み込み中...",
    "processing.loading.workflows": "ワークフローを読み込み中...",
    "processing.loading.extractors": "抽出パイプラインを読み込み中...",
    "processing.error.runs": "関数実行の読み込みに失敗しました。",
    "processing.function.defaultName": "関数 {id}",
    "processing.error.transformations": "変換の読み込みに失敗しました。",
    "processing.transformation.defaultName": "変換 {id}",
    "processing.error.workflows": "ワークフローの読み込みに失敗しました。",
    "processing.error.extractors": "抽出パイプラインの読み込みに失敗しました。",
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
      "{bucketSeconds} 秒ごとの関数実行の並列数。",
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
    "permissions.error": "権限の読み込みに失敗しました。",
    "permissions.currentUser": "現在のユーザー",
    "permissions.currentSuffix": "（現在）",
    "permissions.group.fallback": "グループ {id}",
    "permissions.upload.label": "アクセス情報の JSON ファイルをアップロード",
    "permissions.upload.uploading": "アップロード中...",
    "permissions.upload.invalid": "{fileName} のアクセス情報が不正です",
    "permissions.upload.empty": "ユーザーがありません。権限エクスポート JSON をアップロードしてください。",
    "permissions.spaces.none": "スペースが見つかりません。",
    "permissions.datasets.none": "データセットが見つかりません。",
    "permissions.dataset.unnamed": "無名のデータセット",
    "permissions.space.unnamed": "無名のスペース",
    "permissions.legend.label": "凡例:",
    "permissions.legend.space": "グループに明示的なスペーススコープがある",
    "permissions.legend.dataset": "グループに明示的なデータセットスコープがある",
    "permissions.compare.membership": "アップロードしたユーザーのグループ所属を比較します。",
    "permissions.scopes.space.title": "スペースアクセス",
    "permissions.scopes.space.description": "セキュリティグループごとのスペーススコープ。",
    "permissions.scopes.dataset.title": "データセットアクセス",
    "permissions.scopes.dataset.description": "セキュリティグループごとのデータセットスコープ。",
    "permissions.compare.title": "ユーザー比較",
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
    "dataCatalog.subtitle": "列: データモデル → ビュー → フィールド。",
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
    "healthChecks.raw.overview.title": "Raw テーブル概要",
    "healthChecks.raw.overview.description":
      "空の Raw テーブルを特定するためのデータベース/テーブル走査。",
    "healthChecks.raw.overview.counts": "{databases} DB · {tables} テーブルを走査",
    "healthChecks.raw.overview.databasesProcessed": "DB 処理 {processed} / {total}",
    "healthChecks.raw.overview.tablesScanned": "走査テーブル数 {count}",
    "healthChecks.raw.overview.sampleNote":
      "行数が不明な場合は最大 10 行をサンプルします。",
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
    "healthChecks.permissions.loading": "権限を読み込み中...",
    "healthChecks.permissions.drift.title": "権限スコープのドリフト",
    "healthChecks.permissions.drift.description":
      "ほぼ同一だが僅かに異なるスコープを持つケイパビリティを検出します。",
    "healthChecks.permissions.drift.count":
      "{count} 件のケイパビリティでスコープがほぼ同じです。",
    "healthChecks.permissions.drift.none": "近似したスコープ差分は検出されませんでした。",
    "healthChecks.permissions.drift.noActions": "アクションなし",
    "healthChecks.permissions.drift.itemDiff":
      "{capability} ({actions}) · {scopeType} に小さな差分: {left} と {right}",
    "healthChecks.permissions.drift.unknownCapability": "不明",
    "dataCatalog.tooltip.empty": "接続がありません。",
  },
};

const I18nContext = createContext<I18nContextValue | null>(null);

export function I18nProvider({ children }: { children: React.ReactNode }) {
  const [language, setLanguage] = useState<Language>(() => {
    if (typeof window === "undefined") return "en";
    const stored = window.localStorage.getItem("qualitizer.language") as Language | null;
    return stored ?? "en";
  });

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
    const setLang = (next: Language) => {
      setLanguage(next);
      if (typeof window !== "undefined") {
        window.localStorage.setItem("qualitizer.language", next);
      }
    };
    return { language, setLanguage: setLang, t };
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
