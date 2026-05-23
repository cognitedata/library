import type { Locale } from "./types";
import type { LocaleMessages } from "./types";

/**
 * Curated overrides for operator UI terms where batch translation misreads context
 * (e.g. Run tab = execute workflow, not physical running).
 */
export const localeTermFixes: Partial<Record<Locale, LocaleMessages>> = {
  ar: {
    "tabs.run": "تشغيل",
    "status.running": "قيد التشغيل…",
    "run.stepStatus.running": "قيد التشغيل",
    "patterns.category.process_line": "خط العملية",
  },
  es: {
    "tabs.run": "Ejecutar",
    "status.running": "Ejecutando…",
    "run.stepStatus.running": "en ejecución",
    "run.extract": "Ejecutar extracción",
    "run.create": "Ejecutar creación de jerarquía",
    "run.write": "Ejecutar escritura",
    "run.all": "Ejecutar todo",
    "patterns.category.general": "General",
    "patterns.category.process_line": "Línea de proceso",
    "configure.step.extract": "Extraer",
  },
  fr: {
    "tabs.run": "Exécuter",
    "configure.step.extract": "Extraire",
    "run.extract": "Lancer l'extraction",
    "run.create": "Lancer la création de hiérarchie",
    "run.write": "Lancer l'écriture",
    "run.all": "Tout exécuter",
    "run.workflowSkipped": "Hors de cette exécution",
    "patterns.category.process_line": "Ligne de procédé",
    "brand.cognite": "Cognite",
  },
  nb: {
    "tabs.run": "Kjør",
    "run.stepStatus.running": "kjører",
    "patterns.category.general": "Generelt",
    "patterns.category.process_line": "Prosesslinje",
    "btn.cancel": "Avbryt",
    "btn.save": "Lagre",
  },
  pt: {
    "tabs.run": "Executar",
    "status.running": "Executando…",
    "run.stepStatus.running": "em execução",
    "run.extract": "Executar extração",
    "run.create": "Executar criação de hierarquia",
    "run.write": "Executar gravação",
    "run.all": "Executar tudo",
    "btn.save": "Salvar",
    "brand.cognite": "Cognite",
    "patterns.category.process_line": "Linha de processo",
  },
  hi: {
    "tabs.run": "चलाएँ",
    "status.running": "चल रहा है…",
    "run.stepStatus.running": "चल रहा है",
    "patterns.category.process_line": "प्रक्रिया लाइन",
  },
  ja: {
    "tabs.run": "実行",
    "status.running": "実行中…",
    "run.stepStatus.running": "実行中",
    "patterns.category.general": "一般",
    "patterns.category.instrument": "計器",
    "patterns.category.process_line": "プロセスライン",
    "controls.themeLight": "ライト",
    "controls.themeDark": "ダーク",
  },
  zh: {
    "tabs.run": "运行",
    "status.running": "运行中…",
    "run.stepStatus.running": "运行中",
    "btn.save": "保存",
    "configure.step.extract": "提取",
    "controls.themeLight": "浅色",
    "controls.themeDark": "深色",
    "patterns.category.instrument": "仪表",
    "patterns.category.process_line": "工艺管线",
    "brand.cognite": "Cognite",
  },
};
