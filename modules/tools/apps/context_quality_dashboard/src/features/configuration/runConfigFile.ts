import { parse } from 'yaml';

import type { RunConfigurationDto } from '../../types';

/** One data model and the `View.property` links to measure in it. The same shape the run's `extraLinks` payload takes. */
export type RunConfigModel = {
  /** `space:externalId:version` */
  model: string;
  links: string[];
};

export type RunConfigErrorCode =
  | 'empty'
  | 'unreadable'
  | 'too-large'
  | 'not-a-list'
  | 'entry-not-a-mapping'
  | 'model-missing'
  | 'model-format'
  | 'links-missing'
  | 'link-format'
  | 'link-duplicate';

export type RunConfigError = {
  code: RunConfigErrorCode;
  /** The text the problem is about, when there is one. */
  subject?: string;
};

export type RunConfigResult =
  | { ok: true; models: RunConfigModel[] }
  | { ok: false; errors: RunConfigError[] };

/** A run's payload is sent to a function call, so the file stays small. A real file is a few lines per model. */
export const MAX_CONFIG_FILE_CHARS = 100_000;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isModelId(text: string): boolean {
  const parts = text.split(':');
  return parts.length === 3 && parts.every((part) => part.trim() !== '');
}

function isLink(text: string): boolean {
  const parts = text.split('.');
  return parts.length === 2 && parts.every((part) => part.trim() !== '');
}

function readEntry(entry: unknown, errors: RunConfigError[]): RunConfigModel | null {
  if (!isRecord(entry)) {
    errors.push({ code: 'entry-not-a-mapping', subject: String(entry) });
    return null;
  }
  const before = errors.length;
  const model = typeof entry.model === 'string' ? entry.model.trim() : '';
  if (model === '') {
    errors.push({ code: 'model-missing' });
  } else if (!isModelId(model)) {
    errors.push({ code: 'model-format', subject: model });
  }
  const rawLinks = entry.links;
  if (!Array.isArray(rawLinks) || rawLinks.length === 0) {
    errors.push({ code: 'links-missing', subject: model });
    return null;
  }
  const links: string[] = [];
  for (const raw of rawLinks) {
    const link = typeof raw === 'string' ? raw.trim() : '';
    if (!isLink(link)) {
      errors.push({ code: 'link-format', subject: String(raw) });
    } else if (links.includes(link)) {
      errors.push({ code: 'link-duplicate', subject: link });
    } else {
      links.push(link);
    }
  }
  return errors.length === before ? { model, links } : null;
}

/**
 * Reads the config file: YAML or JSON (JSON is YAML), a list of data models with their links. Nothing is guessed:
 * every problem is reported so the file can be fixed, and the run is never started from a file that did not pass.
 */
export function parseRunConfigFile(text: string): RunConfigResult {
  if (text.length > MAX_CONFIG_FILE_CHARS) {
    return { ok: false, errors: [{ code: 'too-large' }] };
  }
  let document: unknown;
  try {
    document = parse(text);
  } catch (error) {
    return { ok: false, errors: [{ code: 'unreadable', subject: error instanceof Error ? error.message : undefined }] };
  }
  if (document === null || document === undefined || (Array.isArray(document) && document.length === 0)) {
    return { ok: false, errors: [{ code: 'empty' }] };
  }
  if (!Array.isArray(document)) {
    return { ok: false, errors: [{ code: 'not-a-list' }] };
  }
  const errors: RunConfigError[] = [];
  const models = document.flatMap((entry): RunConfigModel[] => {
    const read = readEntry(entry, errors);
    return read === null ? [] : [read];
  });
  return errors.length > 0 ? { ok: false, errors } : { ok: true, models };
}

/** The part of a run's payload the config file supplies. */
export function toRunPayload(models: readonly RunConfigModel[]): { extraLinks: RunConfigModel[] } {
  return { extraLinks: models.map((entry) => ({ model: entry.model, links: [...entry.links] })) };
}

/** Flattens the latest run's measured views into the same shape a config file produces. */
export function runConfigModelsFromLatestRun(configuration: RunConfigurationDto): RunConfigModel[] {
  return configuration.models.map((entry) => ({
    model: entry.model,
    links: entry.views.flatMap((view) => view.properties.map((property) => `${view.view}.${property}`)),
  }));
}

function normalizeModels(models: readonly RunConfigModel[]): RunConfigModel[] {
  return [...models]
    .map((entry) => ({ model: entry.model, links: [...entry.links].sort() }))
    .sort((left, right) => left.model.localeCompare(right.model));
}

/** True when both lists name the same models and links, ignoring order. */
export function runConfigModelsEqual(
  left: readonly RunConfigModel[],
  right: readonly RunConfigModel[]
): boolean {
  const a = normalizeModels(left);
  const b = normalizeModels(right);
  if (a.length !== b.length) {
    return false;
  }
  return a.every(
    (entry, index) =>
      entry.model === b[index]?.model &&
      entry.links.length === b[index]?.links.length &&
      entry.links.every((link, linkIndex) => link === b[index]?.links[linkIndex])
  );
}

/**
 * Whether the draft would start a run that differs from the latest completed run.
 * False while the latest run is still loading (`undefined`) or when the payloads match.
 */
export function hasPendingRunConfig(
  models: readonly RunConfigModel[],
  latest: RunConfigurationDto | null | undefined
): boolean {
  if (latest === undefined) {
    return false;
  }
  if (latest === null) {
    return true;
  }
  return !runConfigModelsEqual(models, runConfigModelsFromLatestRun(latest));
}

/** i18n key explaining why Run custom configuration file is disabled; null when it would be enabled. */
export function getRunCustomConfigDisabledReasonKey(params: {
  models: readonly RunConfigModel[] | null;
  configChangedSinceLatestRun: boolean;
  busy: boolean;
  runInProgress: boolean;
  otherRunInProgress: boolean;
  latestRunConfiguration: RunConfigurationDto | null | undefined;
}): string | null {
  const {
    models,
    configChangedSinceLatestRun,
    busy,
    latestRunConfiguration,
  } = params;

  const enabled = configChangedSinceLatestRun && !busy && !params.runInProgress;
  if (enabled) {
    return null;
  }
  if (models === null) {
    return 'configuration.run.needsFile';
  }
  if (busy) {
    return 'configuration.run.disabled.busy';
  }
  if (latestRunConfiguration === undefined) {
    return 'configuration.run.disabled.loadingLatest';
  }
  if (!configChangedSinceLatestRun) {
    return 'configuration.run.disabled.matchesLatest';
  }
  return 'configuration.run.disabled.generic';
}
