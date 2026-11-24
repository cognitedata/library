import type { RunInput } from '../../services/RunService';
import type { DimensionId } from '../../types';
import type { RunConfigModel } from '../configuration/runConfigFile';

import { selectedLinksToRunConfigModels } from './configuredGroupRunRequest';

/**
 * What a person can ask a run for: everything (every dimension and the metrics of the configuration file), every
 * dimension without the file's metrics, only the file's metrics, one dimension, or an explicit selection of both.
 */
export type RunRequest =
  | { kind: 'everything' }
  | { kind: 'dimensions' }
  | { kind: 'configured' }
  | { kind: 'dimension'; dimensionId: DimensionId }
  | {
      kind: 'configuredView';
      groupId: string;
      viewLabel: string;
      links: readonly SelectedLink[];
    }
  | { kind: 'selection'; dimensionIds: readonly DimensionId[]; links: readonly SelectedLink[] };

/** One configured metric picked for a run: the data model it is in and its `View.property` link. */
export type SelectedLink = { model: string; link: string };

/** Whether the run measures the metrics of the configuration file, so that it cannot start without one. */
export function requestNeedsConfigFile(request: RunRequest): boolean {
  return (
    request.kind === 'everything' ||
    request.kind === 'configured' ||
    request.kind === 'configuredView' ||
    (request.kind === 'selection' && request.links.length > 0)
  );
}

/** Groups picked links by data model, in first-seen order, each link once. */
function groupLinks(links: readonly SelectedLink[]): RunConfigModel[] {
  return selectedLinksToRunConfigModels(links);
}

/** The input the workflow is started with for a request. */
export function toRunInput(request: RunRequest, models: readonly RunConfigModel[] | null): RunInput {
  const extraLinks = (models ?? []).map((entry) => ({ model: entry.model, links: [...entry.links] }));
  if (requestNeedsConfigFile(request) && models === null) {
    throw new Error('This run measures the metrics of the configuration file, and there is none.');
  }
  switch (request.kind) {
    case 'selection': {
      if (request.dimensionIds.length === 0 && request.links.length === 0) {
        throw new Error('Nothing is selected to run.');
      }
      return {
        extraLinks: groupLinks(request.links),
        scope: { dimensions: [...request.dimensionIds], configured: request.links.length > 0 },
      };
    }
    case 'everything':
      return { extraLinks, scope: { dimensions: 'all', configured: true } };
    case 'dimensions':
      return { extraLinks: [], scope: { dimensions: 'all', configured: false } };
    case 'configured':
      return { extraLinks, scope: { dimensions: [], configured: true } };
    case 'dimension':
      return { extraLinks: [], scope: { dimensions: [request.dimensionId], configured: false } };
    case 'configuredView':
      if (request.links.length === 0) {
        throw new Error('Nothing is selected to run.');
      }
      return {
        extraLinks: groupLinks(request.links),
        scope: { dimensions: [], configured: true },
      };
  }
}

/** A stable name for a request, for test ids and translation keys. */
export function requestKey(request: RunRequest): string {
  if (request.kind === 'dimension') {
    return `dimension-${request.dimensionId}`;
  }
  if (request.kind === 'configuredView') {
    return `configured-view-${request.groupId}`;
  }
  return request.kind;
}
