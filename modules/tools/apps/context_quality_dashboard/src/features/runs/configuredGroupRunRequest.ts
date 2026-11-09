import type { ConfiguredGroupDto } from '../../types';
import type { RunConfigModel } from '../configuration/runConfigFile';

import type { RunRequest, SelectedLink } from './runRequest';

/** Groups selected links by data model for the workflow payload and RunRequestButton validation. */
export function selectedLinksToRunConfigModels(links: readonly SelectedLink[]): RunConfigModel[] {
  const byModel = new Map<string, string[]>();
  for (const { model, link } of links) {
    const existing = byModel.get(model) ?? [];
    if (!existing.includes(link)) {
      existing.push(link);
    }
    byModel.set(model, existing);
  }
  return [...byModel].map(([model, grouped]) => ({ model, links: grouped }));
}

/** Workflow run that remeasures every configured metric in one view group. */
export function configuredGroupRunRequest(group: ConfiguredGroupDto): {
  request: Extract<RunRequest, { kind: 'configuredView' }>;
  models: RunConfigModel[];
} {
  const links: SelectedLink[] = group.metrics.map((metric) => ({
    model: metric.modelId,
    link: `${metric.view}.${metric.label}`,
  }));
  return {
    request: {
      kind: 'configuredView',
      groupId: group.id,
      viewLabel: group.viewLabel,
      links,
    },
    models: selectedLinksToRunConfigModels(links),
  };
}
