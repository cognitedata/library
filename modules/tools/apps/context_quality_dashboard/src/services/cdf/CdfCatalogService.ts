import type {
  DataOwner,
  Discipline,
  DiscoveredDataModel,
  DiscoveredRelationship,
  DiscoveredView,
  Division,
  Facility,
  Location,
  ViewRef,
} from '../../types';
import type { CatalogService } from '../CatalogService';

import { titleCase } from './exclusionBuckets';
import type { AggregateRow } from './metricRows';
import type { QualityStreamReader } from './QualityStreamReader';

type Options = {
  reader: QualityStreamReader;
};

export class CdfCatalogService implements CatalogService {
  private readonly reader: QualityStreamReader;

  constructor(options: Options) {
    this.reader = options.reader;
  }

  async getFacilities(): Promise<readonly Facility[]> {
    // a facility can exist only for the configured metrics: its space holds none of the entities the built-ins count
    const run = await this.reader.latestRun();
    const configured = await this.reader.extraLinks(run.runId);
    const ids = new Set([
      ...(await this.distinct((row) => row.facilityId)),
      ...configured.flatMap((row) => (row.facilityId === null ? [] : [row.facilityId])),
    ]);
    return [...ids].sort().map((id) => ({ id, name: titleCase(id) }));
  }

  async getLocations(): Promise<readonly Location[]> {
    const run = await this.reader.latestRun();
    return (await this.reader.locations(run.runId)).map((row) => ({
      id: row.id,
      name: row.name,
      parentId: row.parentId,
      facilityIds: row.facilityIds,
    }));
  }

  async getDivisions(): Promise<readonly Division[]> {
    return (await this.distinct((row) => row.divisionId)).map((id) => ({ id, name: titleCase(id) }));
  }

  async getDisciplines(): Promise<readonly Discipline[]> {
    return (await this.distinct((row) => row.disciplineId)).map((id) => ({ id, name: titleCase(id) }));
  }

  async getDataOwners(): Promise<readonly DataOwner[]> {
    return (await this.distinct((row) => row.ownerId)).map((id) => ({
      id,
      name: titleCase(id),
      contactLabel: '',
    }));
  }

  // Data model discovery is not part of the metrics stream, so nothing is discovered. Nothing is invented either.
  async getDataModels(): Promise<readonly DiscoveredDataModel[]> {
    return [];
  }

  async getViews(_dataModelSpace: string): Promise<readonly DiscoveredView[]> {
    return [];
  }

  async getRelationships(_sourceView: ViewRef): Promise<readonly DiscoveredRelationship[]> {
    return [];
  }

  private async distinct(pick: (row: AggregateRow) => string | null): Promise<string[]> {
    const run = await this.reader.latestRun();
    const rows = await this.reader.aggregates(run.runId);
    const ids = new Set<string>();
    for (const row of rows) {
      const id = pick(row);
      if (id !== null) {
        ids.add(id);
      }
    }
    return [...ids].sort();
  }
}
