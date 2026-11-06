import { MOCK_DATASET } from '../../mock/dataset';
import {
  getRelationshipsForView,
  getViewsForDataModel,
  MOCK_DATA_MODELS,
} from '../../mock/discovery';
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

import { withLatency, type LatencyMode } from './latency';

type MockCatalogServiceOptions = {
  latency: LatencyMode;
};

export class MockCatalogService implements CatalogService {
  private readonly latency: LatencyMode;

  constructor(options: MockCatalogServiceOptions) {
    this.latency = options.latency;
  }

  getFacilities(): Promise<readonly Facility[]> {
    return withLatency(() => MOCK_DATASET.facilities, this.latency);
  }

  getLocations(): Promise<readonly Location[]> {
    return withLatency(
      () => [
        {
          id: 'demo-north-central',
          name: 'Demo North and Central',
          parentId: null,
          facilityIds: ['demo-north', 'demo-central'],
        },
        { id: 'demo-north', name: 'Demo North', parentId: 'demo-north-central', facilityIds: ['demo-north'] },
        { id: 'demo-central', name: 'Demo Central', parentId: 'demo-north-central', facilityIds: ['demo-central'] },
      ],
      this.latency
    );
  }

  getDivisions(): Promise<readonly Division[]> {
    return withLatency(() => MOCK_DATASET.divisions, this.latency);
  }

  getDisciplines(): Promise<readonly Discipline[]> {
    return withLatency(() => MOCK_DATASET.disciplines, this.latency);
  }

  getDataOwners(): Promise<readonly DataOwner[]> {
    return withLatency(() => MOCK_DATASET.owners, this.latency);
  }

  getDataModels(): Promise<readonly DiscoveredDataModel[]> {
    return withLatency(() => MOCK_DATA_MODELS, this.latency);
  }

  getViews(dataModelSpace: string): Promise<readonly DiscoveredView[]> {
    return withLatency(() => getViewsForDataModel(dataModelSpace), this.latency);
  }

  getRelationships(sourceView: ViewRef): Promise<readonly DiscoveredRelationship[]> {
    return withLatency(() => getRelationshipsForView(sourceView), this.latency);
  }
}
