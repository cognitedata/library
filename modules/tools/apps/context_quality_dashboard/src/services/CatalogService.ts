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
} from '../types';

export interface CatalogService {
  getFacilities(): Promise<readonly Facility[]>;
  /** The location filters the latest run can use as a scope. None when the project has none or they cannot be read. */
  getLocations(): Promise<readonly Location[]>;
  getDivisions(): Promise<readonly Division[]>;
  getDisciplines(): Promise<readonly Discipline[]>;
  getDataOwners(): Promise<readonly DataOwner[]>;
  getDataModels(): Promise<readonly DiscoveredDataModel[]>;
  getViews(dataModelSpace: string): Promise<readonly DiscoveredView[]>;
  getRelationships(sourceView: ViewRef): Promise<readonly DiscoveredRelationship[]>;
}
