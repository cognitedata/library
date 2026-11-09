import { useContext } from 'react';

import { ServicesContext } from './servicesContextValue';

export function useServices() {
  const services = useContext(ServicesContext);
  if (services === null) {
    throw new Error('useServices must be used within a ServicesProvider.');
  }
  return services;
}

export function useQualityDashboardService() {
  return useServices().qualityDashboardService;
}

export function useConfigurationService() {
  return useServices().configurationService;
}

export function useCatalogService() {
  return useServices().catalogService;
}

export function useExportService() {
  return useServices().exportService;
}

export function useRunService() {
  return useServices().runService;
}

export function useBackend() {
  return useServices().backend;
}
