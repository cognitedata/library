import type { AppServices } from '../services/createServices';
import type { QualityDashboardService } from '../services/QualityDashboardService';

/**
 * The services with some quality-service methods replaced. The mock service is a class, so spreading it would drop its
 * service methods; this keeps every method that is not replaced.
 */
export function withQualityService(services: AppServices, overrides: Partial<QualityDashboardService>): AppServices {
  const base = services.qualityDashboardService;
  const qualityDashboardService = new Proxy<QualityDashboardService>(base, {
    get(target, property) {
      if (Reflect.has(overrides, property)) {
        return Reflect.get(overrides, property);
      }
      const value: unknown = Reflect.get(target, property);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  });
  return { ...services, qualityDashboardService };
}
