import { screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ProductShell } from '../../app/ProductShell';
import { createMockServices } from '../../services/mock/createMockServices';
import { renderOverview } from '../../test/renderOverview';
import type { AlertsDto } from '../../types';

import { AlertsPage } from './AlertsPage';

function renderAlerts(
  hostState: Record<string, unknown> = { page: 'alerts' },
  services = createMockServices({ latency: 0 })
) {
  return renderOverview(<ProductShell hostInitialState={JSON.stringify(hostState)} />, { services });
}

describe('AlertsPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.setItem('context-quality-dashboard.language', 'en');
  });

  it('renders the alerts page', async () => {
    renderAlerts();

    await waitFor(() => {
      expect(screen.getByText('Alerts')).toBeInTheDocument();
    });
  });

  it('uses current filters in the alerts query', async () => {
    const services = createMockServices({ latency: 0 });
    const getAlerts = vi.spyOn(services.qualityDashboardService, 'getAlerts');

    renderAlerts({ page: 'alerts', filters: { facilityId: 'demo-north' } }, services);

    await waitFor(() => {
      expect(getAlerts).toHaveBeenCalledWith({ facilityId: 'demo-north' });
    });
  });

  it('updates the alerts query when filters change', async () => {
    const user = userEvent.setup();
    const services = createMockServices({ latency: 0 });
    const getAlerts = vi.spyOn(services.qualityDashboardService, 'getAlerts');

    renderAlerts({ page: 'alerts' }, services);

    await waitFor(() => {
      expect(screen.getByLabelText('Global filters')).toBeInTheDocument();
    });

    const filtersSection = screen.getByLabelText('Global filters');
    const facilityTrigger = within(filtersSection).getAllByRole('combobox')[0];
    await user.click(facilityTrigger);
    await user.click(screen.getByRole('option', { name: 'Demo North' }));

    await waitFor(() => {
      expect(getAlerts).toHaveBeenCalledWith({ facilityId: 'demo-north' });
    });
  });

  it('renders alert summary from the DTO', async () => {
    renderAlerts();

    await waitFor(() => {
      expect(screen.getByTestId('alert-summary')).toBeInTheDocument();
    });

    expect(screen.getByTestId('alert-summary-total')).toBeInTheDocument();
    expect(screen.getByTestId('alert-summary-error')).toBeInTheDocument();
    expect(screen.getByTestId('alert-summary-warning')).toBeInTheDocument();
  });

  it('renders critical, warning, and informational counts from the DTO', async () => {
    const services = createMockServices({ latency: 0 });
    const summary = { total: 3, critical: 1, warning: 2, informational: 0 };
    vi.spyOn(services.qualityDashboardService, 'getAlerts').mockResolvedValue({
      alerts: [
        {
          id: 'a1',
          type: 'tier-threshold',
          severity: 'critical',
          status: 'open',
          titleKey: 'alert.tierThreshold.title',
          bodyKey: 'alert.tierThreshold.body',
          actionKey: 'action.reviewMonitoring',
          dimensionId: 'time-series',
          metricId: 'tsToAssetRate',
          facilityId: 'demo-north',
          ownerId: 'instrumentation-lead',
          affectedCount: 2,
          createdAt: '2026-08-28T08:00:00.000Z',
        },
        {
          id: 'a2',
          type: 'pending-review',
          severity: 'warning',
          status: 'open',
          titleKey: 'alert.pendingReview.title',
          bodyKey: 'alert.pendingReview.body',
          actionKey: 'action.pendingReview',
          dimensionId: 'file-annotation',
          metricId: 'pendingReview',
          facilityId: 'demo-central',
          ownerId: 'pid-review-lead',
          affectedCount: 1,
          createdAt: '2026-08-29T10:30:00.000Z',
        },
        {
          id: 'a3',
          type: 'recurrence',
          severity: 'warning',
          status: 'open',
          titleKey: 'alert.recurrence.title',
          bodyKey: 'alert.recurrence.body',
          actionKey: 'action.reviewWorkOrders',
          dimensionId: 'maintenance',
          metricId: 'woWithoutNotification',
          facilityId: 'demo-north',
          ownerId: 'sap-steward',
          affectedCount: 3,
          createdAt: '2026-08-30T14:15:00.000Z',
        },
      ],
      summary,
    });

    renderAlerts({ page: 'alerts' }, services);

    await waitFor(() => {
      expect(screen.getByTestId('alert-summary-total')).toHaveTextContent('3');
    });

    expect(screen.getByTestId('alert-summary-error')).toHaveTextContent('1');
    expect(screen.getByTestId('alert-summary-warning')).toHaveTextContent('2');
  });

  it('renders alerts returned by the service in the table', async () => {
    renderAlerts();

    await waitFor(() => {
      expect(screen.getByTestId('alert-table')).toBeInTheDocument();
    });

    expect(screen.getByTestId('alert-row-alert-001')).toBeInTheDocument();
    expect(screen.getByText('Tier 1 time series rate below target')).toBeInTheDocument();
  });

  it('does not calculate alert severity in React', async () => {
    const services = createMockServices({ latency: 0 });
    const getAlerts = vi.spyOn(services.qualityDashboardService, 'getAlerts');

    renderAlerts({ page: 'alerts' }, services);

    await waitFor(() => {
      expect(getAlerts).toHaveBeenCalled();
    });

    const result = await getAlerts.mock.results[0]?.value as AlertsDto;
    expect(result.summary.critical).toBe(
      result.alerts.filter((alert) => alert.severity === 'critical').length
    );
  });

  it('navigates to dimensions when an alert has dimensionId', async () => {
    const user = userEvent.setup();
    renderAlerts();

    await waitFor(() => {
      expect(screen.getByTestId('alert-investigate-alert-001')).toBeInTheDocument();
    });

    await user.click(screen.getByTestId('alert-investigate-alert-001'));

    await waitFor(() => {
      expect(screen.getByRole('heading', { name: 'Time Series' })).toBeInTheDocument();
    });
  });

  it('preserves dimensionId and metricId in app state when navigating from an alert', async () => {
    const user = userEvent.setup();
    renderAlerts();

    await waitFor(() => {
      expect(screen.getByTestId('alert-investigate-alert-001')).toBeInTheDocument();
    });

    await user.click(screen.getByTestId('alert-investigate-alert-001'));

    await waitFor(() => {
      expect(screen.getByText('Time series to asset rate')).toBeInTheDocument();
    });
  });

  it('remains usable for alerts without metric navigation identifiers', async () => {
    renderAlerts();

    await waitFor(() => {
      expect(screen.getByTestId('alert-row-alert-004')).toBeInTheDocument();
    });

    expect(screen.getByText('Workflow run failed for quality refresh')).toBeInTheDocument();
    expect(screen.getByTestId('alert-investigate-alert-004')).toHaveTextContent('Open dimension');
  });

  it('renders loading state', () => {
    const services = createMockServices({ latency: 0 });

    renderOverview(<AlertsPage />, {
      services: {
        ...services,
        qualityDashboardService: {
          ...services.qualityDashboardService,
          getAlerts: () => new Promise(() => undefined),
        },
      },
      hostInitialState: JSON.stringify({ page: 'alerts' }),
    });

    expect(screen.getByLabelText('Loading alerts')).toBeInTheDocument();
  });

  it('renders error state', async () => {
    const services = createMockServices({ latency: 0 });

    renderOverview(<AlertsPage />, {
      services: {
        ...services,
        qualityDashboardService: {
          ...services.qualityDashboardService,
          getAlerts: () => Promise.reject(new Error('alerts failed')),
        },
      },
      hostInitialState: JSON.stringify({ page: 'alerts' }),
    });

    await waitFor(() => {
      expect(screen.getByText('Failed to load alerts')).toBeInTheDocument();
    });
  });

  it('renders empty state without fake alerts', async () => {
    const services = createMockServices({ latency: 0 });
    vi.spyOn(services.qualityDashboardService, 'getAlerts').mockResolvedValue({
      alerts: [],
      summary: { total: 0, critical: 0, warning: 0, informational: 0 },
    });

    renderAlerts({ page: 'alerts', filters: { facilityId: 'missing-facility' } }, services);

    await waitFor(() => {
      expect(screen.getByTestId('alerts-empty')).toBeInTheDocument();
    });

    expect(screen.queryByTestId('alert-table')).not.toBeInTheDocument();
  });

  it('renders English alert strings', async () => {
    renderAlerts();

    await waitFor(() => {
      expect(screen.getByLabelText('Alert summary')).toBeInTheDocument();
    });

    expect(within(screen.getByLabelText('Alert summary')).getByText('Critical')).toBeInTheDocument();
  });

  it('renders Japanese alert strings', async () => {
    localStorage.setItem('context-quality-dashboard.language', 'ja');

    renderAlerts();

    await waitFor(() => {
      expect(screen.getByLabelText('アラート概要')).toBeInTheDocument();
    });

    expect(within(screen.getByLabelText('アラート概要')).getByText('重大')).toBeInTheDocument();
  });
});
