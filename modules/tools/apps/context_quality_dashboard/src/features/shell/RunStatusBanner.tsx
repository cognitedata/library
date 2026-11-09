import {
  Alert,
  AlertActions,
  AlertDescription,
} from '@cognite/aura/components/alert';
import { Button } from '@cognite/aura/components/button';
import { IconCircleCheckFilled, IconInfoCircleFilled, IconX } from '@cognite/aura/icons';

import { appContentHorizontalInsetClassName } from './appContentHorizontalInset';
import { useRunBannerViewModel } from './useRunBannerViewModel';

type BannerAlertVariant = 'info' | 'error' | 'success';

export function RunStatusBanner() {
  const { banner, title, body, dismiss, dismissLabel, primaryActionLabel, onPrimaryAction } = useRunBannerViewModel();

  if (!banner.visible) {
    return null;
  }

  const alertVariant = bannerAlertVariant(banner.variant);
  const variantTestId = `run-status-banner-${banner.variant}`;

  return (
    <div
      className={`relative z-10 w-full shrink-0 ${bannerWrapperBackgroundClass(alertVariant)}`}
      data-testid="run-status-banner"
    >
      <Alert
        variant={alertVariant === 'success' ? 'default' : alertVariant}
        alertStyle="strip"
        role="status"
        aria-live={banner.variant === 'running' ? 'polite' : undefined}
        icon={bannerIcon(banner.variant)}
        className={`${appContentHorizontalInsetClassName} ${bannerAlertClassName(alertVariant)}`}
        data-testid={variantTestId}
      >
        <AlertDescription className="min-w-0 truncate">
          <span className="font-semibold text-foreground">{title}</span>
          {body === '' ? null : (
            <span className="text-secondary-foreground">{` — ${body}`}</span>
          )}
        </AlertDescription>
        {dismiss === null && onPrimaryAction === null ? null : (
          <AlertActions>
            {onPrimaryAction === null ? null : (
              <Button
                type="button"
                variant="ghost"
                size="sm"
                className="shrink-0"
                data-testid="run-status-banner-primary-action"
                onClick={onPrimaryAction}
              >
                {primaryActionLabel}
              </Button>
            )}
            {dismiss === null ? null : (
              <Button
                type="button"
                variant="ghost"
                size="sm"
                className="size-7 shrink-0 p-0"
                aria-label={dismissLabel}
                data-testid="run-status-banner-dismiss"
                onClick={dismiss}
              >
                <IconX className="size-4" aria-hidden />
              </Button>
            )}
          </AlertActions>
        )}
      </Alert>
    </div>
  );
}

function bannerAlertVariant(variant: 'running' | 'success' | 'failed'): BannerAlertVariant {
  if (variant === 'running') {
    return 'info';
  }
  if (variant === 'success') {
    return 'success';
  }
  return 'error';
}

function bannerAlertClassName(variant: BannerAlertVariant): string {
  const base = 'w-full max-w-none rounded-none border-x-0 border-t-0 border-b shadow-none';
  if (variant === 'success') {
    return `${base} !bg-success-background/10 border-l-4 border-l-success-foreground [&>[data-slot=alert-icon]]:text-success-foreground`;
  }
  if (variant === 'info') {
    return `${base} border-l-4 border-l-info-foreground`;
  }
  return `${base} border-l-4 border-l-destructive-foreground`;
}

function bannerWrapperBackgroundClass(variant: BannerAlertVariant): string {
  if (variant === 'success') {
    return 'bg-success-background/10';
  }
  if (variant === 'info') {
    return 'bg-info-muted-background';
  }
  return 'bg-destructive-muted-background';
}

function bannerIcon(variant: 'running' | 'success' | 'failed') {
  if (variant === 'running') {
    return (
      <IconInfoCircleFilled
        className="size-4 text-info-foreground"
        aria-hidden
        data-testid="run-status-banner-info-icon"
      />
    );
  }
  if (variant === 'success') {
    return <IconCircleCheckFilled className="size-4 text-success-foreground" aria-hidden />;
  }
  return undefined;
}
