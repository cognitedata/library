import { formatCount } from '../overview/contextualisationCoverageLabels';

type AverageGaugeProps = {
  /** The average, 0 to 100. */
  percent: number;
  /** How many instances the average is over. */
  count: number;
  ariaLabel: string;
  countLabel: string;
};

/**
 * The tile of a metric that is a value, not a share of a population: a scale from 0 to 100% with the average filled in,
 * and how many instances it is the average of. It has the bar and the legend line of the other tiles, so they line up.
 */
export function AverageGauge({ percent, count, ariaLabel, countLabel }: AverageGaugeProps) {
  const filled = Math.max(0, Math.min(100, percent));
  return (
    <div className="flex w-full min-w-0 flex-col gap-1.5" data-testid="average-gauge">
      <div className="flex h-3 w-full min-w-0 overflow-hidden rounded-sm bg-muted/80" role="img" aria-label={ariaLabel}>
        <div className="h-full bg-sky-600" style={{ width: `${filled}%` }} data-testid="average-gauge-fill" />
      </div>
      <div className="flex w-full justify-between text-[11px] leading-none tabular-nums text-muted-foreground" aria-hidden="true">
        <span>0%</span>
        <span>100%</span>
      </div>
      <ul className="mt-1 flex w-full min-w-0 flex-col gap-1.5">
        <li className="flex w-full min-w-0 items-center gap-1.5 text-xs text-muted-foreground">
          <span className="size-2 shrink-0 rounded-sm bg-sky-600" aria-hidden="true" />
          <span className="min-w-0 flex-1 truncate">{countLabel}</span>
          <span className="shrink-0 tabular-nums text-foreground">{formatCount(count)}</span>
        </li>
      </ul>
    </div>
  );
}
