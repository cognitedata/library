import { useEffect, useMemo, useState } from "react";
import { normalizeStatus, toTimestampLoose } from "@/shared/time-utils";
import type { ExtPipeConfigSummary, ExtPipeRunSummary, LoadState } from "./types";
import { useI18n } from "@/shared/i18n";

type UseExtractionPipelineDataArgs = {
  isSdkLoading: boolean;
  sdk: { project: string; get: Function; post: Function };
  windowRange: { start: number; end: number } | null;
};

export function useExtractionPipelineData({
  isSdkLoading,
  sdk,
  windowRange,
}: UseExtractionPipelineDataArgs) {
  const { t } = useI18n();
  const [extractorsStatus, setExtractorsStatus] = useState<LoadState>("idle");
  const [extractorsError, setExtractorsError] = useState<string | null>(null);
  const [extractorConfigs, setExtractorConfigs] = useState<ExtPipeConfigSummary[]>([]);
  const [extractorRunsAll, setExtractorRunsAll] = useState<
    Array<ExtPipeRunSummary & { externalId: string }>
  >([]);

  useEffect(() => {
    if (isSdkLoading) return;
    let cancelled = false;
    const loadExtractorConfigs = async () => {
      setExtractorsStatus("loading");
      setExtractorsError(null);
      setExtractorConfigs([]);
      setExtractorRunsAll([]);
      try {
        const configs: ExtPipeConfigSummary[] = [];
        let cursor: string | undefined;
        do {
          const response = await sdk.get<{
            items?: ExtPipeConfigSummary[];
            nextCursor?: string | null;
          }>(`/api/v1/projects/${sdk.project}/extpipes`, {
            params: { limit: "100", cursor },
          });
          configs.push(...(response.data?.items ?? []));
          cursor = response.data?.nextCursor ?? undefined;
        } while (cursor);

        if (!cancelled) {
          setExtractorConfigs(configs);
          setExtractorsStatus("success");
        }
      } catch (error) {
        if (!cancelled) {
          setExtractorsError(
            error instanceof Error ? error.message : t("processing.error.extractors")
          );
          setExtractorsStatus("error");
        }
      }
    };

    loadExtractorConfigs();
    return () => {
      cancelled = true;
    };
  }, [isSdkLoading, sdk, t]);

  useEffect(() => {
    if (isSdkLoading || !windowRange) return;
    if (extractorConfigs.length === 0) {
      setExtractorRunsAll([]);
      return;
    }
    let cancelled = false;
    const loadExtractorRuns = async () => {
      setExtractorsStatus("loading");
      setExtractorsError(null);
      setExtractorRunsAll([]);
      try {
        const runs: Array<ExtPipeRunSummary & { externalId: string }> = [];
        for (const config of extractorConfigs) {
          let cursor: string | undefined;
          const seenTimes: number[] = [];
          const startMessages: Array<ExtPipeRunSummary & { createdTime: number }> = [];
          const stopMessages: Array<ExtPipeRunSummary & { createdTime: number }> = [];
          const otherEvents: Array<ExtPipeRunSummary & { createdTime: number }> = [];
          do {
            const response = await sdk.post<{
              items?: Array<ExtPipeRunSummary & { createdTime?: unknown }>;
              nextCursor?: string | null;
            }>(`/api/v1/projects/${sdk.project}/extpipes/runs/list`, {
              data: {
                limit: 100,
                cursor,
                filter: {
                  externalId: config.externalId,
                  createdTime: {
                    min: windowRange.start,
                    max: windowRange.end,
                  },
                },
              },
            });
            for (const item of response.data?.items ?? []) {
              const createdTime = toTimestampLoose(item.createdTime);
              if (createdTime == null) continue;
              const status = item.status?.toLowerCase?.() ?? item.status ?? "";
              const message = (item.message ?? "").toLowerCase();
              if (status === "seen") {
                seenTimes.push(createdTime);
                continue;
              }
              if (status === "success" || status === "failure") {
                if (message.includes("extractor started")) {
                  startMessages.push({ ...item, createdTime });
                  continue;
                }
                if (message.includes("successful shutdown")) {
                  stopMessages.push({ ...item, createdTime });
                  continue;
                }
              }
              otherEvents.push({ ...item, createdTime });
            }
            cursor = response.data?.nextCursor ?? undefined;
          } while (cursor);

          const sortedSeen = [...seenTimes].sort((a, b) => a - b);
          const sortedStarts = [...startMessages].sort((a, b) => a.createdTime - b.createdTime);
          const sortedStops = [...stopMessages].sort((a, b) => a.createdTime - b.createdTime);
          const startCandidates = [
            ...(sortedSeen.length > 0 ? [sortedSeen[0]] : []),
            ...(sortedStarts.length > 0 ? [sortedStarts[0].createdTime] : []),
          ].sort((a, b) => a - b);
          const startTime = startCandidates[0];
          const lastSeen = sortedSeen.length > 0 ? sortedSeen[sortedSeen.length - 1] : undefined;
          const stopEvent = startTime
            ? sortedStops.find((event) => event.createdTime >= startTime)
            : undefined;

          if (startTime != null) {
            const endTime = stopEvent?.createdTime ?? lastSeen ?? startTime;
            runs.push({
              id: stopEvent?.id ?? Math.floor(startTime),
              status: stopEvent?.status ?? "seen",
              message:
                stopEvent?.message ??
                (sortedSeen.length > 0
                  ? t("processing.extractor.seenEvents", { count: sortedSeen.length })
                  : t("processing.extractor.started")),
              createdTime: startTime,
              endTime,
              externalId: config.externalId,
            });
          }

          for (const event of otherEvents) {
            runs.push({
              ...event,
              createdTime: event.createdTime,
              externalId: config.externalId,
            });
          }
        }

        if (!cancelled) {
          setExtractorRunsAll(runs);
          setExtractorsStatus("success");
        }
      } catch (error) {
        if (!cancelled) {
          setExtractorsError(
            error instanceof Error ? error.message : t("processing.error.extractors")
          );
          setExtractorsStatus("error");
        }
      }
    };

    loadExtractorRuns();
    return () => {
      cancelled = true;
    };
  }, [isSdkLoading, sdk, extractorConfigs, windowRange, t]);

  const extractorConfigMap = useMemo(() => {
    return extractorConfigs.reduce<Record<string, ExtPipeConfigSummary>>((acc, config) => {
      acc[config.externalId] = config;
      return acc;
    }, {});
  }, [extractorConfigs]);

  const filteredExtractorRuns = useMemo(() => {
    if (!windowRange) return [];
    const startWindow = windowRange.start;
    const endWindow = windowRange.end;
    return extractorRunsAll.filter((run) => {
      const start = run.createdTime;
      const end = run.endTime ?? run.createdTime;
      return start <= endWindow && end >= startWindow;
    });
  }, [extractorRunsAll, windowRange]);

  const getExtractorRadius = (run: ExtPipeRunSummary) => {
    const scaled = 6 + Math.sqrt(run.id % 100) * 0.8;
    return Math.min(16, Math.max(4, scaled));
  };

  const getExtractorColor = (run: ExtPipeRunSummary) => {
    const status = normalizeStatus(run.status);
    if (status === "seen") return "#06b6d4";
    if (status.includes("completed") || status.includes("success")) return "#16a34a";
    if (status.includes("failed") || status.includes("error")) return "#f97316";
    if (status.includes("running")) return "#2563eb";
    return "#a855f7";
  };

  return {
    extractorsStatus,
    extractorsError,
    extractorConfigs,
    extractorRunsAll,
    extractorConfigMap,
    filteredExtractorRuns,
    getExtractorRadius,
    getExtractorColor,
  };
}
