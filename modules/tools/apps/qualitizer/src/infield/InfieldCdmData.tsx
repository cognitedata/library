import { useEffect, useState } from "react";
import { useAppSdk } from "@/shared/auth";
import { ApiError } from "@/shared/ApiError";
import { InfieldDataLocationPicker } from "./InfieldDataLocationPicker";
import { InfieldDataNodeDialog } from "./InfieldDataNodeDialog";
import { InfieldDataQualityTable } from "./InfieldDataQualityTable";
import { LoadProgressCard } from "@/shared/LoadProgressCard";
import {
  buildInfieldCdmDataQualityReport,
  buildInfieldViewMappingQualityReport,
  buildViewMappingSampleTasks,
  getInfieldCdmViewSources,
  INFIELD_CDM_DATA_MODEL,
  INFIELD_CDM_SCHEMA_SPACE,
  INFIELD_CDM_VIEWS,
  LEGACY_PREVIEW_ROW_LIMIT,
  LEGACY_VIEW_SAMPLE_CAP,
} from "./fetchers";
import { useInfieldDataLocations } from "./useInfieldDataLocations";
import type {
  InfieldLoadProgress,
  LegacyDataQualityReport,
  LoadState,
  SampledInstanceRow,
} from "./types";

export function InfieldCdmData() {
  const { sdk } = useAppSdk();
  const {
    isSdkLoading,
    locationOptions,
    locationsStatus,
    locationsError,
    locationsProgress,
    selectedKey,
    setSelectedKey,
    selectedOption,
  } = useInfieldDataLocations("infieldCdm");

  const [mappingReport, setMappingReport] = useState<LegacyDataQualityReport | null>(null);
  const [mappingReportStatus, setMappingReportStatus] = useState<LoadState>("idle");
  const [cdmReport, setCdmReport] = useState<LegacyDataQualityReport | null>(null);
  const [cdmReportStatus, setCdmReportStatus] = useState<LoadState>("idle");
  const [reportError, setReportError] = useState<string | null>(null);
  const [reportProgress, setReportProgress] = useState<InfieldLoadProgress | null>(null);
  const [selectedNode, setSelectedNode] = useState<SampledInstanceRow | null>(null);

  const mappingTaskCount =
    selectedOption?.location !== undefined
      ? buildViewMappingSampleTasks(selectedOption.location).length
      : 0;

  useEffect(() => {
    if (selectedOption === null || selectedOption.location === undefined) {
      setMappingReport(null);
      setMappingReportStatus("idle");
      setCdmReport(null);
      setCdmReportStatus("idle");
      setReportProgress(null);
      return;
    }

    const { location, appInstanceSpace: instanceSpace } = selectedOption;
    let cancelled = false;

    const load = async () => {
      setReportError(null);
      setMappingReportStatus("loading");
      setCdmReportStatus("idle");
      setMappingReport({ instanceSpaces: [], results: [] });
      setCdmReport(null);
      setReportProgress({
        phase: "Sampling view mappings",
        current: 0,
        total: mappingTaskCount,
        detail: location.externalId,
      });

      try {
        const viewMappingReport = await buildInfieldViewMappingQualityReport(sdk, location, {
          onProgress: (progress) => {
            if (cancelled) return;
            setReportProgress({
              phase: "Sampling view mappings",
              current: progress.current,
              total: progress.total,
              detail: `${progress.viewKey} · ${progress.instanceSpace}`,
            });
          },
          onResult: (result) => {
            if (cancelled) return;
            setMappingReport((current) => ({
              instanceSpaces: [...new Set([...(current?.instanceSpaces ?? []), result.instanceSpace])],
              results: [...(current?.results ?? []), result],
            }));
          },
        });

        if (cancelled) return;

        setMappingReport(viewMappingReport);
        setMappingReportStatus("success");
        setCdmReportStatus("loading");
        setCdmReport({ instanceSpaces: [instanceSpace], results: [] });
        setReportProgress({
          phase: "Sampling Infield CDM views",
          current: 0,
          total: INFIELD_CDM_VIEWS.length,
          detail: instanceSpace,
        });

        const qualityReport = await buildInfieldCdmDataQualityReport(sdk, [instanceSpace], {
          onProgress: (progress) => {
            if (cancelled) return;
            setReportProgress({
              phase: "Sampling Infield CDM views",
              current: progress.current,
              total: progress.total,
              detail: `${progress.viewKey} · ${progress.instanceSpace}`,
            });
          },
          onResult: (result) => {
            if (cancelled) return;
            setCdmReport((current) => ({
              instanceSpaces: [instanceSpace],
              results: [...(current?.results ?? []), result],
            }));
          },
        });

        if (!cancelled) {
          setCdmReport(qualityReport);
          setCdmReportStatus("success");
          setReportProgress(null);
        }
      } catch (error) {
        if (!cancelled) {
          setReportError(error instanceof Error ? error.message : "Failed to sample Infield CDM data.");
          setMappingReportStatus((status) => (status === "loading" ? "error" : status));
          setCdmReportStatus((status) => (status === "loading" ? "error" : status));
          setReportProgress(null);
        }
      }
    };

    load();
    return () => {
      cancelled = true;
    };
  }, [sdk, selectedOption, mappingTaskCount]);

  const contextNote =
    selectedOption === null ? null : (
      <p>
        View mappings sample configured and default <code className="text-xs">cdf_cdm</code> /{" "}
        <code className="text-xs">cdf_idm</code> views in each mapping&apos;s data filter space. InField on CDM checks
        run in <code className="text-xs">{selectedOption.appInstanceSpace}</code> across{" "}
        <code className="text-xs">{INFIELD_CDM_VIEWS.length}</code> views in{" "}
        <code className="text-xs">{INFIELD_CDM_SCHEMA_SPACE}</code> (
        <code className="text-xs">{INFIELD_CDM_DATA_MODEL.externalId}</code>).
      </p>
    );

  return (
    <div className="flex flex-col gap-4">
      <InfieldDataLocationPicker
        title="Infield CDM Data Explorer"
        description={
          <>
            Sample node counts per location <code className="text-xs">viewMappings</code> (configured and default
            views) and per <code className="text-xs">cdf_infield</code> view from the{" "}
            <code className="text-xs">InFieldOnCDM</code> data model in{" "}
            <code className="text-xs">dataStorage.appInstanceSpace</code>. Counts are capped at{" "}
            {LEGACY_VIEW_SAMPLE_CAP} per view.
          </>
        }
        locationSelectId="infield-cdm-data-location"
        isSdkLoading={isSdkLoading}
        locationOptions={locationOptions}
        locationsStatus={locationsStatus}
        locationsError={locationsError}
        locationsProgress={locationsProgress}
        selectedKey={selectedKey}
        onSelectedKeyChange={setSelectedKey}
        selectedOption={selectedOption}
        contextNote={contextNote}
      />

      {reportProgress !== null ? <LoadProgressCard progress={reportProgress} /> : null}

      {(mappingReportStatus === "error" || cdmReportStatus === "error") && selectedOption !== null ? (
        <ApiError
          message={reportError}
          api="POST /models/instances/list"
          requestBody={{
            dataModel: INFIELD_CDM_DATA_MODEL,
            schemaSpace: INFIELD_CDM_SCHEMA_SPACE,
            instanceSpace: selectedOption.appInstanceSpace,
            views: getInfieldCdmViewSources(),
          }}
        />
      ) : null}

      {(mappingReportStatus === "loading" || mappingReportStatus === "success") &&
      mappingReport !== null &&
      mappingReport.results.length > 0 ? (
        <InfieldDataQualityTable
          variant="viewMappings"
          title="View mapping data quality"
          description={
            <>
              Sampled node counts per configured <code className="text-xs">viewMappings</code> entry and default{" "}
              <code className="text-xs">cdf_cdm</code> / <code className="text-xs">cdf_idm</code> view where
              customized (capped at {LEGACY_VIEW_SAMPLE_CAP}, showing up to {LEGACY_PREVIEW_ROW_LIMIT} preview rows per
              view).
            </>
          }
          report={mappingReport}
          status={mappingReportStatus === "loading" ? "loading" : "success"}
          totalViews={mappingTaskCount}
          sampleCap={LEGACY_VIEW_SAMPLE_CAP}
          previewLimit={LEGACY_PREVIEW_ROW_LIMIT}
          onPreviewClick={setSelectedNode}
        />
      ) : null}

      {(cdmReportStatus === "loading" || cdmReportStatus === "success") &&
      cdmReport !== null &&
      cdmReport.results.length > 0 ? (
        <InfieldDataQualityTable
          title="Infield CDM data quality"
          description={
            <>
              Sampled node counts per <code className="text-xs">{INFIELD_CDM_SCHEMA_SPACE}</code> view in{" "}
              <code className="text-xs">{selectedOption?.appInstanceSpace}</code> (capped at {LEGACY_VIEW_SAMPLE_CAP},
              showing up to {LEGACY_PREVIEW_ROW_LIMIT} preview rows per view).
            </>
          }
          report={cdmReport}
          status={cdmReportStatus === "loading" ? "loading" : "success"}
          totalViews={INFIELD_CDM_VIEWS.length}
          sampleCap={LEGACY_VIEW_SAMPLE_CAP}
          previewLimit={LEGACY_PREVIEW_ROW_LIMIT}
          onPreviewClick={setSelectedNode}
        />
      ) : null}

      {selectedNode !== null ? (
        <InfieldDataNodeDialog node={selectedNode} onClose={() => setSelectedNode(null)} />
      ) : null}
    </div>
  );
}
