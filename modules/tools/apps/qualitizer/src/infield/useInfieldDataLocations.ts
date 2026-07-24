import { useCallback, useEffect, useMemo, useState } from "react";
import { useAppSdk } from "@/shared/auth";
import { loadNavState, saveNavState } from "@/shared/nav-persistence";
import {
  fetchAllLocationConfigs,
  fetchLegacyConfigData,
  getAllLegacyLocations,
  getInfield2DataLocationOptions,
  getInfieldDataLocationOptions,
  INFIELD_LOCATION_CONFIG_VIEW,
} from "./fetchers";
import type { InfieldDataLocationOption, InfieldLoadProgress, LoadState } from "./types";

export type InfieldDataLocationsVariant = "infield2" | "infieldCdm";

function readPersistedLocationKey(variant: InfieldDataLocationsVariant): string {
  const state = loadNavState();
  return variant === "infield2"
    ? state.infield2DataLocationKey ?? ""
    : state.infieldCdmDataLocationKey ?? "";
}

function resolveLocationKey(
  options: InfieldDataLocationOption[],
  variant: InfieldDataLocationsVariant,
  currentKey: string
): string {
  const persisted = readPersistedLocationKey(variant);
  const candidate = currentKey.length > 0 ? currentKey : persisted;
  if (candidate.length > 0 && options.some((option) => option.locationExternalId === candidate)) {
    return candidate;
  }
  return options[0]?.locationExternalId ?? "";
}

export function useInfieldDataLocations(variant: InfieldDataLocationsVariant = "infieldCdm") {
  const { sdk, isLoading: isSdkLoading } = useAppSdk();

  const [locationOptions, setLocationOptions] = useState<InfieldDataLocationOption[]>([]);
  const [locationsStatus, setLocationsStatus] = useState<LoadState>("idle");
  const [locationsError, setLocationsError] = useState<string | null>(null);
  const [locationsProgress, setLocationsProgress] = useState<InfieldLoadProgress | null>(null);
  const [selectedKey, setSelectedKeyState] = useState(() => readPersistedLocationKey(variant));

  const setSelectedKey = useCallback(
    (key: string) => {
      setSelectedKeyState(key);
      if (key.length > 0) {
        saveNavState(
          variant === "infield2" ? { infield2DataLocationKey: key } : { infieldCdmDataLocationKey: key }
        );
      }
    },
    [variant]
  );

  const applyResolvedLocationKey = useCallback(
    (options: InfieldDataLocationOption[]) => {
      setSelectedKeyState((current) => {
        const next = resolveLocationKey(options, variant, current);
        if (next.length > 0) {
          saveNavState(
            variant === "infield2" ? { infield2DataLocationKey: next } : { infieldCdmDataLocationKey: next }
          );
        }
        return next;
      });
    },
    [variant]
  );

  useEffect(() => {
    if (isSdkLoading) return;

    let cancelled = false;
    const load = async () => {
      setLocationsStatus("loading");
      setLocationsError(null);

      try {
        if (variant === "infield2") {
          setLocationsProgress({
            phase: "Loading location configs",
            current: 0,
            total: 2,
            detail: "Loading legacy APM config (APP_CONFIG_V2)",
          });
          const [legacyConfigData, infieldLocations] = await Promise.all([
            fetchLegacyConfigData(sdk),
            fetchAllLocationConfigs(sdk),
          ]);
          if (cancelled) return;

          setLocationsProgress({
            phase: "Loading location configs",
            current: 1,
            total: 2,
            detail: "Matching Infield CDM location configs",
          });
          const options = getInfield2DataLocationOptions(
            getAllLegacyLocations(legacyConfigData),
            infieldLocations
          );
          setLocationOptions(options);
          setLocationsStatus("success");
          setLocationsProgress(null);
          applyResolvedLocationKey(options);
          return;
        }

        setLocationsProgress({
          phase: "Loading location configs",
          current: 0,
          total: 2,
          detail: "Listing Infield CDM location config nodes",
        });
        const locations = await fetchAllLocationConfigs(sdk);
        if (cancelled) return;

        setLocationsProgress({
          phase: "Loading location configs",
          current: 1,
          total: 2,
          detail: "Loading legacy APM config for APMA instance spaces",
        });
        const legacyConfigData = await fetchLegacyConfigData(sdk);
        const legacyLocations = getAllLegacyLocations(legacyConfigData);
        const options = getInfieldDataLocationOptions(locations, legacyLocations);
        if (!cancelled) {
          setLocationOptions(options);
          setLocationsStatus("success");
          setLocationsProgress(null);
          applyResolvedLocationKey(options);
        }
      } catch (error) {
        if (!cancelled) {
          setLocationsError(error instanceof Error ? error.message : "Failed to load location configs.");
          setLocationsStatus("error");
          setLocationsProgress(null);
        }
      }
    };

    load();
    return () => {
      cancelled = true;
    };
  }, [sdk, isSdkLoading, variant, applyResolvedLocationKey]);

  const selectedOption = useMemo(
    () => locationOptions.find((option) => option.locationExternalId === selectedKey) ?? null,
    [locationOptions, selectedKey]
  );

  return {
    isSdkLoading,
    locationOptions,
    locationsStatus,
    locationsError,
    locationsProgress,
    selectedKey,
    setSelectedKey,
    selectedOption,
  };
}

export { INFIELD_LOCATION_CONFIG_VIEW };
