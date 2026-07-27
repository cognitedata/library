import type { ReactNode } from "react";
import { ApiError } from "@/shared/ApiError";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { LoadProgressCard } from "@/shared/LoadProgressCard";
import { INFIELD_LOCATION_CONFIG_VIEW } from "./useInfieldDataLocations";
import type { InfieldDataLocationOption, InfieldLoadProgress, LoadState } from "./types";

type InfieldDataLocationPickerProps = {
  title: string;
  description: ReactNode;
  locationSelectId: string;
  isSdkLoading: boolean;
  locationOptions: InfieldDataLocationOption[];
  locationsStatus: LoadState;
  locationsError: string | null;
  locationsProgress: InfieldLoadProgress | null;
  selectedKey: string;
  onSelectedKeyChange: (key: string) => void;
  selectedOption: InfieldDataLocationOption | null;
  contextNote?: ReactNode;
  emptyStateMessage?: ReactNode;
  errorRequestBody?: unknown;
  instanceSpaceLabel?: string;
};

export function InfieldDataLocationPicker({
  title,
  description,
  locationSelectId,
  isSdkLoading,
  locationOptions,
  locationsStatus,
  locationsError,
  locationsProgress,
  selectedKey,
  onSelectedKeyChange,
  selectedOption,
  contextNote,
  emptyStateMessage,
  errorRequestBody,
  instanceSpaceLabel = "appInstanceSpace",
}: InfieldDataLocationPickerProps) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">{title}</CardTitle>
        <CardDescription>{description}</CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        {locationsStatus === "loading" || isSdkLoading ? (
          locationsProgress ? (
            <LoadProgressCard progress={locationsProgress} />
          ) : (
            <p className="text-sm text-slate-500">Loading Infield location configs…</p>
          )
        ) : null}

        {locationsStatus === "error" ? (
          <ApiError
            message={locationsError}
            api="POST /models/instances/list"
            requestBody={
              errorRequestBody ?? {
                instanceType: "node",
                source: INFIELD_LOCATION_CONFIG_VIEW,
              }
            }
          />
        ) : null}

        {locationsStatus === "success" && locationOptions.length === 0 ? (
          <p className="text-sm text-slate-500">
            {emptyStateMessage ?? (
              <>
                No locations with <code className="text-xs">{instanceSpaceLabel}</code> found in{" "}
                <code className="text-xs">
                  {INFIELD_LOCATION_CONFIG_VIEW.space}/{INFIELD_LOCATION_CONFIG_VIEW.externalId}
                </code>
                .
              </>
            )}
          </p>
        ) : null}

        {locationOptions.length > 0 ? (
          <div className="max-w-xl">
            <label className="mb-1 block text-xs font-medium text-slate-600" htmlFor={locationSelectId}>
              Location
            </label>
            <select
              id={locationSelectId}
              className="w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700"
              value={selectedKey}
              onChange={(event) => onSelectedKeyChange(event.target.value)}
            >
              {locationOptions.map((option) => (
                <option key={option.locationExternalId} value={option.locationExternalId}>
                  {option.locationName} · {option.appInstanceSpace}
                </option>
              ))}
            </select>
          </div>
        ) : null}

        {selectedOption !== null && contextNote !== undefined ? (
          <div className="space-y-2 text-sm text-slate-600">{contextNote}</div>
        ) : null}
      </CardContent>
    </Card>
  );
}
