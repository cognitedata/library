import { Loader } from "@cognite/aura/components";
import { useQuery } from "@tanstack/react-query";
import { AlertTriangle } from "lucide-react";
import { type ReactNode, useState } from "react";
import { Button } from "@/components/ui/button";
import { setAppConfig } from "@/config/appConfig";
import {
  CONFIG_INSTANCE_SPACE,
  CONFIG_NODE_PATH,
  CONFIG_SCHEMA_SPACE,
} from "@/config/configLocation";
import { useCdfClient } from "@/contexts/CdfClientContext";
import {
  type AppConfigService,
  createAppConfigServiceFromSdk,
} from "@/services/appConfigService";

export interface AppConfigProviderProps {
  children: ReactNode;
  /** Override the CDF loader in tests. */
  serviceFactory?: () => AppConfigService;
}

/**
 * Loads the config node once and blocks the rest of the app until it succeeds.
 * There is no bundled fallback.
 */
export function AppConfigProvider({
  children,
  serviceFactory,
}: AppConfigProviderProps) {
  const { sdk, isAuthLoading } = useCdfClient();
  const canLoad = serviceFactory !== undefined || sdk !== null;
  const [factory] = useState(() => serviceFactory);

  const query = useQuery({
    queryKey: ["ops-summarizer-app-config"],
    enabled: canLoad,
    staleTime: Number.POSITIVE_INFINITY,
    retry: false,
    queryFn: () => {
      const service = factory
        ? factory()
        : createAppConfigServiceFromSdk(requireSdk(sdk));
      return service.load();
    },
  });

  if (isAuthLoading || !canLoad || query.isPending) {
    return <ConfigLoading />;
  }

  if (query.isError || !query.data) {
    return (
      <ConfigLoadError
        message={errorMessage(query.error)}
        onRetry={() => {
          void query.refetch();
        }}
      />
    );
  }

  setAppConfig(query.data);
  return children;
}

function requireSdk(
  sdk: ReturnType<typeof useCdfClient>["sdk"],
): NonNullable<typeof sdk> {
  if (!sdk) {
    throw new Error("CDF client is not ready");
  }
  return sdk;
}

function errorMessage(error: unknown): string {
  if (error instanceof Error && error.message.trim() !== "") {
    return error.message;
  }
  return "The configuration node could not be read.";
}

function ConfigLoading() {
  return (
    <div
      role="status"
      className="flex h-screen items-center justify-center"
      aria-live="polite"
      aria-label="Loading configuration"
    >
      <Loader size={20} className="text-muted-foreground" />
    </div>
  );
}

function ConfigLoadError({
  message,
  onRetry,
}: {
  message: string;
  onRetry: () => void;
}) {
  return (
    <div
      role="alert"
      className="flex h-screen flex-col items-center justify-center gap-4 p-8 text-center"
    >
      <AlertTriangle className="h-12 w-12 text-destructive" />
      <div className="max-w-lg">
        <p className="text-lg font-semibold text-foreground">
          Configuration could not be loaded
        </p>
        <p className="mt-2 text-sm text-muted-foreground">
          Node{" "}
          <span className="font-medium text-foreground">
            {CONFIG_NODE_PATH}
          </span>
          . The app needs dataModels:read on{" "}
          <span className="font-medium text-foreground">
            {CONFIG_SCHEMA_SPACE}
          </span>{" "}
          and dataModelInstances:read on{" "}
          <span className="font-medium text-foreground">
            {CONFIG_INSTANCE_SPACE}
          </span>
          .
        </p>
        <p className="mt-2 text-sm text-muted-foreground">{message}</p>
        <Button type="button" className="mt-4" onClick={onRetry}>
          Try again
        </Button>
      </div>
    </div>
  );
}
