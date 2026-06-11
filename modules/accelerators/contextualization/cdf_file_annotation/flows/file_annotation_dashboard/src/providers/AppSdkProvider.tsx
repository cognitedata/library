import { CogniteClient, type CogniteClient as CogniteClientType } from "@cognite/sdk";
import { connectToHostApp } from "@cognite/app-sdk";
import React, { createContext, useContext, useEffect, useMemo, useState } from "react";
import { isLocalCdfMode, displayProjectName } from "@/runtime/authMode";

interface AppSdkContextValue {
  sdk: CogniteClientType | null;
  isLoading: boolean;
  isLocalMode: boolean;
  projectName: string;
}

const AppSdkContext = createContext<AppSdkContextValue>({
  sdk: null,
  isLoading: false,
  isLocalMode: true,
  projectName: displayProjectName,
});

export function FlowsSdkProvider({ children }: { children: React.ReactNode }) {
  const [sdk, setSdk] = useState<CogniteClientType | null>(null);
  const [projectName, setProjectName] = useState("unknown");
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;

    const initializeSdk = async () => {
      try {
        const { api } = await connectToHostApp({ applicationName: "file-annotation-app" });
        const [project, baseUrl] = await Promise.all([api.getProject(), api.getBaseUrl()]);

        if (cancelled) {
          return;
        }

        const client = new CogniteClient({
          project,
          baseUrl,
          appId: "file-annotation-dashboard",
          oidcTokenProvider: () => api.getAccessToken(),
        });

        setProjectName(project);
        setSdk(client);
      } catch (error) {
        console.error("Failed to initialize Flows SDK:", error);
        if (!cancelled) {
          setProjectName("unknown");
          setSdk(null);
        }
      } finally {
        if (!cancelled) {
          setIsLoading(false);
        }
      }
    };

    void initializeSdk();

    return () => {
      cancelled = true;
    };
  }, []);

  const value = useMemo(
    () => ({
      sdk,
      isLoading,
      isLocalMode: false,
      projectName,
    }),
    [sdk, isLoading, projectName]
  );

  return <AppSdkContext.Provider value={value}>{children}</AppSdkContext.Provider>;
}

export function LocalMockSdkProvider({ children }: { children: React.ReactNode }) {
  const value = useMemo(
    () => ({
      sdk: null,
      isLoading: false,
      isLocalMode: true,
      projectName: displayProjectName,
    }),
    []
  );

  return <AppSdkContext.Provider value={value}>{children}</AppSdkContext.Provider>;
}

type CachedToken = { token: string; expiresAt: number } | null;
let cachedToken: CachedToken = null;

async function fetchClientCredentialsToken(): Promise<string> {
  const proxyUrl = import.meta.env.VITE_TOKEN_PROXY_URL;
  if (!isLocalCdfMode) {
    throw new Error("Token fetch attempted outside local CDF mode.");
  }

  if (!proxyUrl) {
    throw new Error("Missing VITE_TOKEN_PROXY_URL for local CDF mode.");
  }

  if (cachedToken && Date.now() < cachedToken.expiresAt - 60_000) {
    return cachedToken.token;
  }

  const response = await fetch(proxyUrl, {
    method: "POST",
  });

  if (!response.ok) {
    const message = await response.text();
    console.error("Token proxy failed", {
      status: response.status,
      body: message,
    });
    throw new Error(`Token proxy failed: ${response.status} ${message}`);
  }

  const payload = (await response.json()) as {
    access_token: string;
    expires_in?: number;
  };

  const expiresIn = payload.expires_in ?? 3600;
  cachedToken = {
    token: payload.access_token,
    expiresAt: Date.now() + expiresIn * 1000,
  };

  return payload.access_token;
}

function createLocalCdfClient() {
  const project = import.meta.env.VITE_CDF_PROJECT;
  const baseUrl =
    import.meta.env.VITE_CDF_URL ||
    (import.meta.env.VITE_CDF_CLUSTER
      ? `https://${import.meta.env.VITE_CDF_CLUSTER}.cognitedata.com`
      : "");

  if (!project || !baseUrl) {
    throw new Error("Missing VITE_CDF_PROJECT or VITE_CDF_URL for local CDF mode.");
  }

  return new CogniteClient({
    project,
    baseUrl,
    appId: "file-annotation-dashboard-local",
    oidcTokenProvider: fetchClientCredentialsToken,
  });
}

export function LocalCdfSdkProvider({ children }: { children: React.ReactNode }) {
  const [sdk, setSdk] = useState<CogniteClientType | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    if (!isLocalCdfMode) return;
    setIsLoading(true);
    try {
      const client = createLocalCdfClient();
      setSdk(client);
    } catch (error) {
      console.error("Failed to initialize local CDF client:", error);
      setSdk(null);
    } finally {
      setIsLoading(false);
    }
  }, []);

  const value = useMemo(
    () => ({
      sdk,
      isLoading,
      isLocalMode: true,
      projectName: sdk?.project || displayProjectName,
    }),
    [sdk, isLoading]
  );

  return <AppSdkContext.Provider value={value}>{children}</AppSdkContext.Provider>;
}

export function useAppSdk() {
  return useContext(AppSdkContext);
}
