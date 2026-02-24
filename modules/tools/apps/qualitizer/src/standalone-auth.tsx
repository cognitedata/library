/// <reference types="vite/client" />
import { type ReactNode, useEffect, useState } from "react";
import { CogniteClient } from "@cognite/sdk";

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { ApiError } from "@/shared/ApiError";
import { AppSdkContext } from "./sdk-context";

type StandaloneAuthProps = {
  children: ReactNode;
};

type AuthState = "checking" | "authenticated" | "error";

const REQUIRED_ENV = ["CDF_PROJECT", "CDF_PROXY_URL"];

export function StandaloneAuthProvider({ children }: StandaloneAuthProps) {
  const [sdk, setSdk] = useState<CogniteClient | null>(null);
  const [state, setState] = useState<AuthState>("checking");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  const project =
    (import.meta.env.CDF_PROJECT as string | undefined) ??
    (import.meta.env.VITE_CDF_PROJECT as string | undefined);
  const baseUrl =
    (import.meta.env.CDF_URL as string | undefined) ??
    (import.meta.env.VITE_CDF_BASE_URL as string | undefined) ??
    "https://api.cognitedata.com";
  const proxyUrl =
    (import.meta.env.CDF_PROXY_URL as string | undefined) ??
    (import.meta.env.VITE_CDF_PROXY_URL as string | undefined);
  const appId = (import.meta.env.VITE_CDF_APP_ID as string | undefined) ?? "qualitizer-standalone";

  useEffect(() => {
    const missing = REQUIRED_ENV.filter((key) => !(import.meta.env as Record<string, string>)[key]);
    if (missing.length > 0) {
      setState("error");
      setErrorMessage(`Missing environment variables: ${missing.join(", ")}`);
      return;
    }

    if (!proxyUrl || !project) {
      setState("error");
      setErrorMessage("Missing proxy configuration.");
      return;
    }

    const client = new CogniteClient({
      appId,
      project,
      baseUrl: proxyUrl,
      oidcTokenProvider: async () => "proxy",
    });
    setSdk(client);
    setState("authenticated");
  }, [project, appId, proxyUrl]);

  if (state === "authenticated" && sdk) {
    return (
      <AppSdkContext.Provider value={{ sdk, isLoading: false }}>
        {children}
      </AppSdkContext.Provider>
    );
  }

  if (state === "error") {
    return (
      <div className="min-h-screen w-full px-6 py-10">
        <div className="mx-auto w-full max-w-2xl">
          <Card>
            <CardHeader>
              <CardTitle>Standalone Login</CardTitle>
              <CardDescription>Configure environment variables to continue.</CardDescription>
            </CardHeader>
            <CardContent className="space-y-3">
              <ApiError message={errorMessage ?? "Missing configuration."} />
              <div className="text-sm text-slate-600">
                Required variables: {REQUIRED_ENV.join(", ")}.
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen w-full px-6 py-10">
      <div className="mx-auto w-full max-w-2xl">
        <Card>
          <CardHeader>
            <CardTitle>Standalone Proxy</CardTitle>
            <CardDescription>Configure proxy access to use the app.</CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="text-sm text-slate-600">
              Project: {project ?? "unknown"} · Base URL: {baseUrl}
            </div>
            <div className="text-sm text-slate-600">
              Proxy URL: {proxyUrl ?? "missing"}
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

