import { CogniteClient } from "@cognite/sdk";
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { AppSdkContext } from "../sdk-context";

type SdkManagerContextValue = {
  getSdk: (project: string) => CogniteClient;
  sdk: CogniteClient;
  project: string;
  isLoading: boolean;
  projectResolved: boolean;
  availableProjects: string[];
  setSelectedProject: (project: string) => void;
};

const SdkManagerContext = createContext<SdkManagerContextValue | null>(null);

function createSdkForProject(
  project: string,
  baseUrl: string,
  tokenProvider: () => Promise<string>,
  appId: string
): CogniteClient {
  return new CogniteClient({
    appId,
    project,
    baseUrl,
    oidcTokenProvider: tokenProvider,
  });
}

export function SdkManagerProvider({
  children,
  baseSdk,
  isLoading: baseLoading,
}: {
  children: React.ReactNode;
  baseSdk: CogniteClient;
  isLoading: boolean;
}) {
  const [availableProjects, setAvailableProjects] = useState<string[]>([]);
  const [selectedProject, setSelectedProject] = useState<string>(() => baseSdk.project || "");
  const [projectResolved, setProjectResolved] = useState(false);
  const [credentials, setCredentials] = useState<{
    token: string;
    baseUrl: string;
  } | null>(null);
  const cacheRef = useRef<Map<string, CogniteClient>>(new Map());

  const isStandalone = import.meta.env.VITE_STANDALONE === "true";
  const proxyUrl =
    (import.meta.env.CDF_PROXY_URL as string | undefined) ??
    (import.meta.env.VITE_CDF_PROXY_URL as string | undefined);

  useEffect(() => {
    if (baseLoading) return;
    let cancelled = false;
    const loadProjects = async () => {
      try {
        if (!baseSdk?.project) {
          if (!cancelled) setProjectResolved(true);
          return;
        }
        const response = await baseSdk.get<{
          projects?: Array<{ projectUrlName?: string }>;
        }>("/api/v1/token/inspect");
        const projectIds = (response.data?.projects ?? [])
          .map((p) => p.projectUrlName)
          .filter((value): value is string => Boolean(value));
        const unique = Array.from(new Set(projectIds)).sort((a, b) => a.localeCompare(b));
        if (!cancelled) {
          setAvailableProjects(unique);
          const stored = window.localStorage.getItem("qualitizer.lastProject");
          const next =
            stored && unique.includes(stored)
              ? stored
              : unique.includes(baseSdk.project)
                ? baseSdk.project
                : unique[0] ?? baseSdk.project;
          setSelectedProject(next);
          if (next) window.localStorage.setItem("qualitizer.lastProject", next);
          setProjectResolved(true);
        }
      } catch {
        if (!cancelled) {
          setAvailableProjects([]);
          setSelectedProject(baseSdk.project || "");
          setProjectResolved(true);
        }
      }
    };
    loadProjects();
    return () => {
      cancelled = true;
    };
  }, [baseLoading, baseSdk]);

  useEffect(() => {
    if (isStandalone) return;
    const handleMessage = (event: MessageEvent) => {
      if (event.data?.type === "PROVIDE_CREDENTIALS" && event.data?.credentials) {
        const creds = event.data.credentials;
        if (creds.token && creds.baseUrl) {
          setCredentials({ token: creds.token, baseUrl: creds.baseUrl });
        }
      }
    };
    window.addEventListener("message", handleMessage);
    if (window.parent && window.parent !== window) {
      window.parent.postMessage({ type: "REQUEST_CREDENTIALS" }, "*");
    }
    return () => window.removeEventListener("message", handleMessage);
  }, [isStandalone]);

  const getSdk = useCallback(
    (project: string): CogniteClient => {
      if (!project) return baseSdk;

      const cached = cacheRef.current.get(project);
      if (cached) return cached;

      let client: CogniteClient;

      if (isStandalone && proxyUrl) {
        client = createSdkForProject(
          project,
          proxyUrl,
          async () => "proxy",
          (import.meta.env.VITE_CDF_APP_ID as string) ?? "qualitizer-standalone"
        );
      } else if (credentials) {
        const token = credentials.token;
        client = createSdkForProject(
          project,
          credentials.baseUrl,
          async () => token,
          "qualitizer"
        );
      } else {
        return baseSdk;
      }

      cacheRef.current.set(project, client);
      return client;
    },
    [baseSdk, credentials, isStandalone, proxyUrl]
  );

  const sdk = useMemo(
    () => getSdk(selectedProject || baseSdk.project),
    [getSdk, selectedProject, baseSdk.project]
  );

  const value = useMemo<SdkManagerContextValue>(
    () => ({
      getSdk,
      sdk,
      project: selectedProject || baseSdk.project,
      isLoading: baseLoading,
      projectResolved,
      availableProjects,
      setSelectedProject: (next: string) => {
        if (next) {
          setSelectedProject(next);
          window.localStorage.setItem("qualitizer.lastProject", next);
        }
      },
    }),
    [getSdk, sdk, selectedProject, baseSdk.project, baseLoading, projectResolved, availableProjects]
  );

  return (
    <SdkManagerContext.Provider value={value}>
      <AppSdkContext.Provider value={{ sdk, isLoading: baseLoading }}>
        {children}
      </AppSdkContext.Provider>
    </SdkManagerContext.Provider>
  );
}

export function useSdkManager() {
  const context = useContext(SdkManagerContext);
  if (!context) {
    throw new Error("useSdkManager must be used within SdkManagerProvider");
  }
  return context;
}
