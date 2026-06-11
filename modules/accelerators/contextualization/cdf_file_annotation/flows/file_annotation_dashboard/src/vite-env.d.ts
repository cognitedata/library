/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_RUNTIME_MODE?: "cdf_local" | "cdf_host" | "mock";
  readonly VITE_PROJECT_LABEL?: string;
  readonly VITE_TOKEN_PROXY_URL?: string;
  readonly VITE_CDF_PROJECT?: string;
  readonly VITE_CDF_URL?: string;
  readonly VITE_CDF_CLUSTER?: string;
  readonly VITE_PROXY_TARGET?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
