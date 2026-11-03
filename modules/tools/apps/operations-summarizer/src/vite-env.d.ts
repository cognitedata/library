/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly PROJECT: string;
  readonly BASE_URL: string;
  readonly CLIENT_ID: string;
  readonly CLIENT_SECRET: string;
  readonly DEV_MODE?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
