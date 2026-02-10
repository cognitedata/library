## Qualitizer

A set of tools to help you understand, troubleshoot and improve the quality of your CDF deployment.



This tool is built on top of the Cognite Dune application framework, currently in beta.
Once Dune is GA, this tool will be available within the CDF GUI itself. Currently, you can run it in standalone mode with a local proxy that injects client-credentials tokens, allowing technical people to run it on their own machine.


### Prerequisites
- Node.js 18+ (recommended)
- npm (or pnpm/yarn if you prefer)

### Install
```bash
npm install
```

### Run in standalone mode (with proxy)
Standalone mode uses a local proxy that injects client-credentials tokens.

#### 1) Start the proxy
```bash
npm run proxy
```

Proxy environment variables (required):
- `CDF_PROJECT` – CDF project URL name
- `IDP_TOKEN_URL` – OAuth token endpoint (client credentials)
- `IDP_CLIENT_ID`
- `IDP_CLIENT_SECRET`

Proxy environment variables:
- `IDP_SCOPES` – OAuth scopes (defaults to `${CDF_URL}/.default`)
- `CDF_URL` – CDF base URL (default `https://api.cognitedata.com`)
- `PORT` – proxy port (default `4243`)
- `PROXY_INSECURE` – set to `true` to disable TLS verification

#### 2) Start the UI in standalone mode
```bash
npm run standalone
```
The UI runs on `http://localhost:4242` (no TLS/cert required).
To include internal (non-production) pages in standalone mode:
```bash
npm run standalone-internal
```

UI environment variables (required):
- `CDF_PROJECT` – CDF project URL name
- `CDF_PROXY_URL` – proxy base URL (e.g. `http://localhost:4243`)

UI environment variables (optional):
- `VITE_CDF_APP_ID` – app id (default `qualitizer-standalone`)
- `CDF_URL` or `VITE_CDF_BASE_URL` – displayed in the UI (default `https://api.cognitedata.com`)

Note: the Vite config passes the following envs to the client:
- `CDF_PROJECT`, `CDF_URL`, `IDP_TENANT_ID`, `IDP_CLIENT_ID`, `IDP_SCOPES`, `CDF_PROXY_URL`


