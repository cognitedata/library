# Classic Analysis – Cognite Dune Application

This app is a Cognite Dune Application for **classic CDF model analysis**. It analyzes metadata field distribution across **assets**, **time series**, **events**, and **sequences** (with count and sort by count descending). Events support a special **metadata** filter key that groups by metadata field names.

**Target CDF:** org `cog-demo`, project `lervik-industries`, cluster `api.cognite.com`.

## Get Dune going

You can scaffold or update the Dune app with:

```bash
npx @cognite/dune create .
```

When prompted, use the app name **cdf-classic-analysis** (or leave the default). If the CLI asks for more options, accept defaults. If you prefer not to run the interactive create, the app is already set up so you can install and run it directly.

## Install and run locally

You do **not** need to build the app for local development. Use the dev server:

```bash
pnpm install
pnpm run dev
```

Open the URL shown in the terminal (e.g. **http://localhost:5173**) in your browser. Do not open `dist/index.html` directly—the app must be served from the dev server (or a real server) to work. For local auth, configure a `.env` (or environment) with:

- `PROJECT` – CDF project (e.g. `lervik-industries`)
- `BASE_URL` – CDF base URL (e.g. `https://api.cognite.com`)
- `CLIENT_ID` – OAuth client ID
- `CLIENT_SECRET` – OAuth client secret

(See [cog.link/dune](https://cog.link/dune) and `@cognite/dune-fe-auth` for details.)

## Build and deploy

```bash
pnpm run build
npx @cognite/dune deploy
```

(Deploy steps may vary; see [cog.link/dune](https://cog.link/dune) for the current Dune workflow.)

## "Failed to fetch" when using a different CDF project

If you open this app in **another CDF project** (not the one it was first deployed to) and get **"Failed to fetch"** with no results, the app is likely not deployed for that project. The browser blocks the request (often CORS) or the project does not allow the app’s origin.

**Fix:**

1. **Add a deployment** for the target project in `app.json` (or a copy like `app-<project>.json`). Each entry in `deployments` needs:
   - `org` – your CDF org (e.g. `cog-demo`, `ignos-dev`)
   - `project` – the CDF project name
   - `baseUrl` – the cluster URL (e.g. `https://api.cognite.com`, `https://westeurope-1.cognitedata.com`, `https://greenfield.cognitedata.com`)
   - `deploySecretName` – the secret name for the deploy client (e.g. `org_project_cluster`)

2. **Deploy the app to that project:**
   ```bash
   pnpm run build
   npx @cognite/dune deploy
   ```
   If you use a project-specific file (e.g. `app-popdahl.json`), point the CLI at it when deploying (see Dune docs).

3. **Open the app from Fusion** in that project (e.g. from the project’s app launcher). The app must be launched in the same project you deployed to so that authentication and CORS are valid.

If the problem continues, check that the CDF project has the **aggregate** APIs enabled for assets/time series/events/sequences and that your user has the right capabilities.

**Deployed for this project but still "Failed to fetch"?** The CDF project must allow your Fusion app origin for API access (CORS). Ask your CDF admin to add the Fusion host origin (e.g. `https://cog-demo.fusion.cognite.com`) to the project’s allowed origins for API requests, or to confirm that Dune apps are allowed to call the CDF API from that origin.

## "No access" in Fusion

If Fusion shows **"No access"** even though you can log in to CDF, access is controlled by the Fusion/Dune host, not this app. Fix it on the CDF side:

1. **Your user must be in a group** that has:
   - **`files:read`** on the dataset where Dune/Streamlit apps are stored (needed to open the app in Fusion).
   - For development/publishing: **`files:write`** on that dataset.
2. **Basic Fusion capabilities** (usually already granted): `projects:list`, `groups:list`, `groups:read`.
3. Ask your CDF admin to add your group to the correct capabilities, or to add you to a group that already has access to the Dune apps dataset.  
   See [Cognite Docs – Capabilities](https://docs.cognite.com/cdf/access/guides/capabilities) and the Streamlit/Dune apps section.

## Functionality

- **Resource type**: Assets, Time series, Events, Sequences, Files.
- **Filter key**: Metadata key to group by (e.g. `type`, `category`, `subtype`). For Events, use `metadata` to group by metadata **field names** (not values).
- **Run analysis**: Calls CDF aggregate APIs (`uniqueValues` + `uniqueProperties`), sorts by count descending, shows results with counts.
- **Download .txt**: Saves the analysis output as a text file.
