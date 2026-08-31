# azure-cdf-bootstrap

One `terraform apply` instead of twenty clicks in portal.azure.com.

Give it an Entra ID (Azure AD) **tenant ID**; it creates:

1. an Entra **security group** — `<prefix>-admin`
2. an **app registration** — `<prefix>-app` (and its service principal, added to the group)
3. a **client secret**

…and writes everything to a `.env` (mode `0600`) ready for CDF service-principal auth.

## Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/install) >= 1.6
- Azure CLI, logged in: `az login --tenant <tenant-id>`
- Entra permissions to create groups and app registrations — e.g. **Application Developer** +
  **Groups Administrator**, or **Cloud Application Administrator**.

## Usage

```bash
cp terraform.tfvars.example terraform.tfvars   # fill in tenant_id, cdf_cluster, cdf_project
terraform init
terraform apply
```

Then look at `.env`:

```
IDP_TENANT_ID=...
IDP_CLIENT_ID=...
IDP_CLIENT_SECRET=...
IDP_TOKEN_URL=https://login.microsoftonline.com/<tenant>/oauth2/v2.0/token
IDP_SCOPES=https://<cluster>.cognitedata.com/.default
CDF_CLUSTER=...
CDF_PROJECT=...
CDF_URL=https://<cluster>.cognitedata.com
ENTRA_GROUP_ID=...
```

The variable names match what the Cognite Toolkit expects, so the file drops straight into a
Toolkit project.

### Finishing the setup in CDF

This tool is Azure-only. In CDF, create a group whose `sourceId` is the `ENTRA_GROUP_ID` value
(also available as `terraform output group_object_id`) and give it the capabilities you need.

### Verify the credentials work

```bash
set -a && . ./.env && set +a && curl -s -X POST "$IDP_TOKEN_URL" -d grant_type=client_credentials -d client_id=$IDP_CLIENT_ID -d client_secret=$IDP_CLIENT_SECRET -d scope=$IDP_SCOPES | head -c 200
```

## Rotating the secret

```bash
terraform apply -replace=azuread_application_password.cdf
```

Regenerates the secret and rewrites `.env`.

## Cleaning up

```bash
terraform destroy
```

Removes the group, app registration, service principal, and the `.env`.

## Security notes

- **`terraform.tfstate` contains the client secret in plaintext.** With the default local backend
  the state file is as sensitive as the `.env`. Both are gitignored. For shared/team use, configure
  an encrypted remote backend (e.g. Azure Storage with `azurerm` backend).
- `.env` is written with `0600` permissions and never printed to the terminal — the secret is not
  a Terraform output.
- Nothing is hard-coded: tenant, cluster, project, and names are all variables.
