#!/usr/bin/env bash
# Verifies that what terraform claims it created actually exists in Entra ID,
# and that the generated .env is complete and readable only by you.
#
#   ./verify.sh            # check directory objects + .env
#   ./verify.sh --token    # also do a live client_credentials token request
#
# --token sends the client secret to login.microsoftonline.com. It is the only
# true end-to-end proof the credentials work, but it is opt-in for that reason.

set -uo pipefail
cd "$(dirname "$0")"

pass=0 fail=0
ok()   { printf '  \033[32m✓\033[0m %s\n' "$1"; pass=$((pass + 1)); }
bad()  { printf '  \033[31m✗\033[0m %s\n' "$1"; fail=$((fail + 1)); }
head_() { printf '\n\033[1m%s\033[0m\n' "$1"; }

need() {
  command -v "$1" >/dev/null 2>&1 || { echo "error: '$1' not found on PATH"; exit 2; }
}
need az
need terraform

# --- inputs from terraform state ---------------------------------------------
tf() { terraform output -raw "$1" 2>/dev/null; }

GROUP_ID=$(tf group_object_id)
CLIENT_ID=$(tf client_id)
SP_ID=$(tf service_principal_object_id)
ENV_FILE=$(tf env_file_path)

if [[ -z "$GROUP_ID" || -z "$CLIENT_ID" || -z "$SP_ID" ]]; then
  echo "error: no terraform outputs found. Run 'terraform apply' first (in $(pwd))."
  exit 2
fi

# --- az session --------------------------------------------------------------
head_ "Azure session"
ACTIVE_TENANT=$(az account show --query tenantId -o tsv 2>/dev/null)
if [[ -z "$ACTIVE_TENANT" ]]; then
  echo "  not logged in — run: az login --tenant <tenant-id> --allow-no-subscriptions"
  exit 2
fi
EXPECTED_TENANT=$(grep -E '^\s*tenant_id' terraform.tfvars 2>/dev/null | sed -E 's/.*"([^"]+)".*/\1/')
if [[ -n "$EXPECTED_TENANT" && "$ACTIVE_TENANT" != "$EXPECTED_TENANT" ]]; then
  bad "az context is tenant $ACTIVE_TENANT but terraform.tfvars targets $EXPECTED_TENANT"
  echo "     (a 403 on apply usually means exactly this)"
else
  ok "signed in to tenant $ACTIVE_TENANT"
fi

# --- directory objects -------------------------------------------------------
graph() { az rest --method GET --url "https://graph.microsoft.com/v1.0/$1" --query "$2" -o tsv 2>/dev/null; }

head_ "Entra ID objects"

NAME=$(graph "groups/$GROUP_ID" "displayName")
SEC=$(graph "groups/$GROUP_ID" "securityEnabled")
if [[ -n "$NAME" ]]; then
  ok "security group exists: $NAME ($GROUP_ID)"
  [[ "$SEC" == "true" ]] && ok "group is security-enabled" \
                         || bad "group is NOT security-enabled — CDF cannot map it"
else
  bad "security group $GROUP_ID NOT found"
fi

APP_NAME=$(graph "applications(appId='$CLIENT_ID')" "displayName")
if [[ -n "$APP_NAME" ]]; then
  ok "app registration exists: $APP_NAME (client id $CLIENT_ID)"
else
  bad "app registration $CLIENT_ID NOT found"
fi

SP_NAME=$(graph "servicePrincipals/$SP_ID" "displayName")
if [[ -n "$SP_NAME" ]]; then
  ok "service principal exists: $SP_NAME ($SP_ID)"
else
  bad "service principal $SP_ID NOT found"
fi

# Reverse lookup (sp -> groups). The forward listing (group -> members) can lag
# by a minute on a freshly created group; this direction is consistent sooner.
if graph "servicePrincipals/$SP_ID/memberOf" "value[].id" | grep -qx "$GROUP_ID"; then
  ok "service principal is a member of the group"
else
  bad "service principal is NOT in the group (or Graph has not replicated yet)"
fi

# Secret: check it exists and has not expired, without ever reading its value.
SECRET_END=$(graph "applications(appId='$CLIENT_ID')" "passwordCredentials[0].endDateTime")
if [[ -z "$SECRET_END" ]]; then
  bad "no client secret on the app registration"
else
  now=$(date -u +%s)
  # macOS (BSD) date first, then GNU date.
  exp=$(date -j -f "%Y-%m-%dT%H:%M:%SZ" "${SECRET_END%%.*}Z" +%s 2>/dev/null \
        || date -d "$SECRET_END" +%s 2>/dev/null)
  if [[ -n "$exp" && "$exp" -gt "$now" ]]; then
    ok "client secret valid, expires ${SECRET_END%%T*} ($(( (exp - now) / 86400 )) days left)"
  else
    bad "client secret expired or unparseable: $SECRET_END"
  fi
fi

# --- generated .env ----------------------------------------------------------
head_ "Generated .env"
if [[ ! -f "$ENV_FILE" ]]; then
  bad "$ENV_FILE does not exist"
else
  ok "$ENV_FILE exists"

  mode=$(stat -f '%Lp' "$ENV_FILE" 2>/dev/null || stat -c '%a' "$ENV_FILE" 2>/dev/null)
  [[ "$mode" == "600" ]] && ok "permissions are 0600" \
                         || bad "permissions are $mode, expected 600"

  for key in IDP_TENANT_ID IDP_CLIENT_ID IDP_CLIENT_SECRET IDP_TOKEN_URL IDP_SCOPES \
             CDF_CLUSTER CDF_PROJECT CDF_URL ENTRA_GROUP_ID; do
    # Presence and non-emptiness only — values are never printed.
    if grep -qE "^${key}=.+$" "$ENV_FILE"; then ok "$key is set"; else bad "$key is missing or empty"; fi
  done

  # These must agree with the live directory, or the .env is stale.
  grep -qE "^IDP_CLIENT_ID=${CLIENT_ID}$"  "$ENV_FILE" && ok "IDP_CLIENT_ID matches the app registration" \
                                                       || bad "IDP_CLIENT_ID does not match — .env is stale"
  grep -qE "^ENTRA_GROUP_ID=${GROUP_ID}$"  "$ENV_FILE" && ok "ENTRA_GROUP_ID matches the group" \
                                                       || bad "ENTRA_GROUP_ID does not match — .env is stale"

  if grep -qE '^CDF_PROJECT=my-cdf-project$' "$ENV_FILE"; then
    bad "CDF_PROJECT is still the example placeholder 'my-cdf-project'"
  fi
fi

# --- optional live token request ---------------------------------------------
if [[ "${1:-}" == "--token" ]]; then
  head_ "Token request (live)"
  set -a; . "$ENV_FILE"; set +a
  resp=$(curl -s -X POST "$IDP_TOKEN_URL" \
    -d grant_type=client_credentials \
    -d "client_id=$IDP_CLIENT_ID" \
    -d "client_secret=$IDP_CLIENT_SECRET" \
    -d "scope=$IDP_SCOPES")
  if grep -q '"access_token"' <<<"$resp"; then
    ok "client_credentials grant succeeded for $IDP_SCOPES"
  else
    bad "token request failed: $(sed -E 's/.*"error":"([^"]+)".*/\1/' <<<"$resp" | head -c 120)"
    echo "     (an AADSTS500011 here means the CDF cluster in IDP_SCOPES has no app registered in this tenant)"
  fi
fi

# --- summary -----------------------------------------------------------------
printf '\n\033[1m%d passed, %d failed\033[0m\n' "$pass" "$fail"
[[ "$fail" -eq 0 ]] || exit 1
