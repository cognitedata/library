locals {
  group_name    = "${var.name_prefix}-admin"
  app_name      = "${var.name_prefix}-app"
  env_file_path = var.env_file_path != "" ? var.env_file_path : "${path.module}/.env"
}

// The signed-in user (from `az login`) — made owner of everything created here.
data "azuread_client_config" "current" {}

resource "azuread_group" "cdf" {
  display_name     = local.group_name
  description      = "Entra security group backing a CDF group. Map its object ID to the CDF group's sourceId."
  security_enabled = true
  mail_enabled     = false
  owners           = [data.azuread_client_config.current.object_id]
}

resource "azuread_application" "cdf" {
  display_name     = local.app_name
  description      = "Service principal used for CDF authentication."
  sign_in_audience = "AzureADMyOrg"
  owners           = [data.azuread_client_config.current.object_id]
}

resource "azuread_service_principal" "cdf" {
  client_id = azuread_application.cdf.client_id
  owners    = [data.azuread_client_config.current.object_id]
}

// Membership is what ties the credentials to the CDF group's capabilities.
resource "azuread_group_member" "sp" {
  group_object_id  = azuread_group.cdf.object_id
  member_object_id = azuread_service_principal.cdf.object_id
}

// Recorded once at create time so the expiry does not drift on every plan.
resource "time_offset" "secret_expiry" {
  offset_days = var.secret_validity_days
}

resource "azuread_application_password" "cdf" {
  application_id = azuread_application.cdf.id
  display_name   = "terraform-managed"
  end_date       = time_offset.secret_expiry.rfc3339
}

resource "local_sensitive_file" "env" {
  filename        = local.env_file_path
  file_permission = "0600"

  content = templatefile("${path.module}/templates/env.tftpl", {
    tenant_id     = var.tenant_id
    client_id     = azuread_application.cdf.client_id
    client_secret = azuread_application_password.cdf.value
    cdf_cluster   = var.cdf_cluster
    cdf_project   = var.cdf_project
    group_id      = azuread_group.cdf.object_id
    group_name    = local.group_name
  })
}
