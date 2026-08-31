variable "tenant_id" {
  description = "Entra ID (Azure AD) tenant ID to create the group and app registration in."
  type        = string

  validation {
    condition     = can(regex("^[0-9a-fA-F-]{36}$", var.tenant_id))
    error_message = "tenant_id must be a GUID. Find yours with: az account show --query tenantId -o tsv"
  }

  validation {
    condition     = var.tenant_id != "00000000-0000-0000-0000-000000000000"
    error_message = "tenant_id is still the placeholder from terraform.tfvars.example — set your real tenant ID."
  }
}

variable "name_prefix" {
  description = "Prefix for the created resources: <prefix>-admin (group) and <prefix>-app (app registration)."
  type        = string
  default     = "cdf"
}

variable "cdf_cluster" {
  description = "CDF cluster the credentials target, e.g. westeurope-1, api, az-eastus-1. Only used to render the .env."
  type        = string
  default     = "westeurope-1"
}

variable "cdf_project" {
  description = "CDF project name. Only used to render the .env; leave empty if not known yet."
  type        = string
  default     = ""
}

variable "secret_validity_days" {
  description = "Lifetime of the generated client secret, in days."
  type        = number
  default     = 180
}

variable "env_file_path" {
  description = "Where to write the generated .env file."
  type        = string
  default     = ""
}
