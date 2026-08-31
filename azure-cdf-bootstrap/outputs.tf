output "group_object_id" {
  description = "Object ID of the Entra security group. Use as sourceId when creating the CDF group."
  value       = azuread_group.cdf.object_id
}

output "group_display_name" {
  description = "Display name of the Entra security group."
  value       = azuread_group.cdf.display_name
}

output "client_id" {
  description = "Application (client) ID of the app registration."
  value       = azuread_application.cdf.client_id
}

output "service_principal_object_id" {
  description = "Object ID of the service principal, member of the security group."
  value       = azuread_service_principal.cdf.object_id
}

output "env_file_path" {
  description = "Path of the generated .env file (contains the client secret)."
  value       = local_sensitive_file.env.filename
}

output "next_steps" {
  description = "What to do with these values."
  value       = <<-EOT
    1. Credentials written to ${local_sensitive_file.env.filename} (mode 0600, gitignored).
    2. In CDF, create a group with sourceId = ${azuread_group.cdf.object_id} and the capabilities you need.
    3. The secret is also stored in plaintext in terraform.tfstate — keep that file as safe as the .env.
  EOT
}
