// Authenticates with your own `az login` session — no secrets needed to run this.
provider "azuread" {
  tenant_id = var.tenant_id
}
