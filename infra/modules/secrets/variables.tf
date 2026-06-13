variable "databricks_host" {
  type        = string
  description = "Databricks workspace URL"
}

variable "databricks_token" {
  type        = string
  sensitive   = true
  description = "Databricks PAT"
}

variable "databricks_warehouse_id" {
  type        = string
  description = "Databricks SQL warehouse ID"
}

variable "cribl_password" {
  type        = string
  sensitive   = true
  description = "Cribl admin password"
}

variable "session_secret" {
  type        = string
  sensitive   = true
  description = "Portal session secret"
}
