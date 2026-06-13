variable "region" {
  type    = string
  default = "us-east-1"
}

variable "operator_cidr" {
  type        = string
  description = "Your /32 for ALB + SSH access"
}

variable "key_name" {
  type        = string
  default     = ""
  description = "Optional EC2 keypair for SSH"
}

variable "instance_type" {
  type    = string
  default = "t3.small"
}

variable "logging_instance_type" {
  type    = string
  default = "t3.medium"
}

variable "account_a" {
  type    = string
  default = "337394138208"
}

variable "account_b" {
  type    = string
  default = "522412052544"
}

variable "account_c" {
  type    = string
  default = "624627265315"
}

variable "vpc_cidrs" {
  type = map(string)
  default = {
    logging = "10.30.0.0/16"
    acct_b  = "10.31.0.0/16"
    acct_c  = "10.32.0.0/16"
  }
}

# Secrets — pass via TF_VAR_* environment variables, never committed
variable "databricks_host" {
  type        = string
  description = "Databricks workspace URL, e.g. https://dbc-xxxx.cloud.databricks.com"
  default     = "https://dbc-2ef2bfc1-c689.cloud.databricks.com"
}

variable "databricks_token" {
  type        = string
  sensitive   = true
  description = "Databricks PAT — pass via TF_VAR_databricks_token"
}

variable "databricks_warehouse_id" {
  type        = string
  description = "Databricks SQL warehouse ID"
  default     = "0a3fea1c53bea9c6"
}

variable "cribl_password" {
  type        = string
  sensitive   = true
  description = "Cribl admin password — pass via TF_VAR_cribl_password"
}

variable "session_secret" {
  type        = string
  sensitive   = true
  description = "Portal session secret — pass via TF_VAR_session_secret"
}

variable "cribl_auth_token" {
  type        = string
  sensitive   = true
  default     = ""
  description = "Cribl Edge enrollment auth token (leader Distributed Settings) — pass via tfvars/TF_VAR; managed-Edge bootstrap only"
}
