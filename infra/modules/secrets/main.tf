terraform {
  required_providers {
    aws = { source = "hashicorp/aws" }
  }
}

locals {
  params = {
    "/logstream/databricks_host"         = var.databricks_host
    "/logstream/databricks_token"        = var.databricks_token
    "/logstream/databricks_warehouse_id" = var.databricks_warehouse_id
    "/logstream/cribl_password"          = var.cribl_password
    "/logstream/session_secret"          = var.session_secret
  }
}

resource "aws_ssm_parameter" "p" {
  for_each = local.params
  name     = each.key
  type     = "SecureString"
  value    = each.value

  # Secret values are operator-managed after first creation (e.g. cribl_password is aligned to the live leader).
  lifecycle {
    ignore_changes = [value]
  }
}
