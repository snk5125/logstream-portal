variable "region"                { type = string }
variable "logging_vpc_id"        { type = string }
variable "logging_subnet_id"     { type = string }
variable "logging_sg_id"         { type = string }
variable "instance_type"         { type = string }
variable "logging_instance_type" { type = string }
variable "ecr_repo_url"          { type = string }

variable "key_name" {
  type    = string
  default = ""
}

# Cribl Edge enrollment auth token (leader Distributed Settings → Auth token).
# Used only by managed Edge bootstrap; passed in via TF_VAR / tfvars, never committed.
variable "cribl_auth_token" {
  type      = string
  sensitive = true
  default   = ""
}

variable "worker_b" {
  type = object({
    vpc_id       = string
    subnet_id    = string
    sg_id        = string
    endpoint_dns = string
  })
}

variable "worker_c" {
  type = object({
    vpc_id       = string
    subnet_id    = string
    sg_id        = string
    endpoint_dns = string
  })
}
