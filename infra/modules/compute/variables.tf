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
