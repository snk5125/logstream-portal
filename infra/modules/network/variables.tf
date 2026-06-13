variable "name" { type = string }
variable "cidr" { type = string }
variable "operator_cidr" {
  type    = string
  default = ""
}
