variable "workload_name"       { type = string }           # "acct-b" / "acct-c"
variable "logging_vpc_id"      { type = string }
variable "logging_subnet_ids"  { type = list(string) }
variable "logging_instance_id" { type = string }           # NLB target (cribl-central)
variable "consumer_account_id" { type = string }           # allowlisted principal
variable "consumer_vpc_id"     { type = string }
variable "consumer_subnet_ids" { type = list(string) }
variable "consumer_sg_id"      { type = string }
