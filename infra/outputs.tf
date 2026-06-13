output "logging_vpc" {
  description = "VPC ID of the logging account"
  value       = module.net_logging.vpc_id
}

output "portal_url" {
  description = "HTTP URL for the portal ALB"
  value       = module.portal_alb.portal_url
}

output "ecr_repo_url" {
  description = "ECR repository URL for the portal image"
  value       = aws_ecr_repository.portal.repository_url
}

output "logging_instance_id" {
  description = "EC2 instance ID of the logging/cribl-central instance"
  value       = module.compute.logging_instance_id
}

output "cribl_leader_private_ip" {
  description = "Private IP of the logging instance (use for SSM port-forward to :9000)"
  value       = module.compute.logging_private_ip
}

output "worker_b_id" {
  description = "EC2 instance ID of the acct-B Edge worker"
  value       = module.compute.worker_b_id
}

output "worker_c_id" {
  description = "EC2 instance ID of the acct-C Edge worker"
  value       = module.compute.worker_c_id
}

output "endpoint_dns_b" {
  description = "PrivateLink interface endpoint DNS for account B → logging :10300"
  value       = module.pl_b.endpoint_dns
}

output "endpoint_dns_c" {
  description = "PrivateLink interface endpoint DNS for account C → logging :10300"
  value       = module.pl_c.endpoint_dns
}

output "cribl_url" {
  description = "Cribl leader URL (reachable via SSM tunnel from logging instance)"
  value       = "http://${module.compute.logging_private_ip}:9000"
}

output "portal_data_volume_id" {
  description = "EBS volume ID holding the portal's SQLite state (the data tier)"
  value       = module.compute.portal_data_volume_id
}
