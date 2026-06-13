output "endpoint_dns" {
  description = "First DNS name for the interface endpoint (used in worker TCP output)"
  value       = aws_vpc_endpoint.ep.dns_entry[0].dns_name
}

output "service_name" {
  description = "VPC endpoint service name"
  value       = aws_vpc_endpoint_service.svc.service_name
}
