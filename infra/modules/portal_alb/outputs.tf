output "portal_url" {
  description = "HTTP URL for the portal via the ALB"
  value       = "http://${aws_lb.portal.dns_name}"
}

output "alb_sg_id" {
  description = "Security group ID of the ALB (referenced by the logging instance SG ingress rule)"
  value       = aws_security_group.alb.id
}
