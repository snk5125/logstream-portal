output "logging_instance_id" { value = aws_instance.logging.id }
output "logging_private_ip"  { value = aws_instance.logging.private_ip }
output "worker_b_id"         { value = aws_instance.worker_b.id }
output "worker_c_id"         { value = aws_instance.worker_c.id }
output "portal_data_volume_id" { value = aws_ebs_volume.portal_data.id }
