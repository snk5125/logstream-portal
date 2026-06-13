output "vpc_id"     { value = aws_vpc.this.id }
output "subnet_ids" { value = aws_subnet.public[*].id }
output "sg_id"      { value = aws_security_group.instance.id }
output "cidr"       { value = aws_vpc.this.cidr_block }
