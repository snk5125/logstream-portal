terraform {
  required_providers {
    aws = {
      source                = "hashicorp/aws"
      configuration_aliases = [aws, aws.consumer]
    }
  }
}

# Managed-Edge model: workers enroll into the leader's default_fleet. Two ports
# flow workload→logging over this NLB/endpoint-service:
#   data   :10300 — each Edge's tcpjson output forwards tagged events
#   enroll :4200  — Edge nodes enroll + pull config from the leader (control plane)
# The leader's :9000 admin API is intentionally NOT exposed to the workload
# accounts — the portal reads it on localhost; edges only need :4200.

locals {
  ports = { data = 10300, enroll = 4200 }
}

# ── Logging account: NLB + endpoint service ───────────────────────────────────

resource "aws_lb" "nlb" {
  name               = "ls-${var.workload_name}"
  internal           = true
  load_balancer_type = "network"
  subnets            = var.logging_subnet_ids
}

resource "aws_lb_target_group" "tg" {
  for_each    = local.ports
  name        = "ls-${var.workload_name}-${each.key}"
  port        = each.value
  protocol    = "TCP"
  vpc_id      = var.logging_vpc_id
  target_type = "instance"
}

resource "aws_lb_target_group_attachment" "att" {
  for_each         = aws_lb_target_group.tg
  target_group_arn = each.value.arn
  target_id        = var.logging_instance_id
  port             = local.ports[each.key]
}

resource "aws_lb_listener" "lst" {
  for_each          = aws_lb_target_group.tg
  load_balancer_arn = aws_lb.nlb.arn
  port              = local.ports[each.key]
  protocol          = "TCP"
  default_action {
    type             = "forward"
    target_group_arn = each.value.arn
  }
}

resource "aws_vpc_endpoint_service" "svc" {
  acceptance_required        = false
  network_load_balancer_arns = [aws_lb.nlb.arn]
  allowed_principals         = ["arn:aws:iam::${var.consumer_account_id}:root"]
  tags                       = { Name = "logstream-${var.workload_name}" }
}

# ── Consumer (workload) account: interface endpoint ───────────────────────────

resource "aws_security_group" "vpce" {
  provider    = aws.consumer
  name_prefix = "ls-vpce-${var.workload_name}-"
  vpc_id      = var.consumer_vpc_id

  # Only the Edge worker instances (their SG) may reach the endpoint, on each
  # forwarded port (data :10300 + enroll :4200).
  dynamic "ingress" {
    for_each = local.ports
    content {
      from_port       = ingress.value
      to_port         = ingress.value
      protocol        = "tcp"
      security_groups = [var.consumer_sg_id]
    }
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_vpc_endpoint" "ep" {
  provider            = aws.consumer
  vpc_id              = var.consumer_vpc_id
  service_name        = aws_vpc_endpoint_service.svc.service_name
  vpc_endpoint_type   = "Interface"
  subnet_ids          = var.consumer_subnet_ids
  security_group_ids  = [aws_security_group.vpce.id]
  private_dns_enabled = false
}
