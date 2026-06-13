terraform {
  required_providers {
    aws = {
      source                = "hashicorp/aws"
      configuration_aliases = [aws, aws.consumer]
    }
  }
}

# Plan-B: workers are standalone Edge instances (not enrolled to the leader).
# Only port 10300 (data-forward) is needed — the :9000 management listener from
# Plan A has been dropped. Each worker's TCP output forwards events to this NLB
# on :10300, which proxies to the logging instance.

locals {
  # Single port in Plan B (data forward only).
  ports = { data = 10300 }
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

  # Only the Edge worker instances (their SG) may reach the endpoint on 10300.
  ingress {
    from_port       = 10300
    to_port         = 10300
    protocol        = "tcp"
    security_groups = [var.consumer_sg_id]
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
