terraform {
  required_providers {
    aws = { source = "hashicorp/aws" }
  }
}

resource "aws_security_group" "alb" {
  name_prefix = "logstream-alb-"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = [var.operator_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_lb" "portal" {
  name               = "logstream-portal"
  load_balancer_type = "application"
  subnets            = var.subnet_ids
  security_groups    = [aws_security_group.alb.id]
}

resource "aws_lb_target_group" "portal" {
  name     = "logstream-portal"
  port     = 8000
  protocol = "HTTP"
  vpc_id   = var.vpc_id

  health_check {
    path    = "/api/personas"
    matcher = "200"
  }
}

resource "aws_lb_target_group_attachment" "portal" {
  target_group_arn = aws_lb_target_group.portal.arn
  target_id        = var.instance_id
  port             = 8000
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.portal.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.portal.arn
  }
}
