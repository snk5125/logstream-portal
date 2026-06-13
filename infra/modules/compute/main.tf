terraform {
  required_providers {
    aws = {
      source                = "hashicorp/aws"
      configuration_aliases = [aws, aws.acct_b, aws.acct_c]
    }
  }
}

# ── AMI lookups — one per provider alias (AL2023 AMI id differs per account) ─

data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

data "aws_ssm_parameter" "al2023_b" {
  provider = aws.acct_b
  name     = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

data "aws_ssm_parameter" "al2023_c" {
  provider = aws.acct_c
  name     = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

# ── Logging instance role ─────────────────────────────────────────────────────
# Permissions: kinesis/sqs by logstream-* prefix, s3 archive, ssm params, ECR pull.

resource "aws_iam_role" "logging" {
  name = "logstream-logging"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "logging" {
  role = aws_iam_role.logging.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["kinesis:*"]
        Resource = "arn:aws:kinesis:${var.region}:*:stream/logstream-*"
      },
      {
        Effect   = "Allow"
        Action   = ["sqs:*"]
        Resource = "arn:aws:sqs:${var.region}:*:logstream-*"
      },
      {
        Effect   = "Allow"
        Action   = ["kinesis:ListStreams", "sqs:ListQueues"]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = ["s3:PutObject", "s3:ListBucket", "s3:CreateBucket"]
        Resource = [
          "arn:aws:s3:::log-archive-*",
          "arn:aws:s3:::log-archive-*/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["ssm:GetParameter", "ssm:GetParameters"]
        Resource = "arn:aws:ssm:${var.region}:*:parameter/logstream/*"
      },
      {
        # Portal mints scoped per-stream read roles (logstream-read-*)
        Sid    = "MintStreamReadRoles"
        Effect = "Allow"
        Action = [
          "iam:CreateRole", "iam:DeleteRole", "iam:GetRole",
          "iam:PutRolePolicy", "iam:DeleteRolePolicy",
        ]
        Resource = "arn:aws:iam::337394138208:role/logstream/logstream-read-*"
      },
      {
        Effect = "Allow"
        Action = [
          "ecr:GetAuthorizationToken",
          "ecr:BatchGetImage",
          "ecr:GetDownloadUrlForLayer"
        ]
        Resource = "*"
      },
      {
        # SSM Session Manager (for operator access without SSH)
        Effect = "Allow"
        Action = [
          "ssmmessages:CreateControlChannel",
          "ssmmessages:CreateDataChannel",
          "ssmmessages:OpenControlChannel",
          "ssmmessages:OpenDataChannel",
          "ssm:UpdateInstanceInformation"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_instance_profile" "logging" {
  name = "logstream-logging"
  role = aws_iam_role.logging.name
}

# ── Logging instance ──────────────────────────────────────────────────────────
# Plan B: single container — cribl/cribl in CRIBL_DIST_MODE=master (single-instance
# leader+default-worker). Exposes port 9000 (API) and 10300 (TCP ingest).
# Portal container pulled from ECR.

resource "aws_instance" "logging" {
  ami                    = data.aws_ssm_parameter.al2023.value
  instance_type          = var.logging_instance_type
  subnet_id              = var.logging_subnet_id
  vpc_security_group_ids = [var.logging_sg_id]
  iam_instance_profile   = aws_iam_instance_profile.logging.name
  key_name               = var.key_name != "" ? var.key_name : null

  user_data = templatefile("${path.module}/user_data_logging.sh.tftpl", {
    region       = var.region
    ecr_repo_url = var.ecr_repo_url
  })

  # AMI sourced from the rolling "latest" pointer; never replace live instances
  # over an AMI roll. user_data is ignored too so the data-tier mount edit (and
  # future tweaks) update the IaC source of truth without replacing the running
  # instance — userData re-runs only on a real rebuild.
  lifecycle {
    ignore_changes = [ami, user_data]
  }

  tags = { Name = "logstream-cribl-central" }
}

# ── Portal data tier — dedicated EBS volume, decoupled from the instance ───────
# SQLite (stream registry) lives here, mounted at /opt/logstream-data on the host
# and bind-mounted into the portal container at /data. Surviving instance
# replacement is the whole point, so prevent_destroy guards against accidental
# teardown taking the data with it.
resource "aws_ebs_volume" "portal_data" {
  availability_zone = aws_instance.logging.availability_zone
  size              = 5
  type              = "gp3"
  tags              = { Name = "logstream-portal-data" }

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_volume_attachment" "portal_data" {
  device_name = "/dev/sdf"
  volume_id   = aws_ebs_volume.portal_data.id
  instance_id = aws_instance.logging.id
}

# ── Worker instances — acct_b ─────────────────────────────────────────────────
# Plan B: standalone Cribl Edge (single-instance), NOT enrolled to the leader.
# Each worker's TCP output forwards to the logging account's NLB on :10300 via
# the PrivateLink endpoint DNS. Workers pull cribl/cribl from Docker Hub.

resource "aws_iam_role" "worker_b" {
  provider = aws.acct_b
  name     = "logstream-worker"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "worker_b" {
  provider = aws.acct_b
  role     = aws_iam_role.worker_b.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      # SSM Session Manager — only inbound access workers need
      Effect = "Allow"
      Action = [
        "ssmmessages:CreateControlChannel",
        "ssmmessages:CreateDataChannel",
        "ssmmessages:OpenControlChannel",
        "ssmmessages:OpenDataChannel",
        "ssm:UpdateInstanceInformation"
      ]
      Resource = "*"
    }]
  })
}

resource "aws_iam_instance_profile" "worker_b" {
  provider = aws.acct_b
  name     = "logstream-worker"
  role     = aws_iam_role.worker_b.name
}

resource "aws_instance" "worker_b" {
  provider               = aws.acct_b
  ami                    = data.aws_ssm_parameter.al2023_b.value
  instance_type          = var.instance_type
  subnet_id              = var.worker_b.subnet_id
  vpc_security_group_ids = [var.worker_b.sg_id]
  iam_instance_profile   = aws_iam_instance_profile.worker_b.name
  key_name               = var.key_name != "" ? var.key_name : null

  user_data = templatefile("${path.module}/user_data_worker.sh.tftpl", {
    endpoint_dns = var.worker_b.endpoint_dns
    group        = "acct_b"
  })

  lifecycle {
    ignore_changes = [ami]
  }

  tags = { Name = "logstream-worker-acct-b" }
}

# ── Worker instances — acct_c ─────────────────────────────────────────────────

resource "aws_iam_role" "worker_c" {
  provider = aws.acct_c
  name     = "logstream-worker"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "worker_c" {
  provider = aws.acct_c
  role     = aws_iam_role.worker_c.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "ssmmessages:CreateControlChannel",
        "ssmmessages:CreateDataChannel",
        "ssmmessages:OpenControlChannel",
        "ssmmessages:OpenDataChannel",
        "ssm:UpdateInstanceInformation"
      ]
      Resource = "*"
    }]
  })
}

resource "aws_iam_instance_profile" "worker_c" {
  provider = aws.acct_c
  name     = "logstream-worker"
  role     = aws_iam_role.worker_c.name
}

resource "aws_instance" "worker_c" {
  provider               = aws.acct_c
  ami                    = data.aws_ssm_parameter.al2023_c.value
  instance_type          = var.instance_type
  subnet_id              = var.worker_c.subnet_id
  vpc_security_group_ids = [var.worker_c.sg_id]
  iam_instance_profile   = aws_iam_instance_profile.worker_c.name
  key_name               = var.key_name != "" ? var.key_name : null

  user_data = templatefile("${path.module}/user_data_worker.sh.tftpl", {
    endpoint_dns = var.worker_c.endpoint_dns
    group        = "acct_c"
  })

  lifecycle {
    ignore_changes = [ami]
  }

  tags = { Name = "logstream-worker-acct-c" }
}
