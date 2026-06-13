# ── Networks ─────────────────────────────────────────────────────────────────

module "net_logging" {
  source        = "./modules/network"
  providers     = { aws = aws.logging }
  name          = "logging"
  cidr          = var.vpc_cidrs["logging"]
  operator_cidr = var.operator_cidr
}

module "net_b" {
  source    = "./modules/network"
  providers = { aws = aws.acct_b }
  name      = "acct-b"
  cidr      = var.vpc_cidrs["acct_b"]
}

module "net_c" {
  source    = "./modules/network"
  providers = { aws = aws.acct_c }
  name      = "acct-c"
  cidr      = var.vpc_cidrs["acct_c"]
}

# ── ECR repository (logging account) ─────────────────────────────────────────
# Declared here at root so it's created before compute, then passed in.

resource "aws_ecr_repository" "portal" {
  provider     = aws.logging
  name         = "logstream-portal"
  force_delete = true
}

# ── Compute (EC2 instances + IAM) ────────────────────────────────────────────
# Logging instance is created before PrivateLink so the NLB can target it.
# Worker instances reference module.pl_b/pl_c endpoint DNS — no cycle because
# the logging instance and the workers are separate resources; Terraform resolves
# the dependency graph automatically.

module "compute" {
  source    = "./modules/compute"
  providers = { aws = aws.logging, aws.acct_b = aws.acct_b, aws.acct_c = aws.acct_c }

  region                = var.region
  logging_vpc_id        = module.net_logging.vpc_id
  logging_subnet_id     = module.net_logging.subnet_ids[0]
  logging_sg_id         = module.net_logging.sg_id
  instance_type         = var.instance_type
  logging_instance_type = var.logging_instance_type
  ecr_repo_url          = aws_ecr_repository.portal.repository_url
  key_name              = var.key_name

  worker_b = {
    vpc_id       = module.net_b.vpc_id
    subnet_id    = module.net_b.subnet_ids[0]
    sg_id        = module.net_b.sg_id
    endpoint_dns = module.pl_b.endpoint_dns
  }

  worker_c = {
    vpc_id       = module.net_c.vpc_id
    subnet_id    = module.net_c.subnet_ids[0]
    sg_id        = module.net_c.sg_id
    endpoint_dns = module.pl_c.endpoint_dns
  }
}

# ── PrivateLink (NLB + endpoint service per workload account) ─────────────────

module "pl_b" {
  source    = "./modules/privatelink"
  providers = { aws = aws.logging, aws.consumer = aws.acct_b }

  workload_name       = "acct-b"
  logging_vpc_id      = module.net_logging.vpc_id
  logging_subnet_ids  = module.net_logging.subnet_ids
  logging_instance_id = module.compute.logging_instance_id
  consumer_account_id = var.account_b
  consumer_vpc_id     = module.net_b.vpc_id
  consumer_subnet_ids = module.net_b.subnet_ids
  consumer_sg_id      = module.net_b.sg_id
}

module "pl_c" {
  source    = "./modules/privatelink"
  providers = { aws = aws.logging, aws.consumer = aws.acct_c }

  workload_name       = "acct-c"
  logging_vpc_id      = module.net_logging.vpc_id
  logging_subnet_ids  = module.net_logging.subnet_ids
  logging_instance_id = module.compute.logging_instance_id
  consumer_account_id = var.account_c
  consumer_vpc_id     = module.net_c.vpc_id
  consumer_subnet_ids = module.net_c.subnet_ids
  consumer_sg_id      = module.net_c.sg_id
}

# ── Portal ALB (logging account, operator-restricted) ─────────────────────────

module "portal_alb" {
  source    = "./modules/portal_alb"
  providers = { aws = aws.logging }

  vpc_id        = module.net_logging.vpc_id
  subnet_ids    = module.net_logging.subnet_ids
  instance_id   = module.compute.logging_instance_id
  operator_cidr = var.operator_cidr
}

# ── Security-group rules on the logging instance SG ──────────────────────────
# Allow traffic from the ALB on port 8000, and from the VPC CIDR on the Cribl
# ports (NLB forwarded traffic arrives from within the VPC on the instance).

resource "aws_security_group_rule" "logging_from_alb_8000" {
  provider                 = aws.logging
  type                     = "ingress"
  from_port                = 8000
  to_port                  = 8000
  protocol                 = "tcp"
  source_security_group_id = module.portal_alb.alb_sg_id
  security_group_id        = module.net_logging.sg_id
}

resource "aws_security_group_rule" "logging_cribl_api_vpc" {
  provider          = aws.logging
  type              = "ingress"
  from_port         = 9000
  to_port           = 9000
  protocol          = "tcp"
  cidr_blocks       = [var.vpc_cidrs["logging"]]
  security_group_id = module.net_logging.sg_id
}

resource "aws_security_group_rule" "logging_cribl_ingest_vpc" {
  provider          = aws.logging
  type              = "ingress"
  from_port         = 10300
  to_port           = 10300
  protocol          = "tcp"
  cidr_blocks       = [var.vpc_cidrs["logging"]]
  security_group_id = module.net_logging.sg_id
}

# ── SSM secrets (logging account) ────────────────────────────────────────────

module "secrets" {
  source    = "./modules/secrets"
  providers = { aws = aws.logging }

  databricks_host         = var.databricks_host
  databricks_token        = var.databricks_token
  databricks_warehouse_id = var.databricks_warehouse_id
  cribl_password          = var.cribl_password
  session_secret          = var.session_secret
}
