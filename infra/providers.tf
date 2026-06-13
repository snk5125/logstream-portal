terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.40" }
  }
}

provider "aws" {                       # logging account A (default profile)
  alias   = "logging"
  region  = var.region
  profile = "default"
}

provider "aws" {                       # workload account B
  alias   = "acct_b"
  region  = var.region
  profile = "seth-demo-b"
}

provider "aws" {                       # workload account C
  alias   = "acct_c"
  region  = var.region
  profile = "seth-demo-c"
}
