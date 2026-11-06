terraform {
  required_version = "~> 1.14.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "6.38.0"
    }
  }
}

provider "aws" {
  region              = "eu-central-1"
  allowed_account_ids = [var.aws_account_id]

  default_tags {
    tags = {
      Project   = local.name
      Purpose   = "bounded-demo"
      ManagedBy = "terraform"
    }
  }
}
