variable "aws_account_id" {
  description = "ID of the approved AWS account."
  type        = string

  validation {
    condition     = can(regex("^[0-9]{12}$", var.aws_account_id))
    error_message = "Use a 12-digit AWS account ID."
  }
}

variable "ami_id" {
  description = "Reviewed Canonical Ubuntu 24.04 LTS amd64 server AMI in eu-central-1."
  type        = string

  validation {
    condition     = can(regex("^ami-[0-9a-f]{17}$", var.ami_id))
    error_message = "Use the exact reviewed AMI ID in eu-central-1."
  }
}

variable "operator_ipv4_cidr" {
  description = "Current public IPv4 address of the operator as a /32."
  type        = string

  validation {
    condition = (
      can(cidrnetmask(var.operator_ipv4_cidr)) &&
      endswith(var.operator_ipv4_cidr, "/32") &&
      var.operator_ipv4_cidr != "0.0.0.0/32"
    )
    error_message = "Use a single non-zero IPv4 address as a /32 CIDR."
  }
}

variable "ssh_public_key" {
  description = "Operator's OpenSSH Ed25519 or RSA public key."
  type        = string

  validation {
    condition     = can(regex("^ssh-(ed25519|rsa) [A-Za-z0-9+/=]+( [^\\r\\n]+)?$", trimspace(var.ssh_public_key)))
    error_message = "Provide a single OpenSSH Ed25519 or RSA public key line."
  }
}
