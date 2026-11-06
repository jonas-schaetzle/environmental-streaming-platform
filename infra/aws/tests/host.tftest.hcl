mock_provider "aws" {
  override_during = plan

  mock_data "aws_ami" {
    defaults = { id = "ami-0123456789abcdef0" }
  }
}

variables {
  aws_account_id     = "123456789012"
  ami_id             = "ami-0123456789abcdef0"
  operator_ipv4_cidr = "203.0.113.10/32"
  ssh_public_key     = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA test"
}

run "keeps_the_costed_host_shape" {
  command = plan

  assert {
    condition = (
      aws_instance.host.instance_type == "t3.large" &&
      aws_instance.host.credit_specification[0].cpu_credits == "standard" &&
      !aws_instance.host.monitoring &&
      aws_instance.host.associate_public_ip_address
    )
    error_message = "Use the reviewed On-Demand shape, standard credits, basic monitoring and one ephemeral IPv4."
  }

  assert {
    condition = (
      aws_instance.host.root_block_device[0].volume_size == 40 &&
      aws_instance.host.root_block_device[0].volume_type == "gp3" &&
      aws_instance.host.root_block_device[0].encrypted &&
      aws_instance.host.root_block_device[0].delete_on_termination
    )
    error_message = "The encrypted 40 GiB gp3 root volume must be removed on authorized termination."
  }

  assert {
    condition = (
      aws_instance.host.metadata_options[0].http_tokens == "required" &&
      aws_instance.host.metadata_options[0].http_put_response_hop_limit == 1 &&
      aws_instance.host.instance_initiated_shutdown_behavior == "stop"
    )
    error_message = "Require IMDSv2 and map host poweroff to stop."
  }

  assert {
    condition = (
      aws_vpc_security_group_ingress_rule.ssh.cidr_ipv4 == var.operator_ipv4_cidr &&
      aws_vpc_security_group_ingress_rule.ssh.ip_protocol == "tcp" &&
      aws_vpc_security_group_ingress_rule.ssh.from_port == 22 &&
      aws_vpc_security_group_ingress_rule.ssh.to_port == 22 &&
      toset(keys(aws_vpc_security_group_egress_rule.web)) == toset(["80", "443"])
    )
    error_message = "Only the operator's SSH port and the two outbound web ports belong in this configuration."
  }
}

run "rejects_open_ssh_cidr" {
  command = plan

  variables {
    operator_ipv4_cidr = "0.0.0.0/0"
  }

  expect_failures = [var.operator_ipv4_cidr]
}

run "rejects_ipv6_for_ipv4_input" {
  command = plan

  variables {
    operator_ipv4_cidr = "2001:db8::1/32"
  }

  expect_failures = [var.operator_ipv4_cidr]
}
