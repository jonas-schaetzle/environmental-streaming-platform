locals {
  name = "environmental-streaming-demo"
}

data "aws_ami" "ubuntu" {
  owners = ["099720109477"]

  filter {
    name   = "image-id"
    values = [var.ami_id]
  }

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }

  filter {
    name   = "architecture"
    values = ["x86_64"]
  }

  filter {
    name   = "is-public"
    values = ["true"]
  }

  filter {
    name   = "state"
    values = ["available"]
  }
}

resource "aws_vpc" "demo" {
  cidr_block           = "10.42.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = { Name = local.name }
}

resource "aws_subnet" "demo" {
  vpc_id     = aws_vpc.demo.id
  cidr_block = "10.42.1.0/24"
  tags       = { Name = local.name }
}

resource "aws_internet_gateway" "demo" {
  vpc_id = aws_vpc.demo.id
  tags   = { Name = local.name }
}

resource "aws_route_table" "demo" {
  vpc_id = aws_vpc.demo.id
  tags   = { Name = local.name }

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.demo.id
  }
}

resource "aws_route_table_association" "demo" {
  subnet_id      = aws_subnet.demo.id
  route_table_id = aws_route_table.demo.id
}

resource "aws_security_group" "host" {
  name        = local.name
  description = "SSH access for the operator and outbound HTTP(S)"
  vpc_id      = aws_vpc.demo.id
  tags        = { Name = local.name }
}

resource "aws_vpc_security_group_ingress_rule" "ssh" {
  security_group_id = aws_security_group.host.id
  cidr_ipv4         = var.operator_ipv4_cidr
  ip_protocol       = "tcp"
  from_port         = 22
  to_port           = 22
}

resource "aws_vpc_security_group_egress_rule" "web" {
  for_each = toset(["80", "443"])

  security_group_id = aws_security_group.host.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "tcp"
  from_port         = tonumber(each.key)
  to_port           = tonumber(each.key)
}

resource "aws_key_pair" "operator" {
  key_name   = local.name
  public_key = trimspace(var.ssh_public_key)
}

resource "aws_instance" "host" {
  ami                                  = data.aws_ami.ubuntu.id
  instance_type                        = "t3.large"
  subnet_id                            = aws_subnet.demo.id
  vpc_security_group_ids               = [aws_security_group.host.id]
  associate_public_ip_address          = true
  key_name                             = aws_key_pair.operator.key_name
  monitoring                           = false
  instance_initiated_shutdown_behavior = "stop"
  user_data                            = file("${path.module}/bootstrap.sh")
  tags                                 = { Name = local.name }
  volume_tags                          = { Name = local.name }

  credit_specification {
    cpu_credits = "standard"
  }

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 40
    encrypted             = true
    delete_on_termination = true
  }

  # Bootstrap needs the route and outbound rules before the host first boots.
  depends_on = [
    aws_route_table_association.demo,
    aws_vpc_security_group_egress_rule.web,
  ]
}
