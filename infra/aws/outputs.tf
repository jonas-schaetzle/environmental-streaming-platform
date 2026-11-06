output "instance_id" {
  description = "ID used to verify the host lifecycle in EC2."
  value       = aws_instance.host.id
}

output "public_ipv4" {
  description = "Ephemeral public IPv4 address. Recheck after stop/start."
  value       = aws_instance.host.public_ip
}
