output "hosts" {
  description = "Public IP of each bench host."
  value       = { for k, h in aws_instance.host : k => h.public_ip }
}

output "ssh" {
  description = "SSH command for each bench host."
  value       = { for k, h in aws_instance.host : k => "ssh ubuntu@${h.public_ip}" }
}

output "bucket" {
  description = "The bench bucket."
  value       = aws_s3_bucket.bench.bucket
}

output "registry" {
  description = "The ECR repository for the workspace image."
  value       = aws_ecr_repository.workspace.repository_url
}

output "availability_zone" {
  description = "Availability zone of the hosts and the EBS volume."
  value       = local.az
}
