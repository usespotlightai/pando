output "repository" {
  description = "The ECR repository probe.sh push pushes the probe image to."
  value       = aws_ecr_repository.probe.repository_url
}

output "runtime_arn" {
  description = "ARN of the probe runtime, for probe.sh invoke; empty until image_digest is set."
  value       = one(aws_bedrockagentcore_agent_runtime.probe[*].agent_runtime_arn)
}

output "log_group" {
  description = "The CloudWatch log group of the runtime's DEFAULT endpoint, where the probe's stdout goes."
  value       = try("/aws/bedrock-agentcore/runtimes/${aws_bedrockagentcore_agent_runtime.probe[0].agent_runtime_id}-DEFAULT", "")
}

output "bucket" {
  description = "The bucket of the end-to-end check."
  value       = aws_s3_bucket.store.bucket
}

output "region" {
  description = "The region of the runtime."
  value       = local.region
}
