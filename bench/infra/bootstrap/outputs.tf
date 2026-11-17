output "role_arn" {
  description = "Repository variable AWS_ROLE_ARN."
  value       = aws_iam_role.github.arn
}

output "state_bucket" {
  description = "Repository variable BENCH_STATE_BUCKET."
  value       = aws_s3_bucket.state.bucket
}

output "host_permissions_boundary" {
  description = "Repository variable BENCH_HOST_BOUNDARY."
  value       = aws_iam_policy.host_boundary.arn
}

output "store_user" {
  description = "Create this user's access key in the console for the S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY secrets."
  value       = aws_iam_user.store.name
}
