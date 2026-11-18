data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

locals {
  account = data.aws_caller_identity.current.account_id
  region  = data.aws_region.current.region
}

# The probe image; bench/agentcore/push.sh builds it for arm64 and pushes it here.
resource "aws_ecr_repository" "probe" {
  name                 = "pando-agentcore-probe"
  force_delete         = true
  image_tag_mutability = "MUTABLE"
}

# The store of the end-to-end check. force_destroy lets `terraform destroy` remove it with its
# contents.
resource "aws_s3_bucket" "store" {
  bucket_prefix = "pando-agentcore-probe-"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "store" {
  bucket                  = aws_s3_bucket.store.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "store" {
  bucket = aws_s3_bucket.store.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "store" {
  bucket = aws_s3_bucket.store.id

  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

# The role the runtime's microVMs run as; the metadata service hands out its credentials.
resource "aws_iam_role" "runtime" {
  name_prefix = "pando-agentcore-probe-"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "bedrock-agentcore.amazonaws.com" }
      Action    = "sts:AssumeRole"
      Condition = {
        StringEquals = { "aws:SourceAccount" = local.account }
        ArnLike      = { "aws:SourceArn" = "arn:aws:bedrock-agentcore:${local.region}:${local.account}:*" }
      }
    }]
  })
}

resource "aws_iam_role_policy" "runtime" {
  name = "pando-agentcore-probe"
  role = aws_iam_role.runtime.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "RegistryLogin"
        Effect   = "Allow"
        Action   = "ecr:GetAuthorizationToken"
        Resource = "*"
      },
      {
        Sid      = "Registry"
        Effect   = "Allow"
        Action   = ["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer"]
        Resource = aws_ecr_repository.probe.arn
      },
      {
        Sid      = "Logs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams", "logs:DescribeLogGroups"]
        Resource = "arn:aws:logs:${local.region}:${local.account}:log-group:/aws/bedrock-agentcore/runtimes/*"
      },
      {
        Sid      = "Bucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = aws_s3_bucket.store.arn
      },
      {
        Sid      = "Objects"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
        Resource = "${aws_s3_bucket.store.arn}/*"
      },
    ]
  })
}

# The runtime, on platform V2: Firecracker microVMs restored from a snapshot, the platform that
# offers /dev/fuse and loop devices. There is no MMDS setting to make: V2 accepts only the
# token (MMDSv2) flow.
resource "aws_bedrockagentcore_agent_runtime" "probe" {
  count = var.image_digest != "" ? 1 : 0

  agent_runtime_name = "pando_probe"
  description        = "Pando probe: what an AgentCore V2 microVM offers Pando"
  role_arn           = aws_iam_role.runtime.arn
  platform_version   = "V2"

  agent_runtime_artifact {
    container_configuration {
      container_uri = "${aws_ecr_repository.probe.repository_url}@${var.image_digest}"
    }
  }

  network_configuration {
    network_mode = "PUBLIC"
  }

  protocol_configuration {
    server_protocol = "HTTP"
  }

  lifecycle_configuration {
    idle_runtime_session_timeout = var.idle_timeout_seconds
    max_lifetime                 = var.max_lifetime_seconds
  }

  environment_variables = {
    PANDO_BUCKET      = aws_s3_bucket.store.bucket
    PANDO_REGION      = local.region
    PROBE_ALLOW_SHELL = var.allow_shell ? "1" : "0"
  }

  depends_on = [aws_iam_role_policy.runtime]
}
