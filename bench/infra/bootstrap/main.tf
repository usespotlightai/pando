# One-time setup for running the bench from GitHub Actions. Apply it once, by hand, with an
# administrator's credentials; it keeps its state locally (keep terraform.tfstate somewhere safe).
#
# It creates:
# - the GitHub OIDC identity provider (skip with create_oidc_provider = false if the account has one);
# - the role the bench workflow assumes, only from the repository's `bench` environment;
# - the bucket that holds the bench's Terraform state;
# - the permissions boundary every bench host role must carry;
# - an IAM user for Pando's S3_* keys, limited to the bench buckets. Create its access key in the
#   console and store it as repository secrets; Terraform never sees it.

terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project = "pando-bench"
    }
  }
}

variable "region" {
  description = "Region of the state bucket and the bench."
  type        = string
  default     = "us-east-1"
}

variable "github_repository" {
  description = "Repository allowed to assume the bench role, as owner/name."
  type        = string
  default     = "usespotlightai/pando"
}

variable "github_environment" {
  description = "GitHub environment the bench workflow runs in."
  type        = string
  default     = "bench"
}

variable "create_oidc_provider" {
  description = "Create the token.actions.githubusercontent.com identity provider. False when the account already has it."
  type        = bool
  default     = true
}

data "aws_caller_identity" "current" {}

locals {
  account = data.aws_caller_identity.current.account_id
  # Every bench resource name starts with this; the state bucket deliberately does not.
  bench_buckets = "arn:aws:s3:::pando-bench-*"
  host_roles    = "arn:aws:iam::${local.account}:role/pando-bench-host-*"
  host_profiles = "arn:aws:iam::${local.account}:instance-profile/pando-bench-host-*"
  oidc_arn      = var.create_oidc_provider ? aws_iam_openid_connect_provider.github[0].arn : "arn:aws:iam::${local.account}:oidc-provider/token.actions.githubusercontent.com"
}

resource "aws_iam_openid_connect_provider" "github" {
  count          = var.create_oidc_provider ? 1 : 0
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
}

resource "aws_s3_bucket" "state" {
  bucket_prefix = "pando-tfstate-"

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_versioning" "state" {
  bucket = aws_s3_bucket.state.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "state" {
  bucket = aws_s3_bucket.state.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "state" {
  bucket                  = aws_s3_bucket.state.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# The most a bench host role can ever do, whatever policy the workflow attaches to it. Without
# it, a workflow that can create host roles could create one with more power than its own.
resource "aws_iam_policy" "host_boundary" {
  name        = "pando-bench-host-boundary"
  description = "Upper bound for Pando bench host roles"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation", "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
        Resource = [local.bench_buckets, "${local.bench_buckets}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["ecr:GetAuthorizationToken"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["ecr:*"]
        Resource = "arn:aws:ecr:${var.region}:${local.account}:repository/pando-bench/*"
      },
      {
        Effect = "Allow"
        Action = [
          "ec2:Describe*", "ec2:CreateVolume", "ec2:CreateSnapshot", "ec2:CreateTags",
          "ec2:AttachVolume", "ec2:DetachVolume", "ec2:DeleteVolume", "ec2:DeleteSnapshot",
          "ec2:EnableFastSnapshotRestores", "ec2:DisableFastSnapshotRestores",
        ]
        Resource = "*"
      },
    ]
  })
}

resource "aws_iam_role" "github" {
  name        = "pando-bench-github"
  description = "Assumed by the bench workflow in ${var.github_repository}"
  # The workflow's AWS session lasts its whole job (up to 6 h).
  max_session_duration = 21600

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = local.oidc_arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          "token.actions.githubusercontent.com:sub" = "repo:${var.github_repository}:environment:${var.github_environment}"
        }
      }
    }]
  })
}

resource "aws_iam_role_policy" "github" {
  name = "pando-bench-terraform"
  role = aws_iam_role.github.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "State"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = [aws_s3_bucket.state.arn, "${aws_s3_bucket.state.arn}/bench/*"]
      },
      {
        Sid      = "BenchBuckets"
        Effect   = "Allow"
        Action   = "s3:*"
        Resource = [local.bench_buckets, "${local.bench_buckets}/*"]
      },
      {
        Sid      = "Registry"
        Effect   = "Allow"
        Action   = "ecr:*"
        Resource = "arn:aws:ecr:${var.region}:${local.account}:repository/pando-bench/*"
      },
      {
        Sid      = "Ami"
        Effect   = "Allow"
        Action   = "ssm:GetParameter"
        Resource = "arn:aws:ssm:${var.region}::parameter/aws/service/canonical/*"
      },
      {
        Sid    = "Ec2Create"
        Effect = "Allow"
        Action = [
          "ec2:Describe*", "ec2:RunInstances", "ec2:CreateSecurityGroup",
          "ec2:ImportKeyPair", "ec2:CreateVolume", "ec2:AttachVolume",
        ]
        Resource = "*"
      },
      {
        # Tags only on what is being created. Tagging an existing resource would make it count
        # as the bench's and open it to the deletes below.
        Sid       = "Ec2TagOnCreate"
        Effect    = "Allow"
        Action    = "ec2:CreateTags"
        Resource  = "*"
        Condition = { StringEquals = { "ec2:CreateAction" = ["RunInstances", "CreateSecurityGroup", "ImportKeyPair", "CreateVolume"] } }
      },
      {
        # Changing or deleting anything needs the bench's tag, which the provider adds to all
        # it creates.
        Sid    = "Ec2Tagged"
        Effect = "Allow"
        Action = [
          "ec2:TerminateInstances", "ec2:StopInstances", "ec2:StartInstances", "ec2:ModifyInstanceAttribute",
          "ec2:DeleteSecurityGroup", "ec2:AuthorizeSecurityGroupIngress", "ec2:AuthorizeSecurityGroupEgress",
          "ec2:RevokeSecurityGroupIngress", "ec2:RevokeSecurityGroupEgress", "ec2:ModifySecurityGroupRules",
          "ec2:DeleteKeyPair", "ec2:DetachVolume", "ec2:DeleteVolume", "ec2:DeleteSnapshot", "ec2:ModifyVolume",
          "ec2:ModifyInstanceMetadataOptions", "ec2:AssociateIamInstanceProfile",
          "ec2:ReplaceIamInstanceProfileAssociation", "ec2:DisassociateIamInstanceProfile",
        ]
        Resource  = "*"
        Condition = { StringEquals = { "aws:ResourceTag/Project" = "pando-bench" } }
      },
      {
        Sid       = "HostRoleCreate"
        Effect    = "Allow"
        Action    = ["iam:CreateRole", "iam:PutRolePolicy"]
        Resource  = local.host_roles
        Condition = { StringEquals = { "iam:PermissionsBoundary" = aws_iam_policy.host_boundary.arn } }
      },
      {
        Sid    = "HostRoleManage"
        Effect = "Allow"
        Action = [
          "iam:GetRole", "iam:GetRolePolicy", "iam:ListRolePolicies", "iam:ListAttachedRolePolicies",
          "iam:ListInstanceProfilesForRole", "iam:DeleteRolePolicy", "iam:DeleteRole", "iam:TagRole",
          "iam:UntagRole", "iam:UpdateAssumeRolePolicy",
        ]
        Resource = local.host_roles
      },
      {
        Sid       = "HostRolePass"
        Effect    = "Allow"
        Action    = "iam:PassRole"
        Resource  = local.host_roles
        Condition = { StringEquals = { "iam:PassedToService" = "ec2.amazonaws.com" } }
      },
      {
        Sid    = "HostProfiles"
        Effect = "Allow"
        Action = [
          "iam:CreateInstanceProfile", "iam:DeleteInstanceProfile", "iam:GetInstanceProfile",
          "iam:AddRoleToInstanceProfile", "iam:RemoveRoleFromInstanceProfile", "iam:TagInstanceProfile",
        ]
        Resource = local.host_profiles
      },
      {
        Sid      = "NoBoundaryEscape"
        Effect   = "Deny"
        Action   = ["iam:DeleteRolePermissionsBoundary", "iam:PutRolePermissionsBoundary", "iam:AttachRolePolicy"]
        Resource = "*"
      },
    ]
  })
}

# Pando's own store credentials: S3 on the bench buckets, nothing else.
resource "aws_iam_user" "store" {
  name = "pando-bench-store"
}

resource "aws_iam_user_policy" "store" {
  name = "pando-bench-buckets"
  user = aws_iam_user.store.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:ListBucket", "s3:GetBucketLocation", "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
      Resource = [local.bench_buckets, "${local.bench_buckets}/*"]
    }]
  })
}

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
