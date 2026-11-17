locals {
  hosts     = var.second_host ? ["a", "b"] : ["a"]
  az        = var.subnet_id != "" ? data.aws_subnet.chosen[0].availability_zone : sort(data.aws_ec2_instance_type_offerings.nvme.locations)[0]
  subnet_id = var.subnet_id != "" ? var.subnet_id : data.aws_subnet.default[0].id
  vpc_id    = var.subnet_id != "" ? data.aws_subnet.chosen[0].vpc_id : data.aws_vpc.default[0].id
}

data "aws_caller_identity" "current" {}

data "aws_ssm_parameter" "ubuntu" {
  name = "/aws/service/canonical/ubuntu/server/${var.ubuntu_version}/stable/current/amd64/hvm/ebs-gp3/ami-id"
}

data "aws_ec2_instance_type_offerings" "nvme" {
  location_type = "availability-zone"

  filter {
    name   = "instance-type"
    values = [var.instance_type]
  }
}

data "aws_subnet" "chosen" {
  count = var.subnet_id != "" ? 1 : 0
  id    = var.subnet_id
}

data "aws_vpc" "default" {
  count   = var.subnet_id != "" ? 0 : 1
  default = true
}

data "aws_subnet" "default" {
  count             = var.subnet_id != "" ? 0 : 1
  vpc_id            = data.aws_vpc.default[0].id
  availability_zone = local.az
  default_for_az    = true
}

# The bucket every system under test stores its data in. force_destroy lets `terraform destroy`
# remove it with its contents.
resource "aws_s3_bucket" "bench" {
  bucket_prefix = "pando-bench-"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "bench" {
  bucket                  = aws_s3_bucket.bench.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bench" {
  bucket = aws_s3_bucket.bench.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "bench" {
  bucket = aws_s3_bucket.bench.id

  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

# The workspace as an OCI image, for the docker pull and lazy pull rows.
resource "aws_ecr_repository" "workspace" {
  name         = "pando-bench/workspace"
  force_delete = true
}

resource "aws_iam_role" "host" {
  name_prefix          = "pando-bench-host-"
  permissions_boundary = var.host_permissions_boundary != "" ? var.host_permissions_boundary : null

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "host" {
  name = "pando-bench"
  role = aws_iam_role.host.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "Bucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = aws_s3_bucket.bench.arn
      },
      {
        Sid      = "Objects"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
        Resource = "${aws_s3_bucket.bench.arn}/*"
      },
      {
        Sid      = "RegistryLogin"
        Effect   = "Allow"
        Action   = "ecr:GetAuthorizationToken"
        Resource = "*"
      },
      {
        Sid    = "Registry"
        Effect = "Allow"
        Action = [
          "ecr:BatchCheckLayerAvailability", "ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer",
          "ecr:InitiateLayerUpload", "ecr:UploadLayerPart", "ecr:CompleteLayerUpload", "ecr:PutImage",
          "ecr:DescribeImages", "ecr:ListImages", "ecr:BatchDeleteImage",
        ]
        Resource = aws_ecr_repository.workspace.arn
      },
      {
        Sid      = "EbsDescribe"
        Effect   = "Allow"
        Action   = ["ec2:DescribeVolumes", "ec2:DescribeSnapshots", "ec2:DescribeFastSnapshotRestores", "ec2:DescribeInstances"]
        Resource = "*"
      },
      {
        # The EBS rows snapshot the workspace volume and create, attach and delete volumes from
        # it. Everything they create carries the Project tag, and they touch nothing without it.
        Sid       = "EbsCreateTagged"
        Effect    = "Allow"
        Action    = ["ec2:CreateVolume", "ec2:CreateSnapshot"]
        Resource  = "*"
        Condition = { StringEquals = { "aws:RequestTag/Project" = "pando-bench" } }
      },
      {
        Sid       = "EbsCreateSnapshotOfTaggedVolume"
        Effect    = "Allow"
        Action    = "ec2:CreateSnapshot"
        Resource  = "arn:aws:ec2:${var.region}:${data.aws_caller_identity.current.account_id}:volume/*"
        Condition = { StringEquals = { "aws:ResourceTag/Project" = "pando-bench" } }
      },
      {
        Sid       = "EbsCreateVolumeFromTaggedSnapshot"
        Effect    = "Allow"
        Action    = "ec2:CreateVolume"
        Resource  = "arn:aws:ec2:${var.region}::snapshot/*"
        Condition = { StringEquals = { "aws:ResourceTag/Project" = "pando-bench" } }
      },
      {
        Sid       = "EbsTagOnCreate"
        Effect    = "Allow"
        Action    = "ec2:CreateTags"
        Resource  = "*"
        Condition = { StringEquals = { "ec2:CreateAction" = ["CreateVolume", "CreateSnapshot"] } }
      },
      {
        Sid    = "EbsManageTagged"
        Effect = "Allow"
        Action = [
          "ec2:AttachVolume", "ec2:DetachVolume", "ec2:DeleteVolume", "ec2:DeleteSnapshot",
          "ec2:EnableFastSnapshotRestores", "ec2:DisableFastSnapshotRestores",
        ]
        Resource  = "*"
        Condition = { StringEquals = { "aws:ResourceTag/Project" = "pando-bench" } }
      },
      {
        # The save measure counts the blocks an EBS snapshot stores beyond the workspace snapshot.
        Sid       = "EbsChangedBlocks"
        Effect    = "Allow"
        Action    = "ebs:ListChangedBlocks"
        Resource  = "arn:aws:ec2:${var.region}::snapshot/*"
        Condition = { StringEquals = { "aws:ResourceTag/Project" = "pando-bench" } }
      },
    ]
  })
}

resource "aws_iam_instance_profile" "host" {
  name_prefix = "pando-bench-host-"
  role        = aws_iam_role.host.name
}

resource "aws_key_pair" "bench" {
  key_name_prefix = "pando-bench-"
  public_key      = var.ssh_public_key
}

resource "aws_security_group" "host" {
  name_prefix = "pando-bench-"
  description = "Pando bench hosts: SSH from the operator, anything between the hosts"
  vpc_id      = local.vpc_id
}

resource "aws_vpc_security_group_ingress_rule" "ssh" {
  for_each          = toset(var.ssh_cidrs)
  security_group_id = aws_security_group.host.id
  cidr_ipv4         = each.value
  ip_protocol       = "tcp"
  from_port         = 22
  to_port           = 22
}

resource "aws_vpc_security_group_ingress_rule" "between_hosts" {
  security_group_id            = aws_security_group.host.id
  referenced_security_group_id = aws_security_group.host.id
  ip_protocol                  = "-1"
}

resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.host.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

# The gp3 volume the EBS rows start from. Created before the host, so its id can go into the
# host's environment file.
resource "aws_ebs_volume" "workspace" {
  availability_zone = local.az
  size              = var.ebs_volume_gib
  type              = "gp3"
  iops              = 3000
  throughput        = 125
  encrypted         = true

  tags = {
    Name = "pando-bench-ebs"
  }
}

resource "aws_instance" "host" {
  for_each = toset(local.hosts)

  ami                                  = nonsensitive(data.aws_ssm_parameter.ubuntu.value)
  instance_type                        = var.instance_type
  subnet_id                            = local.subnet_id
  vpc_security_group_ids               = [aws_security_group.host.id]
  key_name                             = aws_key_pair.bench.key_name
  iam_instance_profile                 = aws_iam_instance_profile.host.name
  associate_public_ip_address          = true
  instance_initiated_shutdown_behavior = "stop"
  ebs_optimized                        = true

  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  root_block_device {
    volume_type = "gp3"
    volume_size = var.root_volume_gib
    encrypted   = true
  }

  user_data = templatefile("${path.module}/cloud-init.yaml.tftpl", {
    host       = each.key
    region     = var.region
    bucket     = aws_s3_bucket.bench.bucket
    registry   = aws_ecr_repository.workspace.repository_url
    ebs_volume = each.key == "a" ? aws_ebs_volume.workspace.id : ""
    max_hours  = var.max_hours
  })
  user_data_replace_on_change = true

  tags = {
    Name = "pando-bench-${each.key}"
  }

  lifecycle {
    ignore_changes = [ami]
  }
}

resource "aws_volume_attachment" "workspace" {
  device_name = "/dev/sdf"
  volume_id   = aws_ebs_volume.workspace.id
  instance_id = aws_instance.host["a"].id
}
