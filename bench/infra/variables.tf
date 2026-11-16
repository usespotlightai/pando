variable "region" {
  description = "AWS region for the hosts, the bucket and the registry."
  type        = string
  default     = "us-east-1"
}

variable "instance_type" {
  description = "Instance type of each bench host. It needs local NVMe instance storage."
  type        = string
  default     = "m6id.4xlarge"
}

variable "second_host" {
  description = "Start host b, for the cross-host cold and save measures."
  type        = bool
  default     = false
}

variable "ssh_public_key" {
  description = "OpenSSH public key allowed to log in as ubuntu."
  type        = string
}

variable "ssh_cidrs" {
  description = "CIDR blocks allowed to reach the hosts over SSH, for example [\"203.0.113.4/32\"]."
  type        = list(string)
}

variable "ubuntu_version" {
  description = "Ubuntu release of the host image, resolved through Canonical's public SSM parameters."
  type        = string
  default     = "26.04"
}

variable "root_volume_gib" {
  description = "Size of each host's root volume (gp3)."
  type        = number
  default     = 100
}

variable "ebs_volume_gib" {
  description = "Size of the gp3 volume attached to host a for the EBS rows."
  type        = number
  default     = 100
}

variable "max_hours" {
  description = "Each host powers itself off this many hours after boot, so a forgotten host stops billing compute. 0 turns it off."
  type        = number
  default     = 8
}

variable "host_permissions_boundary" {
  description = "ARN of the permissions boundary for the host role. Required when the workflow role from bootstrap/ applies this; empty for none."
  type        = string
  default     = ""
}

variable "subnet_id" {
  description = "Subnet for the hosts. Empty uses the default VPC's subnet in an availability zone that offers the instance type."
  type        = string
  default     = ""
}
