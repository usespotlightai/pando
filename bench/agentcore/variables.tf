variable "region" {
  description = "AWS region of the runtime, the registry and the bucket. AgentCore V2 runs in a subset of regions."
  type        = string
  default     = "us-east-1"
}

variable "image_digest" {
  description = "Digest (sha256:...) of the probe image in the repository, as probe.sh push prints it. Empty creates only the repository."
  type        = string
  default     = ""
}

variable "allow_shell" {
  description = "Let an invocation run a shell command given in its payload (\"shell\")."
  type        = bool
  default     = false
}

variable "idle_timeout_seconds" {
  description = "Seconds a session may stay idle before AgentCore ends it."
  type        = number
  default     = 900
}

variable "max_lifetime_seconds" {
  description = "Seconds a session may live in all."
  type        = number
  default     = 28800
}
