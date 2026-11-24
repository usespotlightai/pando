variable "region" {
  description = "Region of the state bucket and the bench."
  type        = string
  default     = "eu-north-1"
}

variable "github_repository" {
  description = "Repository allowed to assume the bench role, as owner/name."
  type        = string
  default     = "usespotlightai/pando"
}

variable "github_subject_prefix" {
  description = "Start of the OIDC subject the repository's tokens carry: gh api repos/OWNER/NAME/actions/oidc/customization/sub prints it as sub_claim_prefix."
  type        = string
  default     = "repo:usespotlightai@318774664/pando@1460617490"
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
