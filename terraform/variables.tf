variable "aws_region" {
  description = "The target AWS region for provisioning storage infrastructure."
  type        = string
  default     = "us-east-1"
}

variable "project_prefix" {
  description = "Unique namespace prefix applied to all Medallion lakehouse buckets."
  type        = string
  default     = "jude-airquality"

  validation {
    condition     = can(regex("^[a-z0-9-]+$", var.project_prefix))
    error_message = "The project_prefix must contain only lowercase alphanumeric characters and hyphens."
  }
}