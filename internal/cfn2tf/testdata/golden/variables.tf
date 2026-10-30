# Parameters of the stack, defaulting to the values it was deployed with.

variable "environment" {
  type    = string
  default = "dev"
}

variable "export_retention_days" {
  type    = number
  default = 90
}

data "aws_caller_identity" "current" {
}

data "aws_partition" "current" {
}
