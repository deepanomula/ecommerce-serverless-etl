variable "aws_region" {
  type        = string
  default     = "us-east-2"
  description = "The AWS region to deploy the taxi pipeline resources"
}

variable "project_name" {
  type        = string
  default     = "nyc-taxi-pipeline"
  description = "Prefix used for naming pipeline resources"
}