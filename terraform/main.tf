terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Bronze Bucket: Raw, immutable sensor landing zone
resource "aws_s3_bucket" "bronze" {
  bucket        = "${var.project_prefix}-bronze"
  force_destroy = true
}

# Silver Bucket: Cleaned, schema-enforced Parquet storage
resource "aws_s3_bucket" "silver" {
  bucket        = "${var.project_prefix}-silver"
  force_destroy = true
}

# Gold Bucket: Analytical aggregations and reporting tables
resource "aws_s3_bucket" "gold" {
  bucket        = "${var.project_prefix}-gold"
  force_destroy = true
}