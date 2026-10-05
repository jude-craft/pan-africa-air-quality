output "bronze_bucket_name" {
  description = "The globally unique name of the Bronze lakehouse bucket."
  value       = aws_s3_bucket.bronze.id
}

output "silver_bucket_name" {
  description = "The globally unique name of the Silver lakehouse bucket."
  value       = aws_s3_bucket.silver.id
}

output "gold_bucket_name" {
  description = "The globally unique name of the Gold lakehouse bucket."
  value       = aws_s3_bucket.gold.id
}

output "bronze_bucket_arn" {
  description = "The ARN of the Bronze bucket for IAM policy binding."
  value       = aws_s3_bucket.bronze.arn
}