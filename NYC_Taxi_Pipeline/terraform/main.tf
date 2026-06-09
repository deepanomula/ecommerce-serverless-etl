# 1. Create the Raw S3 Bucket (where your Lambda will drop CSV data)
resource "aws_s3_bucket" "raw_data" {
  bucket        = "${var.project_name}-raw-deepa"
  force_destroy = true # Allows Terraform to clean up the bucket easily later
}

# 2. Create the Cleaned S3 Bucket (where your Glue job will save Parquet files)
resource "aws_s3_bucket" "cleaned_data" {
  bucket        = "${var.project_name}-cleaned-deepa"
  force_destroy = true
}

# 3. Optional: Block public access to keep your taxi data secure
resource "aws_s3_bucket_public_access_block" "raw_security" {
  bucket = aws_s3_bucket.raw_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# 1. Create the Security Badge (IAM Role) itself
resource "aws_iam_role" "lambda_exec_role" {
  name = "${var.project_name}-lambda-role"

  # This policy tells AWS that only a Lambda service is allowed to wear this security badge
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

# 2. Attach basic execution permissions (so Lambda can write logs to CloudWatch)
resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda_exec_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# 3. Attach custom S3 permissions to the badge
resource "aws_iam_role_policy" "lambda_s3_access" {
  name = "${var.project_name}-lambda-s3-policy"
  role = aws_iam_role.lambda_exec_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.raw_data.arn,
          "${aws_s3_bucket.raw_data.arn}/*"
        ]
      },
      # ADD THIS NEW BLOCK FOR GLUE AUTOMATION:
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun"
        ],
        Resource = [
          "arn:aws:glue:us-east-2:285355564164:job/nyctaxi_s3_to_snowflake",
          "arn:aws:glue:us-east-2:285355564164:job/nyc-taxi-pipeline-transformer"
        ]
      }
    ]
  })
}

# 1. Create the IAM Role for AWS Glue
resource "aws_iam_role" "glue_exec_role" {
  name = "${var.project_name}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

# 2. Attach the standard AWS managed policy for Glue Service operations
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_exec_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# 3. Attach custom S3 permissions to the Glue role
resource "aws_iam_role_policy" "glue_s3_access" {
  name = "${var.project_name}-glue-s3-policy"
  role = aws_iam_role.glue_exec_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.raw_data.arn,
          "${aws_s3_bucket.raw_data.arn}/*",
          aws_s3_bucket.cleaned_data.arn,
          "${aws_s3_bucket.cleaned_data.arn}/*"
        ]
      }
    ]
  })
}