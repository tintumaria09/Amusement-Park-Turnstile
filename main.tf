terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
    }
  }
}

provider "aws" {
  region = "ap-south-1"
}

# The Main Turnstile Queue

resource "aws_sqs_queue" "terraform_queue" {
  name                       = "amusement-park-turnstile"
  visibility_timeout_seconds = 30
  message_retention_seconds  = 86400
  receive_wait_time_seconds  = 20
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.terraform_dlq.arn
    maxReceiveCount     = 4
  })

}

# The Dead-Letter Queue (DLQ)

resource "aws_sqs_queue" "terraform_dlq" {
  name = "amusement-park-turnstile-dlq"
}


# The Explicit Redrive Allow Policy (Prevents circular dependencies)

resource "aws_sqs_queue_redrive_allow_policy" "terraform_queue_redrive_allow_policy" {
  queue_url = aws_sqs_queue.terraform_dlq.id

  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.terraform_queue.arn]
  })
}

resource "aws_dynamodb_table" "processed_tickets" {
  name           = "processed-tickets"
  billing_mode   = "PROVISIONED"
  read_capacity  = 5
  write_capacity = 5
  hash_key       = "ticket_id"

  attribute {
    name = "ticket_id"
    type = "S" # String

  }

  # Enable TTL on "expires_at" attribute
  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }
}
