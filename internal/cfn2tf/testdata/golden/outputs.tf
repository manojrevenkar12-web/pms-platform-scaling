output "topic_arn" {
  value = aws_sns_topic.reservation_events_topic.arn
}

output "queue_url" {
  value = aws_sqs_queue.channel_sync_queue.url
}

output "exports_bucket_name" {
  value = aws_s3_bucket.exports_bucket.id
}

output "worker_role_arn" {
  value = aws_iam_role.worker_role.arn
}
