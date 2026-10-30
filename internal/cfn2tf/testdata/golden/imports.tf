# Bind the resources above to what CloudFormation created. Remove after the first apply.

import {
  to = aws_sns_topic.reservation_events_topic
  id = "arn:aws:sns:eu-central-1:123456789012:reservation-events-dev"
}

import {
  to = aws_sqs_queue.channel_sync_dead_letter_queue
  id = "https://sqs.eu-central-1.amazonaws.com/123456789012/channel-sync-dlq-dev"
}

import {
  to = aws_sqs_queue.channel_sync_queue
  id = "https://sqs.eu-central-1.amazonaws.com/123456789012/channel-sync-dev"
}

import {
  to = aws_sqs_queue_policy.channel_sync_queue_policy
  id = "https://sqs.eu-central-1.amazonaws.com/123456789012/channel-sync-dev"
}

import {
  to = aws_sns_topic_subscription.channel_sync_subscription
  id = "arn:aws:sns:eu-central-1:123456789012:reservation-events-dev:6b0e71bd-7e97-4d97-80ce-4a0994e55286"
}

import {
  to = aws_s3_bucket.exports_bucket
  id = "pms-exports-dev-123456789012"
}

import {
  to = aws_s3_bucket_versioning.exports_bucket
  id = "pms-exports-dev-123456789012"
}

import {
  to = aws_s3_bucket_server_side_encryption_configuration.exports_bucket
  id = "pms-exports-dev-123456789012"
}

import {
  to = aws_s3_bucket_lifecycle_configuration.exports_bucket
  id = "pms-exports-dev-123456789012"
}

import {
  to = aws_s3_bucket_public_access_block.exports_bucket
  id = "pms-exports-dev-123456789012"
}

import {
  to = aws_dynamodb_table.idempotency_table
  id = "channel-sync-idempotency-dev"
}

import {
  to = aws_cloudwatch_log_group.worker_log_group
  id = "/pms/dev/channel-sync"
}

import {
  to = aws_ssm_parameter.channel_sync_endpoint_parameter
  id = "/pms/dev/channel-sync/queue-url"
}

import {
  to = aws_iam_role.worker_role
  id = "channel-sync-worker-dev"
}

import {
  to = aws_iam_role_policy.worker_role_consume_channel_sync
  id = "channel-sync-worker-dev:consume-channel-sync"
}

import {
  to = aws_iam_role_policy_attachment.worker_role_0
  id = "channel-sync-worker-dev/arn:aws:iam::aws:policy/CloudWatchLogsReadOnlyAccess"
}

import {
  to = aws_iam_role_policy.worker_exports_policy
  id = "channel-sync-worker-dev:write-exports"
}
