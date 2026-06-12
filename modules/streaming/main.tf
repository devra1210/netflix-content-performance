resource "aws_kinesis_stream" "kaggle_dataset" {
  name             = "${var.name_prefix}-kaggle-dataset"
  retention_period = var.kinesis_retention_hours
  encryption_type  = "KMS"
  kms_key_id       = "alias/aws/kinesis"

  shard_count = var.kinesis_stream_mode == "PROVISIONED" ? var.kinesis_shard_count : null

  stream_mode_details {
    stream_mode = var.kinesis_stream_mode
  }

  shard_level_metrics = var.kinesis_stream_mode == "PROVISIONED" ? [
    "IncomingBytes",
    "IncomingRecords",
    "OutgoingBytes",
    "OutgoingRecords",
    "WriteProvisionedThroughputExceeded",
    "ReadProvisionedThroughputExceeded",
  ] : []
}
