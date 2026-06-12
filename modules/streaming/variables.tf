variable "name_prefix" {
  description = "Resource name prefix."
  type        = string
}

variable "kinesis_stream_mode" {
  description = "Kinesis stream capacity mode."
  type        = string
}

variable "kinesis_shard_count" {
  description = "Number of shards when using PROVISIONED capacity mode."
  type        = number
}

variable "kinesis_retention_hours" {
  description = "How long records are retained in the Kinesis stream."
  type        = number
}
