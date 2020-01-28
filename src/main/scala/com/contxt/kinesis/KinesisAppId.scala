package com.contxt.kinesis

private[kinesis] case class KinesisAppId(
  streamName: String,
  applicationName: String
)
