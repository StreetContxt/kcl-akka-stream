package com.contxt.kinesis

private[kinesis] case class KinesisAppId(
  regionName: String,
  streamName: String,
  applicationName: String
)
