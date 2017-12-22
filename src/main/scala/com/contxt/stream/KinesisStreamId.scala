package com.contxt.stream

private[stream] case class KinesisStreamId(
  regionName: String,
  streamName: String,
  applicationName: String
)
