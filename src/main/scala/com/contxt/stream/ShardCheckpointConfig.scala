package com.contxt.stream

import scala.concurrent.duration.Duration

case class ShardCheckpointConfig(
  checkpointPeriod: Duration,
  checkpointAfterCompletingNrOfRecords: Int,
  maxWaitForCompletionOnStreamShutdown: Duration
)
