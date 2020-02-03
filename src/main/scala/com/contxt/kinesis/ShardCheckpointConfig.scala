package com.contxt.kinesis

import com.typesafe.config.Config
import scala.concurrent.duration._

case class ShardCheckpointConfig(
    checkpointPeriod: Duration,
    checkpointAfterProcessingNrOfRecords: Int,
    maxWaitForCompletionOnStreamShutdown: Duration
)

object ShardCheckpointConfig {
  def apply(config: Config): ShardCheckpointConfig = {
    val localConfig = config.getConfig("com.contxt.kinesis.consumer.shard-checkpoint-config")
    ShardCheckpointConfig(
      checkpointPeriod = localConfig.getDuration("checkpoint-period").toMillis.millis,
      checkpointAfterProcessingNrOfRecords = localConfig.getInt("checkpoint-after-processing-nr-of-records"),
      maxWaitForCompletionOnStreamShutdown =
        localConfig.getDuration("max-wait-for-completion-on-stream-shutdown").toMillis.millis
    )
  }
}
