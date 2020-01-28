package com.contxt.kinesis

case class ShardConsumerId(
  streamName: String,
  applicationName: String,
  shardId: String
)

object ShardConsumerId {
  private[kinesis] def apply(appId: KinesisAppId, shardId: String): ShardConsumerId = {
    ShardConsumerId(appId.streamName, appId.applicationName, shardId)
  }
}
