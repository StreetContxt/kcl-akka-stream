package com.contxt.kinesis

case class ShardConsumerId(
  regionName: String,
  streamName: String,
  applicationName: String,
  shardId: String
)

object ShardConsumerId {
  private[kinesis] def apply(appId: KinesisAppId, shardId: String): ShardConsumerId = {
    ShardConsumerId(appId.regionName, appId.streamName, appId.applicationName, shardId)
  }
}
