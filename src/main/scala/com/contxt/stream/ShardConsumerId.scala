package com.contxt.stream

case class ShardConsumerId(
  regionName: String,
  streamName: String,
  applicationName: String,
  shardId: String
)

object ShardConsumerId {
  def apply(appId: KinesisAppId, shardId: String): ShardConsumerId = {
    ShardConsumerId(appId.regionName, appId.streamName, appId.applicationName, shardId)
  }
}
