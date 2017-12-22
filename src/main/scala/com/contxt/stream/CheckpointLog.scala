package com.contxt.stream

private[stream] class CheckpointLog {
  import CheckpointLog._

  def checkpointEvent(shardKey: ShardKey, checkpointEvent: CheckpointEvent): Unit = {}
}

private[stream] object CheckpointLog {
  case class ShardKey(streamId: KinesisStreamId, shardId: String)

  sealed trait CheckpointEvent
  case class CheckpointAck(sequenceNumber: String, subSequenceNumber: Long) extends CheckpointEvent
  case object CheckpointShardEndAck extends CheckpointEvent
  case object CheckpointThrottled extends CheckpointEvent
}
