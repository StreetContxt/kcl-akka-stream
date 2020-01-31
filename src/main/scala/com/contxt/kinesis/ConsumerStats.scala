package com.contxt.kinesis

import akka.stream.QueueOfferResult
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import software.amazon.kinesis.lifecycle.ShutdownReason

import scala.concurrent.Future
import scala.util.control.NonFatal

trait ConsumerStats {
  def checkpointAcked(shardConsumerId: ShardConsumerId): Unit
  def checkpointShardEndAcked(shardConsumerId: ShardConsumerId): Unit
  def checkpointDelayed(shardConsumerId: ShardConsumerId, e: Throwable): Unit
  def checkpointFailed(shardConsumerId: ShardConsumerId, e: Throwable): Unit

  def trackRecord(shardConsumerId: ShardConsumerId, record: KinesisRecord): Unit

  def trackBatchEnqueue(shardConsumerId: ShardConsumerId, batchSize: Int)(
    closure: => Future[QueueOfferResult]
  ): Future[QueueOfferResult]

  def recordNrOfInFlightRecords(shardConsumerId: ShardConsumerId, totalCount: Int): Unit
  def recordNrOfProcessedUncheckpointedRecords(shardConsumerId: ShardConsumerId, totalCount: Int): Unit

  def reportInitialization(shardConsumerId: ShardConsumerId): Unit
  def reportShutdown(shardConsumerId: ShardConsumerId, reason: ShutdownReason): Unit
}

object ConsumerStats {
  private val log = LoggerFactory.getLogger(classOf[ConsumerStats])

  def getInstance(config: Config): ConsumerStats = {
    try {
      val className =
        config.getString("com.contxt.kinesis.consumer.stats-class-name")
      Class.forName(className).newInstance().asInstanceOf[ConsumerStats]
    } catch {
      case NonFatal(e) =>
        log.error("Could not load a `ConsumerStats` instance, falling back to `NoopConsumerStats`.", e)
        new NoopConsumerStats
    }
  }
}

class NoopConsumerStats extends ConsumerStats {
  def checkpointAcked(shardConsumerId: ShardConsumerId): Unit = {}
  def checkpointShardEndAcked(shardConsumerId: ShardConsumerId): Unit = {}
  def checkpointDelayed(shardConsumerId: ShardConsumerId, e: Throwable): Unit = {}
  def checkpointFailed(shardConsumerId: ShardConsumerId, e: Throwable): Unit = {}

  def trackRecord(shardConsumerId: ShardConsumerId, record: KinesisRecord): Unit = {}
  def trackBatchEnqueue(shardConsumerId: ShardConsumerId, batchSize: Int)(
    closure: => Future[QueueOfferResult]
  ): Future[QueueOfferResult] = closure

  def recordNrOfInFlightRecords(shardConsumerId: ShardConsumerId, totalCount: Int): Unit = {}
  def recordNrOfProcessedUncheckpointedRecords(shardConsumerId: ShardConsumerId, totalCount: Int): Unit = {}

  def reportInitialization(shardConsumerId: ShardConsumerId): Unit = {}
  def reportShutdown(shardConsumerId: ShardConsumerId, reason: ShutdownReason): Unit = {}
}
