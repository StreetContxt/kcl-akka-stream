package com.contxt.kinesis

import java.time.ZonedDateTime

import akka.Done
import akka.stream.scaladsl.SourceQueueWithComplete
import akka.stream.{KillSwitch, QueueOfferResult}
import org.slf4j.LoggerFactory
import software.amazon.kinesis.exceptions.{KinesisClientLibDependencyException, ShutdownException, ThrottlingException}
import software.amazon.kinesis.lifecycle.events.{
  InitializationInput,
  LeaseLostInput,
  ProcessRecordsInput,
  ShardEndedInput,
  ShutdownRequestedInput
}
import software.amazon.kinesis.lifecycle.{ShutdownInput, ShutdownReason}
import software.amazon.kinesis.processor.{RecordProcessorCheckpointer, ShardRecordProcessor}
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber

import scala.collection.JavaConversions._
import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

private[kinesis] class ShardCheckpointTracker(shardCheckpointConfig: ShardCheckpointConfig) {
  private val lock = new Object

  private var unprocessedInFlightRecords = Queue.empty[KinesisRecord]
  private var lastCheckpointedAt = ZonedDateTime.now()
  private var lastProcessedButNotCheckpointed = Option.empty[KinesisRecord]
  private var processedButNotCheckpointedCount = 0

  def nrOfInFlightRecords: Int = lock.synchronized {
    unprocessedInFlightRecords.size + processedButNotCheckpointedCount
  }

  def nrOfProcessedUncheckpointedRecords: Int = lock.synchronized {
    popProcessedRecords()
    processedButNotCheckpointedCount
  }

  def watchForCompletion(records: Iterable[KinesisRecord]): Unit = lock.synchronized {
    unprocessedInFlightRecords ++= records
  }

  def shouldCheckpoint: Boolean = lock.synchronized {
    popProcessedRecords()

    processedButNotCheckpointedCount >= shardCheckpointConfig.checkpointAfterProcessingNrOfRecords ||
    durationSinceLastCheckpoint() >= shardCheckpointConfig.checkpointPeriod
  }

  def checkpointLastProcessedRecord(checkpointLogic: KinesisRecord => Unit): Unit = lock.synchronized {
    popProcessedRecords()

    lastProcessedButNotCheckpointed.foreach { kinesisRecord =>
      try {
        checkpointLogic(kinesisRecord)
        lastProcessedButNotCheckpointed = None
      } finally {
        clearCheckpointTriggers()
      }
    }
  }

  def allInFlightRecordsProcessed: Boolean = lock.synchronized {
    unprocessedInFlightRecords.forall(_.completionFuture.isCompleted)
  }

  def allInFlightRecordsProcessedFuture(implicit ec: ExecutionContext): Future[Done] = lock.synchronized {
    Future
      .sequence(unprocessedInFlightRecords.map(_.completionFuture))
      .map(_ => Done)
  }

  private def popProcessedRecords(): Unit = {
    val processedRecords = unprocessedInFlightRecords.takeWhile(_.completionFuture.isCompleted)
    unprocessedInFlightRecords = unprocessedInFlightRecords.drop(processedRecords.size)
    processedButNotCheckpointedCount += processedRecords.size
    lastProcessedButNotCheckpointed = processedRecords.lastOption.orElse(lastProcessedButNotCheckpointed)
  }

  private def clearCheckpointTriggers(): Unit = {
    processedButNotCheckpointedCount = 0
    lastCheckpointedAt = ZonedDateTime.now()
  }

  private def durationSinceLastCheckpoint(): Duration = {
    java.time.Duration
      .between(lastCheckpointedAt, ZonedDateTime.now())
      .toMillis
      .millis
  }
}

private[kinesis] class RecordProcessorImpl(
    kinesisAppId: KinesisAppId,
    streamKillSwitch: KillSwitch,
    streamTerminationFuture: Future[Done],
    queue: SourceQueueWithComplete[IndexedSeq[KinesisRecord]],
    shardCheckpointConfig: ShardCheckpointConfig,
    consumerStats: ConsumerStats
) extends ShardRecordProcessor {
  private val log = LoggerFactory.getLogger(getClass)

  private val shardCheckpointTracker = new ShardCheckpointTracker(shardCheckpointConfig)
  private var shardId: String = _
  private lazy val shardConsumerId = ShardConsumerId(kinesisAppId, shardId)

  override def initialize(initializationInput: InitializationInput): Unit = {
    shardId = initializationInput.shardId()
    val offsetString = getOffsetString(initializationInput.extendedSequenceNumber())
    consumerStats.reportInitialization(shardConsumerId)
    log.info(s"Starting $shardConsumerId at $offsetString.")
  }

  override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
    try {
      val records = processRecordsInput.records().toIndexedSeq
      val kinesisRecords = records.map(KinesisRecord.fromMutableRecord)
      shardCheckpointTracker.watchForCompletion(kinesisRecords)
      recordCheckpointerStats()
      if (shardCheckpointTracker.shouldCheckpoint) checkpointAndHandleErrors(processRecordsInput.checkpointer())
      if (kinesisRecords.nonEmpty) blockToEnqueueAndHandleResult(kinesisRecords)
    } catch {
      case NonFatal(e) =>
        log.error("Unhandled exception in `processRecords()`, failing the streaming...", e)
        streamKillSwitch.abort(e)
    }
  }

  override def leaseLost(leaseLostInput: LeaseLostInput): Unit = {
    queue.complete()
    log.info(s"Lease lost: $shardId")
  }

  override def shardEnded(shardEndedInput: ShardEndedInput): Unit = {
    log.info(s"Shard end: $shardId")
    shutdown(
      ShutdownInput
        .builder()
        .checkpointer(shardEndedInput.checkpointer())
        .shutdownReason(ShutdownReason.SHARD_END)
        .build()
    )
  }

  override def shutdownRequested(shutdownRequestedInput: ShutdownRequestedInput): Unit = {
    log.info(s"Shutdown requested: $shardId")
    shutdown(
      ShutdownInput
        .builder()
        .checkpointer(shutdownRequestedInput.checkpointer())
        .shutdownReason(ShutdownReason.REQUESTED)
        .build()
    )
  }

  private def shutdown(shutdownInput: ShutdownInput): Unit = {
    val shutdownReason = shutdownInput.shutdownReason()

    shutdownReason match {
      case ShutdownReason.LEASE_LOST =>
      // Do nothing.

      case ShutdownReason.SHARD_END =>
        waitForInFlightRecordsOrTermination()
        checkpointAndHandleErrors(shutdownInput.checkpointer(), shardEnd = true)

      case ShutdownReason.REQUESTED =>
        /* The shutdown can be requested due to a stream failure or downstream cancellation. In either of these cases,
         * some records will never be processed, so we should not block waiting for all the records to complete.
         * Additionally, when we know the stream failed, and is being torn down, it's pointless to wait for in-flight
         * records to complete. */
        waitForInFlightRecordsUnlessStreamFailed(shardCheckpointConfig.maxWaitForCompletionOnStreamShutdown)
        checkpointAndHandleErrors(shutdownInput.checkpointer())
    }

    queue.complete()
    consumerStats.reportShutdown(shardConsumerId, shutdownReason)
    log.info(s"Finished shutting down $shardConsumerId, reason: $shutdownReason.")
  }

  private def blockToEnqueueAndHandleResult(kinesisRecords: IndexedSeq[KinesisRecord]): Unit = {
    try {
      kinesisRecords.foreach(consumerStats.trackRecord(shardConsumerId, _))
      val enqueueFuture = consumerStats.trackBatchEnqueue(shardConsumerId, kinesisRecords.size) {
        queue.offer(kinesisRecords)
      }
      Await.result(enqueueFuture, Duration.Inf) match {
        case QueueOfferResult.Enqueued =>
        // Do nothing.

        case QueueOfferResult.QueueClosed =>
        // Do nothing.

        case QueueOfferResult.Dropped =>
          streamKillSwitch.abort(
            new AssertionError("RecordProcessor source queue must use `OverflowStrategy.Backpressure`.")
          )

        case QueueOfferResult.Failure(e) =>
          streamKillSwitch.abort(e)
      }
    } catch {
      case NonFatal(e) => streamKillSwitch.abort(e)
    }
  }

  private def checkpointAndHandleErrors(checkpointer: RecordProcessorCheckpointer, shardEnd: Boolean = false): Unit = {
    try {
      if (shardEnd && shardCheckpointTracker.allInFlightRecordsProcessed) {
        // Checkpointing the actual offset is not enough. Instead, we are required to use the checkpoint()
        // method without arguments, which is not covered by Kinesis documentation.
        checkpointer.checkpoint()
        consumerStats.checkpointShardEndAcked(shardConsumerId)
        log.info(s"Successfully checkpointed $shardConsumerId at SHARD_END.")
      } else {
        shardCheckpointTracker.checkpointLastProcessedRecord { kinesisRecord =>
          val seqNumber = kinesisRecord.sequenceNumber
          val subSeqNumber = kinesisRecord.subSequenceNumber.getOrElse(0L)
          checkpointer.checkpoint(seqNumber, subSeqNumber)
          consumerStats.checkpointAcked(shardConsumerId)
          log.info(s"Successfully checkpointed $shardConsumerId at ${kinesisRecord.offsetString}.")
        }
      }
    } catch {
      case e: ShutdownException =>
      // Do nothing.

      case e @ (_: ThrottlingException | _: KinesisClientLibDependencyException) =>
        consumerStats.checkpointDelayed(shardConsumerId, e)
        log.error(s"Failed to checkpoint $shardConsumerId, will retry later.", e)

      case NonFatal(e) =>
        consumerStats.checkpointFailed(shardConsumerId, e)
        log.error(s"Failed to checkpoint $shardConsumerId, failing the streaming...", e)
        streamKillSwitch.abort(e)
    }
  }

  private def recordCheckpointerStats(): Unit = {
    consumerStats.recordNrOfInFlightRecords(shardConsumerId, shardCheckpointTracker.nrOfInFlightRecords)
    consumerStats.recordNrOfProcessedUncheckpointedRecords(
      shardConsumerId,
      shardCheckpointTracker.nrOfProcessedUncheckpointedRecords
    )
  }

  private def waitForInFlightRecordsOrTermination(): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val allProcessedOrTermination =
      Future.firstCompletedOf(Seq(shardCheckpointTracker.allInFlightRecordsProcessedFuture, streamTerminationFuture))
    Try(Await.result(allProcessedOrTermination, Duration.Inf))
  }

  private def waitForInFlightRecordsUnlessStreamFailed(waitDuration: Duration): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val hasStreamFailed = {
      if (streamTerminationFuture.isCompleted) Try(Await.result(streamTerminationFuture, 0.seconds)).isFailure
      else false
    }
    if (!hasStreamFailed) Try(Await.result(shardCheckpointTracker.allInFlightRecordsProcessedFuture, waitDuration))
  }

  private def getOffsetString(n: ExtendedSequenceNumber): String = {
    s"Offset(sequenceNumber=${n.sequenceNumber()}, subSequenceNumber=${n.subSequenceNumber()})"
  }
}
