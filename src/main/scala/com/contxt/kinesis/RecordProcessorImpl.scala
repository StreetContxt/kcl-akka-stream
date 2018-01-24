package com.contxt.kinesis

import akka.Done
import akka.stream.{ KillSwitch, QueueOfferResult }
import akka.stream.scaladsl.SourceQueueWithComplete
import com.amazonaws.services.kinesis.clientlibrary.exceptions.{ KinesisClientLibDependencyException, KinesisClientLibRetryableException, ShutdownException, ThrottlingException }
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{ IRecordProcessor, IShutdownNotificationAware }
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.clientlibrary.types._
import java.time.ZonedDateTime
import org.slf4j.LoggerFactory
import scala.collection.immutable.Queue
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration._
import scala.util.Try
import scala.util.control.NonFatal
import scala.collection.JavaConversions._

private[kinesis] class ShardCheckpointTracker(shardCheckpointConfig: ShardCheckpointConfig) {
  private var unprocessedInFlightRecords = Queue.empty[KinesisRecord]
  private var lastCheckpointedAt = ZonedDateTime.now()
  private var lastProcessedButNotCheckpointed = Option.empty[KinesisRecord]
  private var processedButNotCheckpointedCount = 0

  def nrOfInFlightRecords: Int = {
    unprocessedInFlightRecords.size + processedButNotCheckpointedCount
  }
  def nrOfProcessedUncheckpointedRecords: Int = {
    popProcessedRecords()
    processedButNotCheckpointedCount
  }

  def watchForCompletion(records: Iterable[KinesisRecord]): Unit = {
    unprocessedInFlightRecords ++= records
  }

  def shouldCheckpoint: Boolean = {
    popProcessedRecords()

    processedButNotCheckpointedCount >= shardCheckpointConfig.checkpointAfterProcessingNrOfRecords ||
      durationSinceLastCheckpoint() >= shardCheckpointConfig.checkpointPeriod
  }

  def checkpointLastProcessedRecord(checkpointLogic: KinesisRecord => Unit): Unit = {
    popProcessedRecords()

    lastProcessedButNotCheckpointed.foreach { kinesisRecord =>
      try {
        checkpointLogic(kinesisRecord)
        lastProcessedButNotCheckpointed = None
      }
      finally {
        clearCheckpointTriggers()
      }
    }
  }

  def allInFlightRecordsProcessed: Boolean = unprocessedInFlightRecords.forall(_.completionFuture.isCompleted)

  def allInFlightRecordsProcessedFuture(implicit ec: ExecutionContext): Future[Done] = {
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
    java.time.Duration.between(lastCheckpointedAt, ZonedDateTime.now()).toMillis.millis
  }
}

private[kinesis] class RecordProcessorImpl(
  kinesisAppId: KinesisAppId,
  streamKillSwitch: KillSwitch,
  streamTerminationFuture: Future[Done],
  queue: SourceQueueWithComplete[IndexedSeq[KinesisRecord]],
  shardCheckpointConfig: ShardCheckpointConfig,
  consumerStats: ConsumerStats
) extends IRecordProcessor with IShutdownNotificationAware {
  private val log = LoggerFactory.getLogger(getClass)

  private val shardCheckpointTracker = new ShardCheckpointTracker(shardCheckpointConfig)
  private var shardId: String = _
  private lazy val shardConsumerId = ShardConsumerId(kinesisAppId, shardId)

  override def initialize(initializationInput: InitializationInput): Unit = {
    shardId = initializationInput.getShardId
    val offsetString = getOffsetString(initializationInput.getExtendedSequenceNumber)
    consumerStats.reportInitialization(shardConsumerId)
    log.info(s"Starting $shardConsumerId at $offsetString.")
  }

  override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
    try {
      val records = processRecordsInput.getRecords.toIndexedSeq
      val kinesisRecords = records.map(KinesisRecord.fromMutableRecord)
      shardCheckpointTracker.watchForCompletion(kinesisRecords)
      recordCheckpointerStats()
      if (shardCheckpointTracker.shouldCheckpoint) checkpointAndHandleErrors(processRecordsInput.getCheckpointer)
      if (kinesisRecords.nonEmpty) blockToEnqueueAndHandleResult(kinesisRecords)
    }
    catch {
      case NonFatal(e) =>
        log.error("Unhandled exception in `processRecords()`, failing the streaming...", e)
        streamKillSwitch.abort(e)
    }
  }

  override def shutdown(shutdownInput: ShutdownInput): Unit = {
    val shutdownReason = shutdownInput.getShutdownReason

    shutdownReason match {
      case ShutdownReason.ZOMBIE =>
        // Do nothing.

      case ShutdownReason.TERMINATE =>
        waitForInFlightRecordsOrTermination()
        checkpointAndHandleErrors(shutdownInput.getCheckpointer, shardEnd = true)

      case ShutdownReason.REQUESTED =>
        /* The shutdown can be requested due to a stream failure or downstream cancellation. In either of these cases,
         * some records will never be processed, so we should not block waiting for all the records to complete.
         * Additionally, when we know the stream failed, and is being torn down, it's pointless to wait for in-flight
         * records to complete. */
        waitForInFlightRecordsUnlessStreamFailed(shardCheckpointConfig.maxWaitForCompletionOnStreamShutdown)
        checkpointAndHandleErrors(shutdownInput.getCheckpointer)
    }

    queue.complete()
    consumerStats.reportShutdown(shardConsumerId, shutdownReason)
    log.info(s"Finished shutting down $shardConsumerId, reason: $shutdownReason.")
  }

  override def shutdownRequested(checkpointer: IRecordProcessorCheckpointer): Unit = {
    shutdown(
      new ShutdownInput()
        .withCheckpointer(checkpointer)
        .withShutdownReason(ShutdownReason.REQUESTED)
    )
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
          streamKillSwitch.abort(new AssertionError(
            "RecordProcessor source queue must use `OverflowStrategy.Backpressure`."
          ))

        case QueueOfferResult.Failure(e) =>
          streamKillSwitch.abort(e)
      }
    } catch {
      case NonFatal(e) => streamKillSwitch.abort(e)
    }
  }

  private def checkpointAndHandleErrors(checkpointer: IRecordProcessorCheckpointer, shardEnd: Boolean = false): Unit = {
    try {
      if (shardEnd && shardCheckpointTracker.allInFlightRecordsProcessed) {
        // Checkpointing the actual offset is not enough. Instead, we are required to use the checkpoint()
        // method without arguments, which is not covered by Kinesis documentation.
        checkpointer.checkpoint()
        consumerStats.checkpointShardEndAcked(shardConsumerId)
        log.info(s"Successfully checkpointed $shardConsumerId at SHARD_END.")
      }
      else {
        shardCheckpointTracker.checkpointLastProcessedRecord { kinesisRecord =>
          val seqNumber = kinesisRecord.sequenceNumber
          val subSeqNumber = kinesisRecord.subSequenceNumber.getOrElse(0L)
          checkpointer.checkpoint(seqNumber, subSeqNumber)
          consumerStats.checkpointAcked(shardConsumerId)
          log.info(s"Successfully checkpointed $shardConsumerId at ${kinesisRecord.offsetString}.")
        }
      }
    }
    catch {
      case e: ShutdownException =>
        // Do nothing.

      case e@(_: ThrottlingException | _: KinesisClientLibDependencyException) =>
        consumerStats.checkpointDelayed(shardConsumerId, e)
        log.error(s"Failed to checkpoint $shardConsumerId, will retry later.", e)

      case NonFatal(e) =>
        consumerStats.checkpointFailed(shardConsumerId, e)
        log.error(s"Failed to checkpoint $shardConsumerId, failing the streaming...", e)
        streamKillSwitch.abort(e)
    }
  }

  private def recordCheckpointerStats(): Unit = {
    consumerStats.recordNrOfInFlightRecords(
      shardConsumerId, shardCheckpointTracker.nrOfInFlightRecords
    )
    consumerStats.recordNrOfProcessedUncheckpointedRecords(
      shardConsumerId, shardCheckpointTracker.nrOfProcessedUncheckpointedRecords
    )
  }

  private def waitForInFlightRecordsOrTermination(): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val allProcessedOrTermination = Future.firstCompletedOf(Seq(
      shardCheckpointTracker.allInFlightRecordsProcessedFuture, streamTerminationFuture
    ))
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
    s"Offset(sequenceNumber=${n.getSequenceNumber}, subSequenceNumber=${n.getSubSequenceNumber})"
  }
}
