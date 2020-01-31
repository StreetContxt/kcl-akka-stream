package com.contxt.kinesis

import java.time.Instant

import akka.Done
import akka.util.ByteString
import org.scalatest.{Matchers, WordSpec}
import software.amazon.awssdk.services.kinesis.model.EncryptionType

import scala.concurrent.duration._
import scala.concurrent.{Await, TimeoutException}

class ShardCheckpointTrackerTest extends WordSpec with Matchers {
  private val completionFutureAwaitDuration = 1.second

  "ShardCheckpointTracker" when {
    "deciding whether to checkpoint" should {
      "not checkpoint prematurely" in {
        val tracker = mkCheckpointTracker()
        val records = mkRecords(1)
        tracker.watchForCompletion(records)
        tracker.shouldCheckpoint shouldBe false
      }

      "checkpoint on target record count" in {
        val tracker = mkCheckpointTracker()
        val records =
          mkRecords(checkpointConfig.checkpointAfterProcessingNrOfRecords)
        tracker.watchForCompletion(records)
        records.foreach(_.markProcessed())
        tracker.shouldCheckpoint shouldBe true
      }

      "checkpoint after configure period" in {
        val tracker = mkCheckpointTracker()
        val records = mkRecords(1)
        tracker.watchForCompletion(records)
        Thread.sleep(checkpointConfig.checkpointPeriod.toMillis)
        tracker.shouldCheckpoint shouldBe true
      }
    }

    "checkpointing last processed record" should {
      "do nothing if no processed records" in {
        val tracker = mkCheckpointTracker()
        val records = mkRecords(1)
        tracker.watchForCompletion(records)

        var checkpointedRecord = Option.empty[KinesisRecord]
        tracker.checkpointLastProcessedRecord { record =>
          checkpointedRecord = Some(record)
        }
        checkpointedRecord shouldBe None
      }

      "checkpoint the last processed record in a sequence" in {
        val tracker = mkCheckpointTracker()
        val records = mkRecords(3)
        tracker.watchForCompletion(records)
        records.take(2).foreach(_.markProcessed())

        var checkpointedRecord = Option.empty[KinesisRecord]
        tracker.checkpointLastProcessedRecord { record =>
          checkpointedRecord = Some(record)
        }
        checkpointedRecord shouldBe Some(records(1))
      }

      "checkpoint the last record before a completion gap" in {
        val tracker = mkCheckpointTracker()
        val records = mkRecords(4)
        tracker.watchForCompletion(records)
        Seq(records(0), records(1), records(3)).foreach(_.markProcessed())

        var checkpointedRecord = Option.empty[KinesisRecord]
        tracker.checkpointLastProcessedRecord { record =>
          checkpointedRecord = Some(record)
        }
        checkpointedRecord shouldBe Some(records(1))
      }

      "rethrow an exception and clear checkpoint triggers" in {
        val tracker = mkCheckpointTracker()
        val records = mkRecords(2)
        tracker.watchForCompletion(records)
        records.foreach(_.markProcessed())

        tracker.shouldCheckpoint shouldBe true
        a[TestException] shouldBe thrownBy {
          tracker.checkpointLastProcessedRecord { record =>
            throw new TestException
          }
        }
        tracker.shouldCheckpoint shouldBe false
      }

      "rethrow an exception and keep the last processed record" in {
        val tracker = mkCheckpointTracker()
        val records = mkRecords(2)
        tracker.watchForCompletion(records)
        records.foreach(_.markProcessed())

        var checkpointedRecord1 = Option.empty[KinesisRecord]
        a[TestException] shouldBe thrownBy {
          tracker.checkpointLastProcessedRecord { record =>
            checkpointedRecord1 = Some(record)
            throw new TestException
          }
        }
        checkpointedRecord1 shouldBe Some(records(1))

        var checkpointedRecord2 = Option.empty[KinesisRecord]
        tracker.checkpointLastProcessedRecord { record =>
          checkpointedRecord2 = Some(record)
        }
        checkpointedRecord2 shouldBe Some(records(1))
      }
    }

    "checking for in flight record completion" should {
      "return true if all records are processed" in {
        val tracker = mkCheckpointTracker()
        val records = mkRecords(3)
        tracker.watchForCompletion(records)
        records.foreach(_.markProcessed())
        tracker.allInFlightRecordsProcessed shouldBe true
      }

      "return false if at least one record is not processed" in {
        val tracker = mkCheckpointTracker()
        val records = mkRecords(3)
        tracker.watchForCompletion(records)
        Seq(records(0), records(2)).foreach(_.markProcessed())
        tracker.allInFlightRecordsProcessed shouldBe false
      }
    }

    "creating in flight record completion future" should {
      "return a future that completes when all the records are processed" in {
        val tracker = mkCheckpointTracker()
        val records = mkRecords(3)
        tracker.watchForCompletion(records)
        records.foreach(_.markProcessed())
        import scala.concurrent.ExecutionContext.Implicits.global
        Await.result(tracker.allInFlightRecordsProcessedFuture, completionFutureAwaitDuration) shouldBe Done
      }

      "return a future that wont complete as long as a record remains unprocessed" in {
        val tracker = mkCheckpointTracker()
        val records = mkRecords(3)
        tracker.watchForCompletion(records)
        Seq(records(0), records(2)).foreach(_.markProcessed())
        a[TimeoutException] shouldBe thrownBy {
          import scala.concurrent.ExecutionContext.Implicits.global
          Await.result(tracker.allInFlightRecordsProcessedFuture, completionFutureAwaitDuration)
        }
      }
    }
  }

  private class TestException extends RuntimeException("Test Exception")

  private val checkpointConfig = ShardCheckpointConfig(
    checkpointPeriod = 2.second,
    checkpointAfterProcessingNrOfRecords = 2,
    maxWaitForCompletionOnStreamShutdown = 2.second
  )
  private def mkCheckpointTracker() =
    new ShardCheckpointTracker(checkpointConfig)

  private def mkRecord() = KinesisRecord(
    ByteString("testData".getBytes("UTF-8")),
    partitionKey = "testPartitionKey",
    sequenceNumber = "123",
    approximateArrivalTimestamp = Instant.now(),
    encryptionType = Option(EncryptionType.NONE),
    explicitHashKey = None,
    subSequenceNumber = None
  )
  private def mkRecords(n: Int) = for (i <- 0 until n) yield mkRecord()
}
