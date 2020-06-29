package com.contxt.kinesis

import java.time.Instant

import akka.Done
import akka.util.ByteString
import software.amazon.awssdk.services.kinesis.model.EncryptionType
import software.amazon.kinesis.retrieval.KinesisClientRecord

import scala.concurrent.{Future, Promise}

case class KinesisRecord(
    data: ByteString,
    partitionKey: String,
    explicitHashKey: Option[String],
    sequenceNumber: String,
    subSequenceNumber: Option[Long],
    approximateArrivalTimestamp: Instant,
    encryptionType: Option[EncryptionType]
) {
  private val completionPromise = Promise[Done]()

  private[kinesis] def completionFuture: Future[Done] = completionPromise.future

  /** Record marked as processed are eligible for being checkpointed at driver's discretion.
    *
    * A shard in a Kinesis stream is an ordered sequence of records. The shard is checkpointed by storing an offset
    * of the last processed record. However, if a record is not processed (for example, because of an exception),
    * then no further records after it can be checkpointed.
    *
    * KinesisSource keeps track of all the uncheckpointed records and their ordering. This means you can process
    * records out of order in an asynchronous fashion. Each record must be eventually marked as processed by
    * calling `markProcessed()`, or the steam must be terminated with an exception. If the stream continues
    * after failing to process a record, and not marking it as processed, then no further records can be checkpointed,
    * eventually causing the system to run out of memory. */
  def markProcessed(): Unit = completionPromise.trySuccess(Done)

  private[kinesis] def offsetString: String = {
    subSequenceNumber match {
      case Some(definedSubSequence) => s"Offset(sequenceNumber=$sequenceNumber, subSequenceNumber=$definedSubSequence)"
      case None => s"Offset(sequenceNumber=$sequenceNumber)"
    }
  }
}

object KinesisRecord {
  def fromMutableRecord(record: KinesisClientRecord): KinesisRecord = {
    KinesisRecord(
      data = ByteString(record.data()),
      partitionKey = record.partitionKey(),
      explicitHashKey = Option(record.explicitHashKey()),
      sequenceNumber = record.sequenceNumber(),
      subSequenceNumber = Option(record.subSequenceNumber()),
      approximateArrivalTimestamp = record.approximateArrivalTimestamp(),
      encryptionType = Option(record.encryptionType())
    )
  }
}
