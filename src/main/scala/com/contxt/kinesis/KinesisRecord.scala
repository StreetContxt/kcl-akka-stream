package com.contxt.kinesis

import akka.Done
import akka.util.ByteString
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord
import com.amazonaws.services.kinesis.model.Record
import java.time.Instant
import scala.concurrent.{Future, Promise}

case class KinesisRecord(
    /** See [[com.amazonaws.services.kinesis.model.Record.getData]] for more details. */
    data: ByteString,
    /** See [[com.amazonaws.services.kinesis.model.Record.getPartitionKey]] for more details. */
    partitionKey: String,
    /** See [[com.amazonaws.services.kinesis.clientlibrary.types.UserRecord.getExplicitHashKey]] for more details. */
    explicitHashKey: Option[String],
    /** See [[com.amazonaws.services.kinesis.model.Record.getSequenceNumber]] for more details. */
    sequenceNumber: String,
    /** See [[com.amazonaws.services.kinesis.clientlibrary.types.UserRecord.getSubSequenceNumber]] for more details. */
    subSequenceNumber: Option[Long],
    /** See [[com.amazonaws.services.kinesis.model.Record.getApproximateArrivalTimestamp]] for more details. */
    approximateArrivalTimestamp: Instant,
    /** See [[com.amazonaws.services.kinesis.model.Record.getEncryptionType]] for more details. */
    encryptionType: String
) {
  private val completionPromise = Promise[Done]

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
      case None                     => s"Offset(sequenceNumber=$sequenceNumber)"
    }
  }
}

object KinesisRecord {
  def fromMutableRecord(record: Record): KinesisRecord = {
    val (subSequenceNumber, explicitHashKey) = record match {
      case userRecord: UserRecord => (Some(userRecord.getSubSequenceNumber), Option(userRecord.getExplicitHashKey))
      case _                      => (None, None)
    }
    KinesisRecord(
      data = ByteString(record.getData),
      partitionKey = record.getPartitionKey,
      explicitHashKey = explicitHashKey,
      sequenceNumber = record.getSequenceNumber,
      subSequenceNumber = subSequenceNumber,
      approximateArrivalTimestamp = record.getApproximateArrivalTimestamp.toInstant,
      encryptionType = Option(record.getEncryptionType).getOrElse("NONE")
    )
  }
}
