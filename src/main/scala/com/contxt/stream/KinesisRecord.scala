package com.contxt.stream

import akka.Done
import akka.util.ByteString
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord
import com.amazonaws.services.kinesis.model.Record
import java.time.Instant
import scala.concurrent.{ Future, Promise }

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

  private[stream] def completionFuture: Future[Done] = completionPromise.future

  /** Record marked as processed are eligible for being checkpointed at driver's discretion. */
  def markProcessed(): Unit = completionPromise.trySuccess(Done)

  private[stream] def offsetString: String = {
    subSequenceNumber match {
      case Some(definedSubSequence) => s"Offset(sequenceNumber=$sequenceNumber, subSequenceNumber=$definedSubSequence)"
      case None => s"Offset(sequenceNumber=$sequenceNumber)"
    }
  }
}

object KinesisRecord {
  def fromMutableRecord(record: Record): KinesisRecord = {
    val (subSequenceNumber, explicitHashKey) = record match {
      case userRecord: UserRecord => (Some(userRecord.getSubSequenceNumber), Option(userRecord.getExplicitHashKey))
      case _ => (None, None)
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
