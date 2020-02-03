package com.contxt.kinesis

import akka.util.ByteString
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord
import com.amazonaws.services.kinesis.model.{EncryptionType, Record}
import java.nio.ByteBuffer
import java.time.Instant
import java.util.Date
import org.scalatest.{Matchers, WordSpec}

class KinesisRecordTest extends WordSpec with Matchers {
  "KinesisRecord" when {
    "created from kinesis.model.Record" should {
      "have all the fields" in {
        val data = "testData".getBytes("UTF-8")
        val partitionKey = "testPartitionKey"
        val sequenceNumber = "12345"
        val timestamp = Instant.now()
        val encryptionType = EncryptionType.KMS.toString

        val kinesisRecord = KinesisRecord.fromMutableRecord(
          new Record()
            .withData(ByteBuffer.wrap(data))
            .withPartitionKey(partitionKey)
            .withSequenceNumber(sequenceNumber)
            .withApproximateArrivalTimestamp(Date.from(timestamp))
            .withEncryptionType(encryptionType)
        )
        val expected = KinesisRecord(
          ByteString(data),
          partitionKey = partitionKey,
          sequenceNumber = sequenceNumber,
          approximateArrivalTimestamp = timestamp,
          encryptionType = encryptionType,
          explicitHashKey = None,
          subSequenceNumber = None
        )

        kinesisRecord shouldEqual expected
      }
    }

    "created from kinesis.producer.UserRecord" should {
      "have all the fields" in {
        val data = "testData".getBytes("UTF-8")
        val partitionKey = "testPartitionKey"
        val explicitHashKey = "testExplicitHashKey"
        val sequenceNumber = "12345"
        val subSequenceNumber = 123L
        val timestamp = Instant.now()
        val encryptionType = EncryptionType.NONE.toString

        val userRecord = new UserRecord(
          new Record()
            .withData(ByteBuffer.wrap(data))
            .withPartitionKey(partitionKey)
            .withSequenceNumber(sequenceNumber)
            .withApproximateArrivalTimestamp(Date.from(timestamp))
        )
        def setUserRecordField(fieldName: String, value: Any): Unit = {
          val field = userRecord.getClass.getDeclaredField(fieldName)
          field.setAccessible(true)
          field.set(userRecord, value)
        }
        setUserRecordField("subSequenceNumber", subSequenceNumber)
        setUserRecordField("explicitHashKey", explicitHashKey)

        val kinesisRecord = KinesisRecord.fromMutableRecord(userRecord)

        val expected = KinesisRecord(
          ByteString(data),
          partitionKey = partitionKey,
          explicitHashKey = Some(explicitHashKey),
          sequenceNumber = sequenceNumber,
          subSequenceNumber = Some(subSequenceNumber),
          approximateArrivalTimestamp = timestamp,
          encryptionType = encryptionType
        )

        kinesisRecord shouldEqual expected
      }
    }
  }
}
