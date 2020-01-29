package com.contxt.kinesis

import java.nio.ByteBuffer
import java.time.Instant

import akka.util.ByteString
import org.scalatest.{Matchers, WordSpec}
import software.amazon.awssdk.services.kinesis.model.EncryptionType
import software.amazon.kinesis.retrieval.KinesisClientRecord

class KinesisRecordTest extends WordSpec with Matchers {
  "KinesisRecord" when {
    "created from kinesis.model.Record" should {
      "have all the fields" in {
        val data = "testData".getBytes("UTF-8")
        val partitionKey = "testPartitionKey"
        val sequenceNumber = "12345"
        val timestamp = Instant.now()
        val encryptionType = EncryptionType.KMS

        val kinesisRecord = KinesisRecord.fromMutableRecord(KinesisClientRecord.builder()
          .data(ByteBuffer.wrap(data))
          .partitionKey(partitionKey)
          .sequenceNumber(sequenceNumber)
          .approximateArrivalTimestamp(timestamp)
          .encryptionType(encryptionType)
          .build()
        )
        val expected = KinesisRecord(
          ByteString(data),
          partitionKey = partitionKey,
          sequenceNumber = sequenceNumber,
          approximateArrivalTimestamp = timestamp,
          encryptionType = Some(encryptionType),
          explicitHashKey = None,
          subSequenceNumber = Some(0L)
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

        val userRecord = KinesisClientRecord.builder()
          .data(ByteBuffer.wrap(data))
          .partitionKey(partitionKey)
          .sequenceNumber(sequenceNumber)
          .approximateArrivalTimestamp(timestamp)
          .build()

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
          encryptionType = None
        )

        kinesisRecord shouldEqual expected
      }
    }
  }
}
