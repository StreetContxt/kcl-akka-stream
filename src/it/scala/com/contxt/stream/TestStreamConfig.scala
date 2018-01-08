package com.contxt.stream

import com.amazonaws.auth.AWSCredentialsProviderChain
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{ DataFetchingStrategy, InitialPositionInStream, KinesisClientLibConfiguration }
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration
import java.util.UUID
import scala.concurrent.duration._

case class TestStreamConfig(
  regionName: String,
  streamName: String,
  applicationName: String,
  credentialsProvider: AWSCredentialsProviderChain,
  maxResultsPerGetRecordRequest: Int = 1000,
  idleTimeBetweenGetRecords: Duration = 1.second,
  checkpointAfterCompletingNrOfRecords: Int = 200
) {
  val shardCheckpointConfig = ShardCheckpointConfig(
    checkpointPeriod = 2.seconds,
    checkpointAfterCompletingNrOfRecords,
    maxWaitForCompletionOnStreamShutdown = 4.seconds
  )

  def kclConfig(workerId: String): KinesisClientLibConfiguration = {
    new KinesisClientLibConfiguration(
      applicationName,
      streamName,
      credentialsProvider,
      workerId
    )
      .withRegionName(regionName)
      .withCallProcessRecordsEvenForEmptyRecordList(true)
      .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
      .withDataFetchingStrategy(DataFetchingStrategy.DEFAULT.toString)
      .withMaxRecords(maxResultsPerGetRecordRequest)
      .withIdleMillisBetweenCalls(idleTimeBetweenGetRecords.toMillis)
      .withIdleTimeBetweenReadsInMillis(idleTimeBetweenGetRecords.toMillis)
  }

  def kplConfig: KinesisProducerConfiguration = {
    new KinesisProducerConfiguration()
      .setCredentialsProvider(credentialsProvider)
      .setRegion(regionName)
      .setRecordMaxBufferedTime(1.millis.toMillis)
      .setRequestTimeout(10.seconds.toMillis)
  }
}

object TestStreamConfig {
  def fromRandomUuid(): TestStreamConfig = {
    val uuid = UUID.randomUUID()

    TestStreamConfig(
      regionName = KinesisResourceManager.RegionName,
      streamName = s"${KinesisResourceManager.TempResourcePrefix}_stream_$uuid",
      applicationName = s"${KinesisResourceManager.TempResourcePrefix}_app_$uuid",
      credentialsProvider = KinesisResourceManager.CredentialsProvider
    )
  }
}
