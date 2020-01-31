package com.contxt.kinesis

import java.util.UUID

import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.common.{InitialPositionInStream, InitialPositionInStreamExtended, KinesisClientUtil}
import software.amazon.kinesis.coordinator.CoordinatorConfig
import software.amazon.kinesis.retrieval.RetrievalConfig
import software.amazon.kinesis.retrieval.polling.PollingConfig

import scala.concurrent.duration._

case class TestStreamConfig(
  regionName: String,
  streamName: String,
  applicationName: String,
  credentialsProvider: AwsCredentialsProvider,
  maxResultsPerGetRecordRequest: Int = 1000,
  idleTimeBetweenGetRecords: Duration = 1.second,
  checkpointAfterCompletingNrOfRecords: Int = 200
) {
  val shardCheckpointConfig = ShardCheckpointConfig(
    checkpointPeriod = 2.seconds,
    checkpointAfterCompletingNrOfRecords,
    maxWaitForCompletionOnStreamShutdown = 4.seconds
  )

  def kclConfig(workerId: String): ConsumerConfig = {
    val kinesisClient =
      KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder().region(Region.of(regionName)))
    val initialPositionInStream =
      InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON)
    ConsumerConfig(
      streamName,
      applicationName,
      workerId,
      kinesisClient,
      DynamoDbAsyncClient.builder.region(Region.of(regionName)).build(),
      CloudWatchAsyncClient.builder.region(Region.of(regionName)).build(),
      initialPositionInStream,
      coordinatorConfig = Some(
        new CoordinatorConfig(applicationName)
          .shardConsumerDispatchPollIntervalMillis(idleTimeBetweenGetRecords.toMillis)
      ),
      retrievalConfig = Some(
        new RetrievalConfig(kinesisClient, streamName, applicationName)
          .retrievalSpecificConfig(new PollingConfig(streamName, kinesisClient))
          .initialPositionInStreamExtended(initialPositionInStream)
      )
    )
  }

  def kplConfig: KinesisProducerConfiguration = {
    new KinesisProducerConfiguration()
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
