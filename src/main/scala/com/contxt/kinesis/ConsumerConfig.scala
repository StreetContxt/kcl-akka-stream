package com.contxt.kinesis

import java.text.DateFormat
import java.util.UUID

import com.typesafe.config.Config
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.common.{InitialPositionInStream, InitialPositionInStreamExtended, KinesisClientUtil}
import software.amazon.kinesis.coordinator.CoordinatorConfig
import software.amazon.kinesis.leases.LeaseManagementConfig
import software.amazon.kinesis.metrics.MetricsConfig
import software.amazon.kinesis.retrieval.RetrievalConfig

case class ConsumerConfig(
    streamName: String,
    appName: String,
    workerId: String,
    kinesisClient: KinesisAsyncClient,
    dynamoClient: DynamoDbAsyncClient,
    cloudwatchClient: CloudWatchAsyncClient,
    initialPositionInStreamExtended: InitialPositionInStreamExtended,
    coordinatorConfig: Option[CoordinatorConfig],
    leaseManagementConfig: Option[LeaseManagementConfig],
    metricsConfig: Option[MetricsConfig],
    retrievalConfig: Option[RetrievalConfig]
)

object ConsumerConfig {

  def apply(
      streamName: String,
      appName: String,
      workerId: String = ConsumerConfig.generateWorkerId,
      initialPositionInStreamExtended: InitialPositionInStreamExtended = defaultInitialPosition,
      coordinatorConfig: Option[CoordinatorConfig] = None,
      leaseManagementConfig: Option[LeaseManagementConfig] = None,
      metricsConfig: Option[MetricsConfig] = None,
      retrievalConfig: Option[RetrievalConfig] = None
  )(implicit
      kinesisClient: KinesisAsyncClient = defaultKinesisClient,
      dynamoClient: DynamoDbAsyncClient = defaultDynamoClient,
      cloudwatchClient: CloudWatchAsyncClient = defaultCloudwatchClient
  ): ConsumerConfig = {
    ConsumerConfig(
      streamName,
      appName,
      workerId,
      kinesisClient,
      dynamoClient,
      cloudwatchClient,
      initialPositionInStreamExtended,
      coordinatorConfig,
      leaseManagementConfig,
      metricsConfig,
      retrievalConfig
    )
  }

  def fromConfig(
      config: Config,
      workerId: String = ConsumerConfig.generateWorkerId,
      coordinatorConfig: Option[CoordinatorConfig] = None,
      leaseManagementConfig: Option[LeaseManagementConfig] = None,
      metricsConfig: Option[MetricsConfig] = None,
      retrievalConfig: Option[RetrievalConfig] = None
  )(implicit
      kac: KinesisAsyncClient = defaultKinesisClient,
      dac: DynamoDbAsyncClient = defaultDynamoClient,
      cwac: CloudWatchAsyncClient = defaultCloudwatchClient
  ): ConsumerConfig = {
    val parsedConfig = ParsedConsumerConfig(config)

    ConsumerConfig(
      parsedConfig.streamName,
      parsedConfig.appName,
      workerId,
      kac,
      dac,
      cwac,
      parsedConfig.initialPositionInStreamExtended.getOrElse(defaultInitialPosition),
      coordinatorConfig,
      leaseManagementConfig,
      metricsConfig,
      retrievalConfig
    )
  }

  private def defaultInitialPosition: InitialPositionInStreamExtended =
    InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)

  private def defaultKinesisClient: KinesisAsyncClient =
    KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder())

  private def defaultDynamoClient: DynamoDbAsyncClient = DynamoDbAsyncClient.builder.build()

  private def defaultCloudwatchClient: CloudWatchAsyncClient = CloudWatchAsyncClient.builder.build()

  private def generateWorkerId: String = UUID.randomUUID().toString
}

private case class ParsedConsumerConfig(
    streamName: String,
    appName: String,
    initialPositionInStreamExtended: Option[InitialPositionInStreamExtended]
)

private object ParsedConsumerConfig {
  val KeyAppName = "application-name"
  val KeyStreamName = "stream-name"
  val KeyInitialPosition = "position.initial"
  val KeyInitialPositionAtTimestampTime = "position.time"

  val InitialPositionLatest = "latest"
  val InitialPositionTrimHorizon = "trim-horizon"
  val InitialPositionAtTimestamp = "at-timestamp"

  def apply(config: Config): ParsedConsumerConfig = {
    ParsedConsumerConfig(
      streamName = config.getString(KeyStreamName),
      appName = config.getString(KeyAppName),
      initialPositionInStreamExtended = getStreamPosition(config)
    )
  }

  private def getStreamPosition(config: Config): Option[InitialPositionInStreamExtended] = {
    getStringOption(config, KeyInitialPosition)
      .map {
        case InitialPositionLatest =>
          InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)
        case InitialPositionTrimHorizon =>
          InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON)
        case InitialPositionAtTimestamp =>
          InitialPositionInStreamExtended.newInitialPositionAtTimestamp(
            DateFormat.getInstance().parse(config.getString(KeyInitialPositionAtTimestampTime))
          )
      }
  }

  private def getStringOption(config: Config, key: String): Option[String] = {
    if (config.hasPath(key)) Some(config.getString(key)) else None
  }
}
