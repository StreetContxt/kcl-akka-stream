package com.contxt.kinesis

import java.sql.Date
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

import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}


case class ConsumerConfig(
                           streamName: String,
                           appName: String,
                           workerId: String,
                           kinesisClient: KinesisAsyncClient,
                           dynamoClient: DynamoDbAsyncClient,
                           cloudwatchClient: CloudWatchAsyncClient,
                           initialPositionInStreamExtended: InitialPositionInStreamExtended =
                           InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST),
                           coordinatorConfig: Option[CoordinatorConfig] = None,
                           leaseManagementConfig: Option[LeaseManagementConfig] = None,
                           metricsConfig: Option[MetricsConfig] = None,
                           retrievalConfig: Option[RetrievalConfig] = None
                         ) {

  def withInitialStreamPosition(position: InitialPositionInStream): ConsumerConfig =
    this.copy(
      initialPositionInStreamExtended = InitialPositionInStreamExtended.newInitialPosition(position)
    )

  def withInitialStreamPositionAtTimestamp(time: Date): ConsumerConfig =
    this.copy(
      initialPositionInStreamExtended = InitialPositionInStreamExtended.newInitialPositionAtTimestamp(time)
    )

  def withCoordinatorConfig(config: CoordinatorConfig): ConsumerConfig =
    this.copy(coordinatorConfig = Some(config))

  def withLeaseManagementConfig(config: LeaseManagementConfig): ConsumerConfig =
    this.copy(leaseManagementConfig = Some(config))

  def withMetricsConfig(config: MetricsConfig): ConsumerConfig = this.copy(metricsConfig = Some(config))

  def withRetrievalConfig(config: RetrievalConfig): ConsumerConfig = this.copy(retrievalConfig = Some(config))
}

object ConsumerConfig {
  def apply(streamName: String, appName: String): ConsumerConfig = {
    val kinesisClient = KinesisAsyncClient.builder.build()
    val dynamoClient = DynamoDbAsyncClient.builder.build()
    val cloudWatchClient = CloudWatchAsyncClient.builder.build()

    withNames(streamName, appName)(kinesisClient, dynamoClient, cloudWatchClient)
  }

  def withNames(streamName: String, appName: String)
               (implicit kac: KinesisAsyncClient, dac: DynamoDbAsyncClient, cwac: CloudWatchAsyncClient): ConsumerConfig =
    ConsumerConfig(
      streamName,
      appName,
      generateWorkerId(),
      kac,
      dac,
      cwac
    )

  def fromConfig(config: Config)
                (implicit
                 kac: KinesisAsyncClient = null,
                 dac: DynamoDbAsyncClient = null,
                 cwac: CloudWatchAsyncClient = null
                ): ConsumerConfig = {
    def getOpt[A](key: String, lookup: String => A): Option[A] = if (config.hasPath(key)) Some(lookup(key)) else None

    def getIntOpt(key: String): Option[Int] = getOpt(key, config.getInt)

    def getStringOpt(key: String): Option[String] = getOpt(key, config.getString)

    def getDuration(key: String): FiniteDuration = FiniteDuration(config.getDuration(key).toMillis, MILLISECONDS)

    def getDurationOpt(key: String): Option[FiniteDuration] = getOpt(key, getDuration)

    val StreamPositionLatest = "latest"
    val StreamPositionHorizon = "trim-horizon"
    val StreamPositionTimestamp = "at-timestamp"

    val streamName = config.getString("stream-name")
    val name = config.getString("application-name")
    val latestPos = InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)

    val streamPosition = getStringOpt("position.initial")
      .map {
        case StreamPositionLatest => latestPos
        case StreamPositionHorizon =>
          InitialPositionInStreamExtended.newInitialPosition(
            InitialPositionInStream.TRIM_HORIZON)
        case StreamPositionTimestamp =>
          InitialPositionInStreamExtended.newInitialPositionAtTimestamp(
            DateFormat.getInstance().parse(config.getString("position.time")))
      }
      .getOrElse(latestPos)

    val kinesisClient = Option(kac).getOrElse(KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder()))
    val dynamoClient = Option(dac).getOrElse(DynamoDbAsyncClient.builder.build())
    val cloudWatchClient = Option(cwac).getOrElse(CloudWatchAsyncClient.builder.build())

    ConsumerConfig(
      streamName,
      name,
      generateWorkerId(),
      kinesisClient,
      dynamoClient,
      cloudWatchClient,
      streamPosition
    )
  }

  private def generateWorkerId(): String = UUID.randomUUID().toString
}
