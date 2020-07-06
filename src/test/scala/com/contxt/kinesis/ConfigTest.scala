package com.contxt.kinesis

import com.typesafe.config.ConfigFactory
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.common.InitialPositionInStream

class ConfigTest extends AnyWordSpec with Matchers with MockFactory {

  "ConsumerConfig" should {

    "Use implicit clients" in {
      implicit val kinesis: KinesisAsyncClient = mock[KinesisAsyncClient]
      implicit val dynamo: DynamoDbAsyncClient = mock[DynamoDbAsyncClient]
      implicit val cloudwatch: CloudWatchAsyncClient = mock[CloudWatchAsyncClient]

      def consumerConfig =
        ConsumerConfig(
          streamName = "streamName",
          appName = "applicationName"
        )

      assert(consumerConfig.kinesisClient.eq(kinesis))
      assert(consumerConfig.dynamoClient.eq(dynamo))
      assert(consumerConfig.cloudwatchClient.eq(cloudwatch))
    }

    "Read HOCON configuration" in {
      val config =
        ConfigFactory
          .load()
          .getConfig(
            "com.contxt.kinesis.consumer"
          )

      // verify fromConfig also accepts implicits
      implicit val kinesis: KinesisAsyncClient = mock[KinesisAsyncClient]
      implicit val dynamo: DynamoDbAsyncClient = mock[DynamoDbAsyncClient]
      implicit val cloudwatch: CloudWatchAsyncClient = mock[CloudWatchAsyncClient]

      val consumerConfig = ConsumerConfig.fromConfig(config)

      consumerConfig.streamName shouldBe "unit-test-stream"
      consumerConfig.appName shouldBe "unit-test-app"
      consumerConfig.initialPositionInStreamExtended.getInitialPositionInStream shouldBe InitialPositionInStream.TRIM_HORIZON

      assert(consumerConfig.kinesisClient.eq(kinesis))
      assert(consumerConfig.dynamoClient.eq(dynamo))
      assert(consumerConfig.cloudwatchClient.eq(cloudwatch))
    }

  }

}
