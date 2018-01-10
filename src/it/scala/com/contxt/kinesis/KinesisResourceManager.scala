package com.contxt.kinesis

import com.amazonaws.PredefinedClientConfigurations
import com.amazonaws.auth.{ AWSCredentialsProviderChain, DefaultAWSCredentialsProviderChain }
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput
import com.amazonaws.services.kinesis.model.{ ScalingType, StreamDescription, UpdateShardCountRequest }
import com.amazonaws.services.kinesis.{ AmazonKinesis, AmazonKinesisClientBuilder }
import org.scalatest.concurrent.Eventually._
import scala.concurrent.duration._
import scala.util.control.NonFatal

object KinesisResourceManager {
  val TempResourcePrefix = "deleteMe"
  val CreateStreamTimeout: Duration = 60.seconds
  val ReshardStreamTimeout: Duration = 4.minutes
  val WorkerTerminationTimeout: Duration = 30.seconds

  val RegionName: String = Option(System.getenv("KINESIS_TEST_REGION")).get
  val CredentialsProvider: AWSCredentialsProviderChain = new DefaultAWSCredentialsProviderChain()

  def createStream(regionName: String, streamName: String, shardCount: Int): StreamDescription = {
    withKinesisClient(regionName) { client =>
      client.createStream(streamName, shardCount)
      eventually(timeout(CreateStreamTimeout), interval(2.second)) {
        val description = client.describeStream(streamName).getStreamDescription
        require(description.getStreamStatus == "ACTIVE")
        description
      }
    }
  }

  def deleteStream(regionName: String, streamName: String, applicationName: String): Unit = {
    withKinesisClient(regionName) { client =>
      try {
        client.deleteStream(streamName)
      }
      catch {
        case NonFatal(e) => e.printStackTrace()
      }
    }
    deleteDynamoDbTable(applicationName)
  }

  def reshardStream(regionName: String, streamName: String, newActiveShardCount: Int): Unit = {
    withKinesisClient(regionName) { client =>
      val currentShardCount = client.describeStream(streamName).getStreamDescription.getShards.size
      val expectedShardCount = currentShardCount + newActiveShardCount

      val request = new UpdateShardCountRequest()
        .withStreamName(streamName)
        .withTargetShardCount(newActiveShardCount)
        .withScalingType(ScalingType.UNIFORM_SCALING)

      client.updateShardCount(request)

      eventually(timeout(ReshardStreamTimeout), interval(2.second)) {
        val shardCount = client.describeStream(streamName).getStreamDescription.getShards.size
        require(shardCount == expectedShardCount)
      }
    }
  }

  def withKinesisClient[A](regionName: String)(closure: AmazonKinesis => A): A = {
    val client = mkKinesisClient(regionName)
    try {
      closure(client)
    }
    finally {
      client.shutdown()
    }
  }

  def withDynamoDbClient[A](closure: DynamoDB => A): A = {
    val client = AmazonDynamoDBClientBuilder.standard().build()
    val dynamoDb = new DynamoDB(client)
    try {
      closure(dynamoDb)
    }
    finally {
      dynamoDb.shutdown()
    }
  }

  def updateDynamoDbTableWithRate(tableName: String, requetsPerSecond: Long): Unit = {
    withDynamoDbClient { dynamoDb =>
      val table = dynamoDb.getTable(tableName)
      table.waitForActive()
      table.updateTable(new ProvisionedThroughput(requetsPerSecond, requetsPerSecond))
      table.waitForActive()
    }
  }

  private def mkKinesisClient(regionName: String): AmazonKinesis = {
    val clientBuilder = AmazonKinesisClientBuilder.standard()
    clientBuilder.setRegion(regionName)
    clientBuilder.setCredentials(CredentialsProvider)
    clientBuilder.setClientConfiguration(PredefinedClientConfigurations.defaultConfig())
    clientBuilder.build()
  }

  private def deleteDynamoDbTable(tableName: String): Unit = {
    withDynamoDbClient { dynamoDb =>
      try {
        val table = dynamoDb.getTable(tableName)
        table.waitForActive()
        table.delete()
        table.waitForDelete()
      }
      catch {
        case NonFatal(e) => // Ignore.
      }
    }
  }
}
