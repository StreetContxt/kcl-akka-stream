package com.contxt.kinesis

import com.amazonaws.AmazonServiceException
import org.scalatest.concurrent.Eventually._
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{DeleteTableRequest, DescribeTableRequest, ProvisionedThroughput, TableStatus, UpdateTableRequest}
import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.awssdk.services.kinesis.model.{CreateStreamRequest, DeleteStreamRequest, DescribeStreamRequest, ScalingType, StreamDescription, UpdateShardCountRequest}

import scala.concurrent.duration._
import scala.util.control.NonFatal

object KinesisResourceManager {
  val TempResourcePrefix = "deleteMe"
  val CreateStreamTimeout: Duration = 60.seconds
  val ReshardStreamTimeout: Duration = 4.minutes
  val WorkerTerminationTimeout: Duration = 30.seconds

  val RegionName: String = Option(System.getenv("KINESIS_TEST_REGION")).get
  val CredentialsProvider: DefaultCredentialsProvider =
    DefaultCredentialsProvider.builder.build()

  def createStream(regionName: String, streamName: String, shardCount: Int): StreamDescription = {
    withKinesisClient(regionName) { client =>
      client.createStream(
        CreateStreamRequest
          .builder()
          .streamName(streamName)
          .shardCount(shardCount)
          .build()
      )
      eventually(timeout(CreateStreamTimeout), interval(2.second)) {
        val description = client
          .describeStream(DescribeStreamRequest.builder().streamName(streamName).build())
          .streamDescription()
        require(description.streamStatusAsString() == "ACTIVE")
        description
      }
    }
  }

  def deleteStream(regionName: String, streamName: String, applicationName: String): Unit = {
    withKinesisClient(regionName) { client =>
      try {
        client.deleteStream(DeleteStreamRequest.builder().streamName(streamName).build())
      } catch {
        case NonFatal(e) => e.printStackTrace()
      }
    }
    deleteDynamoDbTable(applicationName)
  }

  def reshardStream(regionName: String, streamName: String, newActiveShardCount: Int): Unit = {
    withKinesisClient(regionName) { client =>
      val currentShardCount = client
        .describeStream(DescribeStreamRequest.builder().streamName(streamName).build())
        .streamDescription()
        .shards()
        .size
      val expectedShardCount = currentShardCount + newActiveShardCount

      val request = UpdateShardCountRequest
        .builder()
        .streamName(streamName)
        .targetShardCount(newActiveShardCount)
        .scalingType(ScalingType.UNIFORM_SCALING)
        .build()

      client.updateShardCount(request)

      eventually(timeout(ReshardStreamTimeout), interval(2.second)) {
        val shardCount = client
          .describeStream(
            DescribeStreamRequest
              .builder()
              .streamName(streamName)
              .build()
          )
          .streamDescription()
          .shards()
          .size
        require(shardCount == expectedShardCount)
      }
    }
  }

  def withKinesisClient[A](regionName: String)(closure: KinesisClient => A): A = {
    val client = mkKinesisClient(regionName)
    closure(client)
  }

  def withDynamoDbClient[A](closure: DynamoDbClient => A): A = {
    val dynamoDb = DynamoDbClient.builder().build()
    closure(dynamoDb)
  }

  def updateDynamoDbTableWithRate(tableName: String, requestPerSecond: Long): Unit = {
    withDynamoDbClient { dynamoDb =>
      waitForTableToBecomeAvailable(tableName, dynamoDb)
      dynamoDb.updateTable(
        UpdateTableRequest
          .builder()
          .tableName(tableName)
          .provisionedThroughput(
            ProvisionedThroughput
              .builder()
              .readCapacityUnits(requestPerSecond)
              .writeCapacityUnits(requestPerSecond)
              .build()
          )
          .build()
      )
      waitForTableToBecomeAvailable(tableName, dynamoDb)
    }
  }

  private def mkKinesisClient(regionName: String): KinesisClient = {
    KinesisClient
      .builder()
      .region(Region.of(regionName))
      .credentialsProvider(CredentialsProvider)
      .build()
  }

  private def deleteDynamoDbTable(tableName: String): Unit = {
    withDynamoDbClient { dynamoDb =>
      try {
        waitForTableToBecomeAvailable(tableName, dynamoDb)
        dynamoDb.deleteTable(
          DeleteTableRequest
            .builder()
            .tableName(tableName)
            .build()
        )
      } catch {
        case NonFatal(e) => // Ignore.
      }
    }
  }

  private def waitForTableToBecomeAvailable(tableName: String, dynamoDb: DynamoDbClient): Unit = {
    println("Waiting for " + tableName + " to become ACTIVE...")
    val startTime = System.currentTimeMillis
    val endTime = startTime + (10 * 60 * 1000)
    while ({
      System.currentTimeMillis < endTime
    }) {
      Thread.sleep(1000 * 20)
      try {
        val table = dynamoDb
          .describeTable(DescribeTableRequest.builder().tableName(tableName).build())
          .table()
        val tableStatus = table.tableStatus()
        if (tableStatus == TableStatus.ACTIVE) return
      } catch {
        case ase: AmazonServiceException =>
          if (!ase.getErrorCode.equalsIgnoreCase("ResourceNotFoundException"))
            throw ase
      }
    }
    throw new RuntimeException("Table " + tableName + " never went active")
  }
}
