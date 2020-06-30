package com.contxt.kinesis

import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{ActorMaterializer, KillSwitches, UniqueKillSwitch}
import akka.testkit.TestKit
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.processor.ShardRecordProcessorFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Try}

class KinesisSourceFactoryTest
    extends TestKit(ActorSystem("TestSystem"))
    with AnyWordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with Eventually
    with MockFactory {
  override protected def afterAll: Unit = TestKit.shutdownActorSystem(system)

  protected implicit val actorMaterializer: ActorMaterializer = ActorMaterializer()
  private val awaitDuration = 5.seconds

  "KinesisSource" when {
    "creating a worker" should {
      "fail stream if worker creation fails" in {
        val exception = new RuntimeException("TestException1")
        val workerFactory: (ShardRecordProcessorFactory, ConsumerConfig) => ManagedWorker = { (_, _) =>
          throw exception
        }
        val source = KinesisSource(workerFactory, clientConfig, shardCheckpointConfig, new NoopConsumerStats)

        val (workerTerminated, streamTerminated) = source
          .toMat(Sink.ignore)(Keep.both)
          .run()

        eventually {
          Try(Await.result(streamTerminated, awaitDuration)) shouldBe Failure(exception)
          Try(Await.result(workerTerminated, awaitDuration)) shouldBe Failure(exception)
        }
      }

      "fail stream if worker.run() fails" in {
        val mockWorker = new MockWorker
        val (workerTerminated, killSwitch, streamTerminated) = runSource(mockWorker)
        val exception = new RuntimeException("TestException2")
        mockWorker.runControl.failure(exception)
        mockWorker.shutdownControl.success(Done)
        eventually {
          Try(Await.result(streamTerminated, awaitDuration)) shouldBe Failure(exception)
          Await.result(workerTerminated, awaitDuration) shouldBe Done
        }
      }

      "fail stream if worker.run() exits" in {
        val mockWorker = new MockWorker
        val (workerTerminated, killSwitch, streamTerminated) = runSource(mockWorker)
        mockWorker.runControl.success(Done)
        mockWorker.shutdownControl.success(Done)
        eventually {
          Try(Await.result(streamTerminated, awaitDuration)).isFailure shouldBe true
          Await.result(workerTerminated, awaitDuration) shouldBe Done
        }
      }

      "stop the worker if the source is cancelled" in {
        val mockWorker = new MockWorker
        val (workerTerminated, killSwitch, streamTerminated) = runSource(mockWorker)
        killSwitch.shutdown()
        mockWorker.shutdownControl.success(Done)
        eventually {
          Await.result(streamTerminated, awaitDuration) shouldBe Done
          Await.result(workerTerminated, awaitDuration) shouldBe Done
        }
      }

      "propagate failures on worker shutdown" in {
        val mockWorker = new MockWorker
        val (workerTerminated, killSwitch, streamTerminated) = runSource(mockWorker)
        val exception = new RuntimeException("TestException3")
        killSwitch.shutdown()
        mockWorker.shutdownControl.failure(exception)
        eventually {
          Await.result(streamTerminated, awaitDuration) shouldBe Done
          Try(Await.result(workerTerminated, awaitDuration)) shouldBe Failure(exception)
        }
      }
    }
  }

  private def runSource(mockWorker: MockWorker): (Future[Done], UniqueKillSwitch, Future[Done]) = {
    val ((workerTerminated, killSwitch), streamTerminated) = mkSource(mockWorker)
      .viaMat(KillSwitches.single)(Keep.both)
      .toMat(Sink.ignore)(Keep.both)
      .run()

    (workerTerminated, killSwitch, streamTerminated)
  }

  private def mkSource(mockWorker: MockWorker): Source[KinesisRecord, Future[Done]] = {
    val workerFactory: (ShardRecordProcessorFactory, ConsumerConfig) => ManagedWorker = { (_, _) =>
      mockWorker
    }
    KinesisSource(workerFactory, clientConfig, shardCheckpointConfig, new NoopConsumerStats)
  }

  private val shardCheckpointConfig = ShardCheckpointConfig(
    checkpointPeriod = 1.minutes,
    checkpointAfterProcessingNrOfRecords = 10000,
    maxWaitForCompletionOnStreamShutdown = 4.seconds
  )

  private def clientConfig =
    new ConsumerConfig(
      streamName = "streamName1",
      appName = "applicationName1",
      workerId = "workerId",
      kinesisClient = mock[KinesisAsyncClient],
      dynamoClient = mock[DynamoDbAsyncClient],
      cloudwatchClient = mock[CloudWatchAsyncClient]
    )

  class MockWorker extends ManagedWorker {
    val runControl: Promise[Done] = Promise[Done]()
    val shutdownControl: Promise[Done] = Promise[Done]()

    override def run(): Unit = Await.result(runControl.future, Duration.Inf)

    override def shutdownAndWait(): Unit =
      Await.result(shutdownControl.future, Duration.Inf)
  }
}
