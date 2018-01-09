package com.contxt.kinesis

import akka.Done
import akka.actor.ActorSystem
import akka.stream.{ ActorMaterializer, KillSwitches, UniqueKillSwitch }
import akka.stream.scaladsl.{ Keep, Sink, Source }
import akka.testkit.TestKit
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import org.scalatest.concurrent.Eventually
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }
import scala.concurrent.{ Await, Future, Promise }
import scala.concurrent.duration._
import scala.util.{ Failure, Try }

class KinesisSourceFactoryTest
  extends TestKit(ActorSystem("TestSystem"))
  with WordSpecLike with BeforeAndAfterAll with Matchers with Eventually
{
  override protected def afterAll: Unit = TestKit.shutdownActorSystem(system)
  protected implicit val materializer: ActorMaterializer = ActorMaterializer()
  private val awaitDuration = 5.seconds

  "KinesisSource" when {
    "creating a stream" should {
      "require `CallProcessRecordsEvenForEmptyRecordList` to be true" in {
        an[IllegalArgumentException] shouldBe thrownBy {
          val badConfig = clientConfig.withCallProcessRecordsEvenForEmptyRecordList(false)
          KinesisSource(badConfig)
        }
        KinesisSource(clientConfig) // Check that config is good otherwise.
      }
    }

    "creating a worker" should {
      "fail stream if worker creation fails" in {
        val exception = new RuntimeException("TestException1")
        val workerFactory: (IRecordProcessorFactory, KinesisClientLibConfiguration) => ManagedWorker = { (_, _) =>
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
    val workerFactory: (IRecordProcessorFactory, KinesisClientLibConfiguration) => ManagedWorker = { (_, _) =>
      mockWorker
    }
    KinesisSource(workerFactory, clientConfig, shardCheckpointConfig, new NoopConsumerStats)
  }

  private val shardCheckpointConfig = ShardCheckpointConfig(
    checkpointPeriod = 1.minutes,
    checkpointAfterCompletingNrOfRecords = 10000,
    maxWaitForCompletionOnStreamShutdown = 4.seconds
  )

  private def clientConfig = {
    new KinesisClientLibConfiguration(
      "applicationName1",
      "streamName1",
      new DefaultAWSCredentialsProviderChain(),
      "workerId1"
    )
      .withCallProcessRecordsEvenForEmptyRecordList(true)
  }

  class MockWorker extends ManagedWorker {
    val runControl: Promise[Done] = Promise[Done]()
    val shutdownControl: Promise[Done] = Promise[Done]()
    override def run(): Unit = Await.result(runControl.future, Duration.Inf)
    override def shutdownAndWait(): Unit = Await.result(shutdownControl.future, Duration.Inf)
  }
}
