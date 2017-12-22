package com.contxt.stream

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.testkit.TestKit
import org.scalatest._
import scala.concurrent.duration._
import org.scalatest.concurrent.Eventually._
import org.scalatest.time._
import com.contxt.stream.MessageUtil._
import org.slf4j.LoggerFactory
import scala.concurrent.Await
import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

class KinesisSourceTest
  extends TestKit(ActorSystem("TestSystem"))
    with fixture.WordSpecLike with BeforeAndAfterAll with Matchers with KinesisTestComponents
{
  private val log = LoggerFactory.getLogger(getClass)
  implicit val patienceConfig: PatienceConfig = PatienceConfig(scaled(Span(120, Seconds)), scaled(Span(4, Seconds)))
  override protected def afterAll: Unit = TestKit.shutdownActorSystem(system)
  protected implicit val materializer: ActorMaterializer = ActorMaterializer()

  private val initialShardCount = 4
  private val halfShardCount = initialShardCount / 2
  private val doubleShardCount = initialShardCount * 2
  require(initialShardCount / 2 * 2 == initialShardCount)

  override type FixtureParam = TestStreamConfig

  override protected def withFixture(test: OneArgTest): Outcome = {
    val config = buildConfig(test.tags)
    try {
      KinesisResourceManager.createStream(config.regionName, config.streamName, initialShardCount)
      val result = test(config)
      if (result.isFailed) Try(dumpStream(config))
      result
    }
    finally {
      KinesisResourceManager.deleteStream(config.regionName, config.streamName, config.applicationName)
    }
  }

  "KinesisSource" when {
    "running a single consumer" should {
      "process all the sent messages" in { implicit config =>
        val sentFuture = messageSource(keyCount = 100, messageIntervalPerKey = 50.millis).take(1000)
          .runWith(producerSink)

        withConsumerSource("singleConsumer") { (consumerSource, _) =>
          val inspectReceived = runKinesisSourceWithInspection(consumerSource)

          eventually {
            val sentMessages = Await.result(sentFuture, 0.seconds)
            val receivedMessages = inspectReceived()
            groupByKey(receivedMessages) shouldEqual groupByKey(sentMessages)
          }
        }
      }
    }

    "a consumer is not checkpointing" should {
      "reprocess messages after the bad consumer shuts down" in { implicit config =>
        val minUncommitedRecordsBeforeBadConsumerShutdown = 500
        val (producerKillSwitch, sentFuture) = messageSource(keyCount = 100, messageIntervalPerKey = 200.millis)
          .viaMat(KillSwitches.single)(Keep.right)
          .toMat(producerSink)(Keep.both)
          .run()

        withConsumerSource("goodConsumer") { (consumerSource1, checkpointLog1) =>
          val inspectReceived1 = runKinesisSourceWithInspection(consumerSource1)

          withConsumerSource("borkenConsumer") { (consumerSource2, _) =>
            val inspectReceived2 = consumerSource2
              .via(extractKeyAndMessage)
              .runWith(Inspectable.sink)

            eventually(require(inspectReceived2().size > minUncommitedRecordsBeforeBadConsumerShutdown))
            checkpointLog1.waitForAtLeastOneCheckpointPerShard(halfShardCount)
          }

          checkpointLog1.waitForAtLeastOneCheckpointPerShard(initialShardCount)
          producerKillSwitch.shutdown()

          eventually {
            val sentMessages = Await.result(sentFuture, 0.seconds)
            dedupAndGroupByKey(inspectReceived1()) shouldEqual groupByKey(sentMessages)
          }
        }
      }
    }

    "rebalancing to more consumers" should {
      "process all the sent messages" in { implicit config =>
        val (producerKillSwitch, sentFuture) = messageSource(keyCount = 100, messageIntervalPerKey = 200.millis)
          .viaMat(KillSwitches.single)(Keep.right)
          .toMat(producerSink)(Keep.both)
          .run()

        withConsumerSource("consumer1") { (consumerSource1, checkpointLog1) =>
          val inspectReceived1 = runKinesisSourceWithInspection(consumerSource1)
          checkpointLog1.waitForAtLeastOneCheckpointPerShard(initialShardCount)

          // Kicking off another consumer source will trigger rebalancing.
          withConsumerSource("consumer2") { (consumerSource2, checkpointLog2) =>
            val inspectReceived2 = runKinesisSourceWithInspection(consumerSource2)
            checkpointLog2.waitForAtLeastOneCheckpointPerShard(halfShardCount)
            producerKillSwitch.shutdown()

            eventually {
              val sentMessages = Await.result(sentFuture, 0.seconds)
              dedupAndGroupByKey(inspectReceived1() ++ inspectReceived2()) shouldEqual groupByKey(sentMessages)
            }

            assertRebalancingTestConditions(inspectReceived1(), inspectReceived2())
          }
        }
      }
    }

    "rebalancing to fewer consumers" should {
      "process all the sent messages" in { implicit config =>
        val keyCount = 100
        val keysPerConsumerForSuccessfulWarmup = keyCount / 2 - 10

        val bootstrapProducerKillSwitch = bootstrapProducer(keyCount).run()

        withConsumerSource("consumer1") { (consumerSource1, checkpointLog1) =>
          val inspectReceived1 = runKinesisSourceWithInspection(consumerSource1.via(filterBootstrapMessages))

          val consumer2ClosureResult = withConsumerSource("consumer2") { (consumerSource2, checkpointLog2) =>
            val inspectReceived2 = runKinesisSourceWithInspection(consumerSource2.via(filterBootstrapMessages))

            // Wait for both consumers to start and divide up the shards.
            checkpointLog1.waitForAtLeastOneCheckpointPerShard(halfShardCount)
            checkpointLog2.waitForAtLeastOneCheckpointPerShard(halfShardCount)
            bootstrapProducerKillSwitch.shutdown()

            val (producerKillSwitch, sentFuture) =
              messageSource(keyCount, messageIntervalPerKey = 200.millis)
                .viaMat(KillSwitches.single)(Keep.right)
                .toMat(producerSink)(Keep.both)
                .run()

            // Wait for data from producer2 to show up in both consumers.
            eventually {
              val keysFromConsumer1 = groupByKey(inspectReceived1()).keySet.size
              require(keysFromConsumer1 >= keysPerConsumerForSuccessfulWarmup)
              val keysFromConsumer2 = groupByKey(inspectReceived2()).keySet.size
              require(keysFromConsumer2 >= keysPerConsumerForSuccessfulWarmup)
            }
            checkpointLog1.waitForAtLeastOneCheckpointPerShard(halfShardCount)
            checkpointLog2.waitForAtLeastOneCheckpointPerShard(halfShardCount)

            (inspectReceived2, producerKillSwitch, sentFuture)
          }
          val (inspectReceived2, producerKillSwitch, sentFuture) = consumer2ClosureResult

          checkpointLog1.waitForAtLeastOneCheckpointPerShard(initialShardCount)
          producerKillSwitch.shutdown()

          eventually {
            val sentByProducer2 = Await.result(sentFuture, 0.seconds)
            dedupAndGroupByKey(inspectReceived2() ++ inspectReceived1()) shouldEqual groupByKey(sentByProducer2)
          }

          assertRebalancingTestConditions(inspectReceived1(), inspectReceived2())
        }
      }
    }

    "scaling the number of shards up" should {
      "process all the sent messages" in { implicit config =>
        val (producerKillSwitch, sentFuture) = messageSource(keyCount = 100, messageIntervalPerKey = 200.millis)
          .viaMat(KillSwitches.single)(Keep.right)
          .toMat(producerSink)(Keep.both)
          .run()

        withConsumerSource("singleConsumer") { (consumerSource, checkpointLog) =>
          val inspectReceived = runKinesisSourceWithInspection(consumerSource)
          checkpointLog.waitForAtLeastOneCheckpointPerShard(initialShardCount)

          KinesisResourceManager.reshardStream(config.regionName, config.streamName, doubleShardCount)
          checkpointLog.waitForAtLeastOneCheckpointPerShard(doubleShardCount)
          producerKillSwitch.shutdown()

          eventually {
            val sentMessages = Await.result(sentFuture, 0.seconds)
            val receivedMessages = inspectReceived()
            dedupAndGroupByKey(receivedMessages) shouldEqual groupByKey(sentMessages)
          }
        }
      }
    }

    "scaling the number of shards down" should {
      "process all the sent messages" in { implicit config =>
        val (producerKillSwitch, sentFuture) = messageSource(keyCount = 100, messageIntervalPerKey = 200.millis)
          .viaMat(KillSwitches.single)(Keep.right)
          .toMat(producerSink)(Keep.both)
          .run()

        withConsumerSource("singleConsumer") { (consumerSource, checkpointLog) =>
          val inspectReceived = runKinesisSourceWithInspection(consumerSource)
          checkpointLog.waitForAtLeastOneCheckpointPerShard(initialShardCount)

          KinesisResourceManager.reshardStream(config.regionName, config.streamName, halfShardCount)
          checkpointLog.waitForAtLeastOneCheckpointPerShard(halfShardCount)
          producerKillSwitch.shutdown()

          eventually {
            val sentMessages = Await.result(sentFuture, 0.seconds)
            val receivedMessages = inspectReceived()
            dedupAndGroupByKey(receivedMessages) shouldEqual groupByKey(sentMessages)
          }
        }
      }
    }

    "getting throttled during checkpoint requests" should {
      "survive and process all the sent messages" taggedAs ThrottledByCheckpoint in { implicit config =>
        implicit val patienceConfig = PatienceConfig(scaled(Span(240, Seconds)), scaled(Span(2, Seconds)))
        val targetShardCount = 8
        KinesisResourceManager.reshardStream(config.regionName, config.streamName, targetShardCount)

        val (producerKillSwitch, sentFuture) = messageSource(keyCount = 100, messageIntervalPerKey = 400.millis)
          .viaMat(KillSwitches.single)(Keep.right)
          .toMat(producerSink)(Keep.both)
          .run()

        withConsumerSource("singleConsumer") { (consumerSource, checkpointLog) =>
          val inspectReceived = runKinesisSourceWithInspection(consumerSource)

          eventually(require(inspectReceived().nonEmpty))
          KinesisResourceManager.updateDynamoDbTableWithRate(config.applicationName, requetsPerSecond = 1)

          checkpointLog.waitForAtLeastOneCheckpointPerShard(targetShardCount)
          checkpointLog.waitForNrOfThrottledCheckpoints(5)
          producerKillSwitch.shutdown()

          eventually {
            val sentMessages = Await.result(sentFuture, 0.seconds)
            dedupAndGroupByKey(inspectReceived()) shouldEqual groupByKey(sentMessages)
          }
        }
      }
    }
  }

  private def assertRebalancingTestConditions(
    receivedByConsumer1: IndexedSeq[KeyAndMessage],
    receivedByConsumer2: IndexedSeq[KeyAndMessage]
  ): Unit = {
    val receivedByConsumer1Only = receivedByConsumer1.toSet -- receivedByConsumer2.toSet
    val receivedByConsumer2Only = receivedByConsumer2.toSet -- receivedByConsumer1.toSet
    val receivedByBoth = receivedByConsumer1.toSet.intersect(receivedByConsumer2.toSet)

    receivedByConsumer1Only should not be empty
    receivedByConsumer2Only should not be empty
  }

  private def dumpStream(config: TestStreamConfig): Unit = {
    implicit val dumpConfig = config.copy(applicationName = s"${config.applicationName}_streamDump")
    withConsumerSource("dumpConsumer") { (kinesisSource, _) =>
      val inspectReceived = kinesisSource
        .via(extractKeyAndMessage)
        .runWith(Inspectable.sink)

      val result = Try {
        var received = IndexedSeq.empty[KeyAndMessage]
        eventually {
          val newReceived = inspectReceived()
          if (newReceived.isEmpty || newReceived != received) {
            received = newReceived
            throw new RuntimeException(s"Still dumping the stream.")
          }
        }
        received
      }
      result match {
        case Success(messages) =>
          log.info(s"Stream ${dumpConfig.streamName} dump: \n${messages.mkString(",")}\n")

        case Failure(e) =>
          log.error(s"Could not dump the stream ${dumpConfig.streamName}.", e)
      }
    }
  }
}
