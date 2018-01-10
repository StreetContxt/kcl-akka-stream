package com.contxt.kinesis

import akka.NotUsed
import akka.stream.{ ActorMaterializer, KillSwitches, ThrottleMode, UniqueKillSwitch }
import akka.stream.scaladsl.{ Flow, Keep, Merge, RunnableGraph, Sink, Source }
import org.scalatest.Tag
import org.scalatest.concurrent.Eventually._
import scala.concurrent.{ Await, Future, Promise }
import scala.concurrent.duration._
import scala.util.Try

trait KinesisTestComponents {
  object ThrottledByCheckpoint extends Tag("ThrottledByCheckpoint")

  type KeyAndMessage = (String, String)
  protected implicit val patienceConfig: PatienceConfig
  protected implicit val materializer: ActorMaterializer

  protected val bootstrapKeyPrefix = "bootstrap"

  protected def buildConfig(tags: Set[String]): TestStreamConfig = {
    tags.foldLeft(TestStreamConfig.fromRandomUuid()) {
      case (currentConfig, ThrottledByCheckpoint.name) =>
        currentConfig.copy(
          checkpointAfterCompletingNrOfRecords = 1,
          idleTimeBetweenGetRecords = 1.millis
        )

      case (currentConfig, _) =>
        currentConfig
    }
  }

  protected def messageSource(
    keyCount: Int, messageIntervalPerKey: FiniteDuration, keyPrefix: String = "key"
  ): Source[KeyAndMessage, NotUsed] = {
    require(keyCount >= 2)
    def mkKey(i: Int) = f"${keyPrefix}_$i%03d"
    def sourceForKey(key: String): Source[KeyAndMessage, NotUsed] = {
      def mkMessage(i: Int) = key -> f"msg_$i%03d"
      Source
        .fromIterator(() => Iterator.from(1).map(mkMessage))
        .throttle(elements = 1, per = messageIntervalPerKey, maximumBurst = 1, mode = ThrottleMode.shaping)
    }
    val sources = (1 to keyCount).map(mkKey).map(sourceForKey)
    Source.combine(sources(0), sources(1), sources.drop(2): _*)(strategy = Merge(_))
  }

  protected def producerSink[A](implicit config: TestStreamConfig): Sink[KeyAndMessage, Future[Seq[KeyAndMessage]]] = {
    KinesisTestProducer.sink(config.streamName, config.kplConfig)
  }

  protected def withConsumerSource[A](workerId: String)(
    closure: (Source[KinesisRecord, NotUsed], InspectableConsumerStats) => A
  )(implicit config: TestStreamConfig): A = {
    val consumerStats = new InspectableConsumerStats
    val (consumerSource, materializationFuture) = liftMaterializedValue {
      KinesisSource(
        KinesisSource.createKclWorker,
        config.kclConfig(workerId),
        config.shardCheckpointConfig,
        consumerStats
      )
        .viaMat(KillSwitches.single)(Keep.both)
    }
    val closureResult = Try(closure(consumerSource, consumerStats))
    Try { // Always keep the original test exception, and try to shutdown cleanly if possible.
      val (workerTerminationFuture, killSwitch) = Await.result(materializationFuture, 0.second)
      killSwitch.shutdown()
      Await.ready(workerTerminationFuture, KinesisResourceManager.WorkerTerminationTimeout)
    }
    closureResult.get
  }

  protected def runKinesisSourceWithInspection(
    kinesisSource: Source[KinesisRecord, NotUsed]
  ): () => IndexedSeq[KeyAndMessage] = {
    kinesisSource
      .via(markRecordsAsProcessed)
      .via(extractKeyAndMessage)
      .runWith(Inspectable.sink)
  }

  protected def extractKeyAndMessage: Flow[KinesisRecord, KeyAndMessage, NotUsed] = {
    Flow[KinesisRecord]
      .map { record =>
        (record.partitionKey, record.data.utf8String)
      }
  }

  protected def markRecordsAsProcessed: Flow[KinesisRecord, KinesisRecord, NotUsed] = {
    Flow[KinesisRecord]
      .map { record =>
        record.markProcessed()
        record
      }
  }

  protected def bootstrapProducer(keyCount: Int)(implicit config: TestStreamConfig): RunnableGraph[UniqueKillSwitch] = {
    messageSource(keyCount, messageIntervalPerKey = 1.second, bootstrapKeyPrefix)
      .viaMat(KillSwitches.single)(Keep.right)
      .to(producerSink)
  }

  protected def filterBootstrapMessages: Flow[KinesisRecord, KinesisRecord, NotUsed] = {
    Flow[KinesisRecord]
      .filter { record =>
        if (record.partitionKey.startsWith(bootstrapKeyPrefix)) {
          record.markProcessed()
          false
        }
        else true
      }
  }

  private def liftMaterializedValue[A, Mat](source: Source[A, Mat]): (Source[A, NotUsed], Future[Mat]) = {
    val promise = Promise[Mat]
    val sourceWithoutMat = source.mapMaterializedValue { mat =>
      promise.trySuccess(mat)
      NotUsed
    }
    (sourceWithoutMat, promise.future)
  }
}
