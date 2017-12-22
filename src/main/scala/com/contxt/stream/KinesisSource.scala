package com.contxt.stream

import akka.stream._
import akka.stream.scaladsl._
import akka.{ Done, NotUsed }
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{ IRecordProcessor, IRecordProcessorFactory }
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{ KinesisClientLibConfiguration, Worker }
import java.lang.Thread.UncaughtExceptionHandler
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ Executors, ThreadFactory }
import org.slf4j.LoggerFactory
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.Try
import scala.util.control.NonFatal

/** Kinesis's failover and loadbalancing behaviours do not guarantee mutually exclusive processing of shards.
  * See [[http://docs.aws.amazon.com/streams/latest/dev/troubleshooting-consumers.html Kinesis Troubleshooting]]
  * for details as well as an additional list of limitations otherwise not covered by Kinesis documentation.
  *
  * If you require mutually exclusive processing, or want to avoid spurious errors caused by concurrent processing of
  * messages with the same key by different nodes, check out Kafka.
  *
  * On the producer side, Kinesis does not guarantee sent message order without excessively slowing things down, see
  * [[https://github.com/awslabs/amazon-kinesis-producer/issues/23 this ticket]] for more details.
  *
  * If you require efficient messages transfer while maintaining order, check out Kafka.
  */
object KinesisSource {

  /** Creates a Source backed by Kinesis Consumer Library, with materialized valued of Future[Done] which completes
    * when the Kinesis worker has fully shutdown. */
  def apply(
    kclConfig: KinesisClientLibConfiguration,
    shardCheckpointConfig: ShardCheckpointConfig
  )(implicit materializer: ActorMaterializer): Source[KinesisRecord, Future[Done]] = {
    KinesisSource(createKclWorker, kclConfig, shardCheckpointConfig, new CheckpointLog)
  }

  private[stream] def apply(
    workerFactory: (IRecordProcessorFactory, KinesisClientLibConfiguration) => Worker,
    kclConfig: KinesisClientLibConfiguration,
    shardCheckpointConfig: ShardCheckpointConfig,
    checkpointLog: CheckpointLog
  )(implicit materializer: ActorMaterializer): Source[KinesisRecord, Future[Done]] = {
    require(
      kclConfig.shouldCallProcessRecordsEvenForEmptyRecordList,
      "`kclConfig.shouldCallProcessRecordsEvenForEmptyRecordList` must be set to `true`."
    )
    val kinesisStreamId = KinesisStreamId(
      kclConfig.getRegionName, kclConfig.getStreamName, kclConfig.getApplicationName
    )
    MergeHub
      .source[IndexedSeq[KinesisRecord]](perProducerBufferSize = 1)
      .viaMat(KillSwitches.single)(Keep.both)
      .watchTermination()(Keep.both)
      .mapMaterializedValue { case ((mergeSink, streamKillSwitch), terminationFuture) =>
        val processorFactory = new RecordProcessorFactoryImpl(
          kinesisStreamId,
          streamKillSwitch, terminationFuture,
          mergeSink,
          shardCheckpointConfig, checkpointLog
        )
        createAndStartKclWorker(workerFactory, processorFactory, kclConfig, streamKillSwitch, terminationFuture)
      }
      .mapConcat(_.toIndexedSeq)
  }

  private[stream] def createKclWorker(
    recordProcessorFactory: IRecordProcessorFactory,
    kclConfig: KinesisClientLibConfiguration
  ): Worker = {
    new Worker.Builder()
      .recordProcessorFactory(recordProcessorFactory)
      .config(kclConfig)
      .build()
  }

  private def createAndStartKclWorker(
    workerFactory: (IRecordProcessorFactory, KinesisClientLibConfiguration) => Worker,
    recordProcessorFactory: IRecordProcessorFactory,
    kclConfig: KinesisClientLibConfiguration,
    streamKillSwitch: KillSwitch,
    streamTerminationFuture: Future[Done]
  ): Future[Done] = {
    implicit val blockingContext: ExecutionContext = BlockingContext.KinesisWorkersSharedContext
    val workerShutdownPromise = Promise[Done]
    Future {
      try {
        val worker = Try(workerFactory(recordProcessorFactory, kclConfig))
        streamTerminationFuture.onComplete { _ =>
          val workerShutdownFuture = Future(worker.get.startGracefulShutdown().get).map(_ => Done)
          workerShutdownPromise.completeWith(workerShutdownFuture)
        }
        worker.get.run() // This call hijacks the thread.
      }
      catch {
        case NonFatal(e) => streamKillSwitch.abort(e)
      }
      streamKillSwitch.abort(new IllegalStateException("Worker shutdown unexpectedly."))
    }
    workerShutdownPromise.future
  }
}

private[stream] class RecordProcessorFactoryImpl(
  kinesisStreamId: KinesisStreamId,
  streamKillSwitch: KillSwitch,
  terminationFuture: Future[Done],
  mergeSink: Sink[IndexedSeq[KinesisRecord], NotUsed],
  shardCheckpointConfig: ShardCheckpointConfig,
  checkpointLog: CheckpointLog
)(implicit materializer: ActorMaterializer) extends IRecordProcessorFactory {
  override def createProcessor(): IRecordProcessor = {
    val queue = Source
      .queue[IndexedSeq[KinesisRecord]](bufferSize = 0, OverflowStrategy.backpressure)
      .to(mergeSink)
      .run()

    new RecordProcessorImpl(
      kinesisStreamId,
      streamKillSwitch, terminationFuture,
      queue,
      shardCheckpointConfig, checkpointLog
    )
  }
}

private[stream] object BlockingContext {
  private val log = LoggerFactory.getLogger(getClass)
  private val threadId = new AtomicInteger(1)

  lazy val KinesisWorkersSharedContext = BlockingContext("KinesisSourceWorker")

  private def apply(name: String): ExecutionContext = {
    val threadFactory = new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val thread = Executors.defaultThreadFactory().newThread(r)
        thread.setName(nextThreadName(name))
        thread.setUncaughtExceptionHandler(uncaughtExceptionHandler)
        thread.setDaemon(true)
        thread
      }
    }
    ExecutionContext.fromExecutor(Executors.newCachedThreadPool(threadFactory))
  }

  private def nextThreadName(prefix: String): String = {
    f"{prefix}_${threadId.getAndIncrement()}%04d"
  }

  private val uncaughtExceptionHandler = new UncaughtExceptionHandler {
    override def uncaughtException(t: Thread, e: Throwable): Unit = {
      e match {
        case NonFatal(e) =>
          log.error(s"Uncaught exception in thread `${t.getName}`.", e)

        case _ =>
          log.error(s"Fatal error in thread `${t.getName}`, exiting VM.", e)
          System.exit(-1)
      }
    }
  }
}
