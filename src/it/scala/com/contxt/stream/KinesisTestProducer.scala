package com.contxt.stream

import akka.NotUsed
import akka.stream.scaladsl._
import com.amazonaws.services.kinesis.producer.{ KinesisProducer, KinesisProducerConfiguration, UserRecordResult }
import com.google.common.util.concurrent.ListenableFuture
import java.nio.ByteBuffer
import scala.concurrent.{ blocking, ExecutionContextExecutor, Future, Promise }
import scala.language.implicitConversions
import scala.util.Try

class KinesisTestProducer(streamName: String, producer: KinesisProducer) {
  def send(key: String, message: String): Future[UserRecordResult] = {
    val messageData = ByteBuffer.wrap(message.getBytes("UTF-8"))
    producer.addUserRecord(streamName, key, messageData)
  }

  def flushAndWait(): Unit = {
    producer.flushSync()
  }

  def shutdown(): Unit = {
    flushAndWait()
    producer.destroy()
  }

  private implicit def listenableToScalaFuture[A](listenable: ListenableFuture[A]): Future[A] = {
    implicit val executionContext: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
    val promise = Promise[A]
    val callback = new Runnable {
      override def run(): Unit = promise.tryComplete(Try(listenable.get()))
    }
    listenable.addListener(callback, executionContext)
    promise.future
  }
}

object KinesisTestProducer {
  def apply(producerConfig: KinesisProducerConfiguration, streamName: String): KinesisTestProducer = {
    val producer = new KinesisProducer(producerConfig)
    new KinesisTestProducer(streamName, producer)
  }

  def sink(
    producerConfig: KinesisProducerConfiguration, streamName: String
  ): Sink[(String, String), Future[Seq[(String, String)]]] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val producer = KinesisTestProducer(producerConfig, streamName)

    Flow[(String, String)]
      .groupBy(maxSubstreams = Int.MaxValue, { case (key, message) => key })
      .detach
      // `parallelism = 1` enforces message ordering, which is good for testing, but too slow for normal use.
      .mapAsync(parallelism = 1){ case keyMessage @ (key, message) =>
        producer
          .send(key, message)
          .map(_ => keyMessage)
      }
      .mergeSubstreams
      .watchTermination()(Keep.right)
      .mapMaterializedValue { terminationFuture =>
        terminationFuture.onComplete(_ => blocking(producer.shutdown()))
        NotUsed
      }
      .toMat(Sink.seq)(Keep.right)
  }
}
