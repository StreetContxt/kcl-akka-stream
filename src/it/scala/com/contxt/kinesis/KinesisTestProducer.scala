package com.contxt.kinesis

import java.nio.ByteBuffer

import akka.NotUsed
import akka.stream.scaladsl._
import com.amazonaws.services.kinesis.producer.{KinesisProducerConfiguration, UserRecordResult}

import scala.concurrent.Future
import scala.language.implicitConversions

class KinesisTestProducer(producer: ScalaKinesisProducer) {
  def send(key: String, message: String): Future[UserRecordResult] = {
    val messageData = ByteBuffer.wrap(message.getBytes("UTF-8"))
    producer.send(key, messageData)
  }

  def shutdown(): Future[Unit] = {
    producer.shutdown()
  }
}

object KinesisTestProducer {
  def apply(streamName: String, producerConfig: KinesisProducerConfiguration): KinesisTestProducer = {
    val producer = ScalaKinesisProducer(streamName, producerConfig)
    new KinesisTestProducer(producer)
  }

  def sink(
    streamName: String,
    producerConfig: KinesisProducerConfiguration
  ): Sink[(String, String), Future[Seq[(String, String)]]] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val producer = KinesisTestProducer(streamName, producerConfig)

    Flow[(String, String)]
      .groupBy(maxSubstreams = Int.MaxValue, { case (key, message) => key })
      .detach
      // `parallelism = 1` enforces message ordering, which is good for testing, but too slow for normal use.
      .mapAsync(parallelism = 1) {
        case keyMessage @ (key, message) =>
          producer
            .send(key, message)
            .map(_ => keyMessage)
      }
      .mergeSubstreams
      .watchTermination()(Keep.right)
      .mapMaterializedValue { terminationFuture =>
        terminationFuture.onComplete(_ => producer.shutdown())
        NotUsed
      }
      .toMat(Sink.seq)(Keep.right)
  }
}
