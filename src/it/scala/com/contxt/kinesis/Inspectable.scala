package com.contxt.kinesis

import java.util.concurrent.ConcurrentLinkedQueue

import akka.stream.scaladsl.Sink
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.stream.{Attributes, Inlet, SinkShape}
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException
import org.scalatest.concurrent.Eventually._

import scala.collection.JavaConverters._
import scala.collection.MapView

object Inspectable {

  /** Returns a Sink that collects incoming elements into a list, and whose state can be inspected at any time
    * by using the function returned as the materialized value. */
  def sink[A]: Sink[A, () => IndexedSeq[A]] = {
    val stage = new GraphStageWithMaterializedValue[SinkShape[A], ConcurrentLinkedQueue[A]] {
      val in: Inlet[A] = Inlet("InspectableSink")
      override val shape: SinkShape[A] = SinkShape(in)

      override def createLogicAndMaterializedValue(
          inheritedAttributes: Attributes
      ): (GraphStageLogic, ConcurrentLinkedQueue[A]) = {
        val nonBlockingQueue = new ConcurrentLinkedQueue[A]

        val logic = new GraphStageLogic(shape) {
          override def preStart(): Unit = pull(in)

          setHandler(
            in,
            new InHandler {
              override def onPush(): Unit = {
                nonBlockingQueue.add(grab(in))
                pull(in)
              }
            }
          )
        }

        (logic, nonBlockingQueue)
      }
    }

    Sink
      .fromGraph(stage)
      .mapMaterializedValue { nonBlockingQueue => () =>
        nonBlockingQueue.asScala.toIndexedSeq
      }
  }
}

private[kinesis] class InspectableConsumerStats extends NoopConsumerStats {
  import InspectableConsumerStats._
  private val checkpointEventsByShardConsumer = new ConcurrentLinkedQueue[(ShardConsumerId, CheckpointEvent)]

  override def checkpointAcked(shardConsumerId: ShardConsumerId): Unit = {
    checkpointEventsByShardConsumer.add(shardConsumerId -> CheckpointAcked)
  }

  override def checkpointDelayed(shardConsumerId: ShardConsumerId, e: Throwable): Unit = {
    e match {
      case _: ThrottlingException => checkpointEventsByShardConsumer.add(shardConsumerId -> CheckpointThrottled)
    }
  }

  def waitForAtLeastOneCheckpointPerShard(minNumberOfShards: Int)(implicit patienceConfig: PatienceConfig): Unit = {
    waitForNrOfCheckpointsPerShard(minNumberOfShards, 1)
  }

  def waitForNrOfCheckpointsPerShard(
      minNumberOfShards: Int,
      checkpointCount: Int
  )(implicit patienceConfig: PatienceConfig): Unit = {
    checkpointEventsByShardConsumer.clear()
    eventually {
      val currentCheckpointCounts = checkpointCountByShardConsumer()
      // The first checkpoint may already be in-progress when we inspect acked checkpoints.
      val minCheckpointCountDelta = checkpointCount + 1
      val shardConsumersWithEnoughCheckpoints = currentCheckpointCounts.collect {
        case (shardConsumer, count) if count >= minCheckpointCountDelta => shardConsumer
      }
      require(shardConsumersWithEnoughCheckpoints.size >= minNumberOfShards)
    }
  }

  def waitForNrOfThrottledCheckpoints(throttledCount: Int)(implicit patienceConfig: PatienceConfig): Unit = {
    eventually {
      require(throttledCheckpointsCount() >= throttledCount)
    }
  }

  private def checkpointCountByShardConsumer(): MapView[ShardConsumerId, Int] = {
    checkpointEventsByShardConsumer.asScala.toIndexedSeq
      .filter { case (_, event) => event == CheckpointAcked }
      .groupBy { case (key, _) => key }
      .mapValues(_.size)
  }

  private def throttledCheckpointsCount(): Int = {
    checkpointEventsByShardConsumer.asScala
      .count { case (_, event) => event == CheckpointThrottled }
  }
}

object InspectableConsumerStats {
  sealed trait CheckpointEvent
  case object CheckpointAcked extends CheckpointEvent
  case object CheckpointThrottled extends CheckpointEvent
}
