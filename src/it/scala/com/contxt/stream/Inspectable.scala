package com.contxt.stream

import akka.stream.{ Attributes, Inlet, SinkShape }
import akka.stream.scaladsl.Sink
import akka.stream.stage.{ GraphStageLogic, GraphStageWithMaterializedValue, InHandler }
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber
import java.util.concurrent.ConcurrentLinkedQueue
import org.scalatest.concurrent.Eventually._
import scala.collection.JavaConverters._

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
            in, new InHandler {
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
      .mapMaterializedValue { nonBlockingQueue =>
        () => nonBlockingQueue.asScala.toIndexedSeq
      }
  }
}

private[stream] class InspectableCheckpointLog extends CheckpointLog {
  import CheckpointLog._
  private val checkpointEventsByShardKey = new ConcurrentLinkedQueue[(ShardKey, CheckpointEvent)]

  override def checkpointEvent(shardKey: ShardKey, checkpointEvent: CheckpointEvent): Unit = {
    checkpointEventsByShardKey.add(shardKey -> checkpointEvent)
  }

  def waitForAtLeastOneCheckpointPerShard(minNumberOfShards: Int)(implicit patienceConfig: PatienceConfig): Unit = {
    waitForNrOfCheckpointsPerShard(minNumberOfShards, 1)
  }

  def waitForNrOfCheckpointsPerShard(
    minNumberOfShards: Int, checkpointCount: Int
  )(implicit patienceConfig: PatienceConfig): Unit = {
    checkpointEventsByShardKey.clear()
    eventually {
      val currentCheckpointCounts = checkpointCountByShardKey()
      // The first checkpoint may already be in-progress when we inspect acked checkpoints.
      val minCheckpointCountDelta = checkpointCount + 1
      val shardKeysWithEnoughCheckpoints = currentCheckpointCounts.collect {
        case (shardKey, count) if count >= minCheckpointCountDelta => shardKey
      }
      require(shardKeysWithEnoughCheckpoints.size >= minNumberOfShards)
    }
  }

  def waitForNrOfThrottledCheckpoints(throttledCount: Int)(implicit patienceConfig: PatienceConfig): Unit = {
    eventually {
      require(throttledCheckpointsCount() >= throttledCount)
    }
  }

  private def checkpointCountByShardKey(): Map[ShardKey, Int] = {
    checkpointEventsByShardKey.asScala.toIndexedSeq
      .filter { case (_, event) => event.isInstanceOf[CheckpointAck] }
      .groupBy { case (key, _) => key }
      .mapValues(_.size)
  }

  private def throttledCheckpointsCount(): Int = {
    checkpointEventsByShardKey.asScala
      .count { case (_, event) => event == CheckpointThrottled }
  }
}
