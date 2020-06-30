package com.contxt.kinesis

import akka.stream._
import akka.stream.scaladsl._
import akka.stream.stage._

import scala.concurrent.{Future, Promise}

private[kinesis] object MaterializerAsValue {

  /** A source that starts already completed, and provides a future of stream materializer as the materialized value. */
  def source[In]: Source[In, Future[Materializer]] = {
    val stage: GraphStageWithMaterializedValue[SourceShape[In], Future[Materializer]] =
      new GraphStageWithMaterializedValue[SourceShape[In], Future[Materializer]] {
        val out: Outlet[In] = Outlet("MaterializerAsValueSource.out")
        val shape: SourceShape[In] = SourceShape(out)

        def createLogicAndMaterializedValue(
            inheritedAttributes: Attributes
        ): (GraphStageLogic, Future[Materializer]) = {
          val promise = Promise[Materializer]()
          val logic = new GraphStageLogic(shape) {
            override def preStart(): Unit = {
              promise.trySuccess(materializer)
              completeStage()
            }

            setHandler(
              out,
              new OutHandler {
                override def onPull(): Unit = {
                  // Do nothing.
                }
              }
            )
          }
          val materializedValue = promise.future
          (logic, materializedValue)
        }
      }
    Source.fromGraph(stage)
  }
}
