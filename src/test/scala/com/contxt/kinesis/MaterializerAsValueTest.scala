package com.contxt.kinesis

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

class MaterializerAsValueTest
    extends TestKit(ActorSystem("TestSystem"))
    with WordSpecLike
    with BeforeAndAfterAll
    with Matchers {
  override protected def afterAll: Unit = TestKit.shutdownActorSystem(system)
  protected implicit val materializer: Materializer = ActorMaterializer()
  private val awaitTimeout = 1.second

  "MaterializerAsValue" when {
    "materialized" should {
      "start already completed" in {
        val resultFuture = MaterializerAsValue.source[Int].runWith(Sink.seq)
        val result = Await.result(resultFuture, awaitTimeout)
        result shouldBe empty
      }

      "return a future of materializer as the materialized value" in {
        val materializerFuture = MaterializerAsValue.source[Int].to(Sink.seq).run
        val liftedMaterializer = Await.result(materializerFuture, awaitTimeout)
        liftedMaterializer shouldBe materializer
      }
    }
  }
}
