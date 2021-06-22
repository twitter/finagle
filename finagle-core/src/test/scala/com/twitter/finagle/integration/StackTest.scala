package com.twitter.finagle.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.client.StackClient
import com.twitter.finagle.client.utils.StringClient
import com.twitter.finagle.liveness.FailureAccrualFactory
import com.twitter.finagle.server.utils.StringServer
import com.twitter.util.{Await, Future}
import java.net.{InetAddress, InetSocketAddress}
import org.scalatest.funsuite.AnyFunSuite

class StackTest extends AnyFunSuite {
  class TestCtx {
    val failService =
      Service.mk[String, String] { s: String => Future.exception(Failure.rejected("unhappy")) }

    val newClientStack =
      StackClient
        .newStack[String, String]
        .replace(
          StackClient.Role.prepFactory,
          (sf: ServiceFactory[String, String]) => sf.map(identity[Service[String, String]])
        )
  }

  test("Client/Server: Status.busy propagates from failAccrual to the top of the stack") {
    new TestCtx {
      val server = StringServer.server.serve(new InetSocketAddress(0), failService)
      val client =
        StringClient.client
          .withStack(newClientStack)
          .configured(FailureAccrualFactory.Param(5, 1.minute))
          .newService(
            Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
            "client"
          )

      // marked busy by FailureAccrualFactory
      for (_ <- 0 until 6) {
        intercept[Exception](Await.result(client("hello\n")))
      }

      assert(client.status == Status.Busy)
    }
  }

  test("ClientBuilder: Status.busy propagates from failAccrual to the top of the stack") {
    new TestCtx {
      val server = StringServer.server
        .withLabel("server")
        .serve(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), failService)

      val client = ClientBuilder()
        .stack(StringClient.client)
        .failureAccrualParams((5, 1.minute))
        .hosts(Seq(server.boundAddress.asInstanceOf[InetSocketAddress]))
        .hostConnectionLimit(1)
        .build()

      // marked busy by FailureAccrualFactory
      for (_ <- 0 until 6) {
        intercept[Exception](Await.result(client("hello\n")))
      }

      assert(client.status == Status.Busy)
    }
  }

  test("Client/Server: Status.busy propagates from failFast to the top of the stack") {
    new TestCtx {
      val client =
        StringClient.client
          .withStack(newClientStack)
          .newService(
            Name.bound(Address(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))),
            "client"
          )

      // marked busy by FailFastFactory
      intercept[Exception](Await.result(client("hello\n")))

      assert(client.status == Status.Busy)
    }
  }
}
