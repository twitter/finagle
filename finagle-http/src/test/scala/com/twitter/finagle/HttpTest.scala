package com.twitter.finagle

import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.service.{ResponseClass, ResponseClassifier}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.toggle.flag
import com.twitter.util.{Await, Duration, Future}
import java.net.InetSocketAddress
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HttpTest extends FunSuite {

  private def classifier(params: Stack.Params): ResponseClassifier =
    params[param.ResponseClassifier].responseClassifier

  test("client uses custom response classifier when specified") {
    flag.overrides.let(Http.ServerErrorsAsFailuresToggleId, 1.0) {
      val customRc: ResponseClassifier = {
        case _ => ResponseClass.Success
      }

      val client = new Http.Client().withResponseClassifier(customRc)
      val rc = classifier(client.params)
      assert(rc == customRc)
    }
  }

  test("client and server emit http specific stats when enabled") {
    val serverReceiver = new InMemoryStatsReceiver
    val clientReceiver = new InMemoryStatsReceiver

    val service = new Service[Request, Response] {
      def apply(request: Request): Future[Response] = {
        val response = request.response
        response.statusCode = 404
        response.write("hello")
        Future.value(response)
      }
    }

    val server =
      Http.server
        .withHttpStats
        .withStatsReceiver(serverReceiver)
        .withLabel("stats_test_server")
        .serve(":*", service)

    val client =
      Http.client
        .withHttpStats
        .withStatsReceiver(clientReceiver)
        .newService("localhost:" + server.boundAddress.asInstanceOf[InetSocketAddress].getPort, "stats_test_client")

    Await.result(client(Request()), Duration.fromSeconds(5))

    assert(serverReceiver.counters(Seq("stats_test_server", "http", "status", "404")) == 1)
    assert(serverReceiver.counters(Seq("stats_test_server", "http", "status", "4XX")) == 1)
    assert(serverReceiver.stats(Seq("stats_test_server", "http", "response_size")) == Seq(5.0))

    assert(clientReceiver.counters(Seq("stats_test_client", "http", "status", "404")) == 1)
    assert(clientReceiver.counters(Seq("stats_test_client", "http", "status", "4XX")) == 1)
    assert(clientReceiver.stats(Seq("stats_test_client", "http", "response_size")) == Seq(5.0))
  }

  test("server uses custom response classifier when specified") {
    flag.overrides.let(Http.ServerErrorsAsFailuresToggleId, 1.0) {
      val customRc: ResponseClassifier = {
        case _ => ResponseClass.Success
      }

      val client = new Http.Server().withResponseClassifier(customRc)
      val rc = classifier(client.params)
      assert(rc == customRc)
    }
  }

}
