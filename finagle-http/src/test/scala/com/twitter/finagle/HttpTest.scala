package com.twitter.finagle

import java.net.InetSocketAddress

import com.twitter.finagle.http.service.HttpResponseClassifier
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.service.{ResponseClass, ResponseClassifier}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.toggle.flag
import com.twitter.util.{Await, Future}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HttpTest extends FunSuite {

  private def classifier(params: Stack.Params): ResponseClassifier =
    params[param.ResponseClassifier].responseClassifier


  test("client uses default response classifier when toggle disabled") {
    flag.overrides.let(Http.ServerErrorsAsFailuresToggleId, 0.0) {
      val rc = classifier(new Http.Client().params)
      assert(rc == ResponseClassifier.Default)
    }
  }

  test("client uses ServerErrorsAsFailures response classifier when toggle enabled") {
    flag.overrides.let(Http.ServerErrorsAsFailuresToggleId, 1.0) {
      val rc = classifier(new Http.Client().params)
      assert(rc == HttpResponseClassifier.ServerErrorsAsFailures)
    }
  }

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

    Await.result(client(Request()))

    assert(serverReceiver.counters(Seq("stats_test_server", "status", "404")) == 1)
    assert(serverReceiver.counters(Seq("stats_test_server", "status", "4XX")) == 1)
    assert(serverReceiver.stats(Seq("stats_test_server", "response_size")) == Seq(5.0))

    assert(clientReceiver.counters(Seq("stats_test_client", "status", "404")) == 1)
    assert(clientReceiver.counters(Seq("stats_test_client", "status", "4XX")) == 1)
    assert(clientReceiver.stats(Seq("stats_test_client", "response_size")) == Seq(5.0))
  }

  test("server uses default response classifier when toggle disabled") {
    flag.overrides.let(Http.ServerErrorsAsFailuresToggleId, 0.0) {
      val rc = classifier(new Http.Server().params)
      assert(rc == ResponseClassifier.Default)
    }
  }

  test("server uses ServerErrorsAsFailures response classifier when toggle enabled") {
    flag.overrides.let(Http.ServerErrorsAsFailuresToggleId, 1.0) {
      val rc = classifier(new Http.Server().params)
      assert(rc == HttpResponseClassifier.ServerErrorsAsFailures)
    }
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
