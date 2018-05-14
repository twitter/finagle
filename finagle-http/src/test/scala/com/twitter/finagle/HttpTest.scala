package com.twitter.finagle

import com.twitter.finagle.filter.NackAdmissionFilter
import com.twitter.finagle.http.{Request, Response, serverErrorsAsFailures}
import com.twitter.finagle.service.{ReqRep, ResponseClass, ResponseClassifier}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Await, Duration, Future, Return}
import java.net.InetSocketAddress
import org.scalatest.FunSuite
import org.scalatest.concurrent.Eventually

class HttpTest extends FunSuite with Eventually {

  private def classifier(params: Stack.Params): ResponseClassifier =
    params[param.ResponseClassifier].responseClassifier

  test("client stack includes exactly one NackAdmissionFilter") {
    val client = Http.client
    val stack = client.stack

    assert(stack.tails.count(_.head.role == NackAdmissionFilter.role) == 1)
  }

  test("client uses custom response classifier by default") {
    val customRc: ResponseClassifier = {
      case _ => ResponseClass.Success
    }

    val client = new Http.Client().withResponseClassifier(customRc)
    val rc = classifier(client.params)
    assert(rc == customRc)
  }

  test("responseClassifierParam toggled off") {
    import com.twitter.finagle.http.{Status => HStatus}

    def rep(code: HStatus): Response = Response(code)
    def reqRep(rep: Response): ReqRep = ReqRep(Request("/index.cgi"), Return(rep))

    val rc = Http.responseClassifierParam.responseClassifier

    def repClass(rep: Response): ResponseClass =
      rc.applyOrElse(reqRep(rep), ResponseClassifier.Default)

    // disabling the classifier
    serverErrorsAsFailures.let(false) {
      assert(rc.isDefinedAt(reqRep(rep(HStatus.Ok))))
      assert(rc.isDefinedAt(reqRep(rep(HStatus.BadRequest))))
      assert(rc.isDefinedAt(reqRep(rep(HStatus.ServiceUnavailable))))

      assert(ResponseClass.Success == repClass(rep(HStatus.Ok)))
      assert(ResponseClass.Success == repClass(rep(HStatus.BadRequest)))
      assert(ResponseClass.Success == repClass(rep(HStatus.ServiceUnavailable)))
    }
  }

  test("responseClassifierParam toggled on") {
    import com.twitter.finagle.http.{Status => HStatus}

    def rep(code: HStatus): Response = Response(code)
    def reqRep(rep: Response): ReqRep = ReqRep(Request("/index.cgi"), Return(rep))

    val rc = Http.responseClassifierParam.responseClassifier

    def repClass(rep: Response): ResponseClass =
      rc.applyOrElse(reqRep(rep), ResponseClassifier.Default)

    // uses the ServerErrorsAsFailures classifier for 500s
    serverErrorsAsFailures.let(true) {
      assert(rc.isDefinedAt(reqRep(rep(HStatus.Ok))))
      assert(rc.isDefinedAt(reqRep(rep(HStatus.BadRequest))))
      assert(rc.isDefinedAt(reqRep(rep(HStatus.ServiceUnavailable))))

      assert(ResponseClass.Success == repClass(rep(HStatus.Ok)))
      assert(ResponseClass.Success == repClass(rep(HStatus.BadRequest)))
      assert(ResponseClass.NonRetryableFailure == repClass(rep(HStatus.ServiceUnavailable)))
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
      Http.server.withHttpStats
        .withStatsReceiver(serverReceiver)
        .withLabel("stats_test_server")
        .serve(":*", service)

    val client =
      Http.client.withHttpStats
        .withStatsReceiver(clientReceiver)
        .newService(
          "localhost:" + server.boundAddress.asInstanceOf[InetSocketAddress].getPort,
          "stats_test_client"
        )

    Await.result(client(Request()), Duration.fromSeconds(5))

    eventually {
      assert(serverReceiver.counters(Seq("stats_test_server", "http", "status", "404")) == 1)
      assert(serverReceiver.counters(Seq("stats_test_server", "http", "status", "4XX")) == 1)
      assert(serverReceiver.stats(Seq("stats_test_server", "http", "response_size")) == Seq(5.0))

      assert(clientReceiver.counters(Seq("stats_test_client", "http", "status", "404")) == 1)
      assert(clientReceiver.counters(Seq("stats_test_client", "http", "status", "4XX")) == 1)
      assert(clientReceiver.stats(Seq("stats_test_client", "http", "response_size")) == Seq(5.0))
      assert(
        clientReceiver.gauges.contains(Seq("stats_test_client", "dispatcher", "serial", "queue_size"))
      )
    }
  }

  test("server uses custom response classifier when specified") {
    val customRc: ResponseClassifier = {
      case _ => ResponseClass.Success
    }

    val client = new Http.Server().withResponseClassifier(customRc)
    val rc = classifier(client.params)
    assert(rc == customRc)
  }

  test("Netty 4 is a default implementation") {
    val transporter = Http.client.params[Http.HttpImpl].transporter
    val listener = Http.server.params[Http.HttpImpl].listener

    val addr = InetSocketAddress.createUnresolved("supdog", 0)

    assert(transporter(Stack.Params.empty)(addr).toString == "Netty4Transporter")
    assert(listener(Stack.Params.empty).toString == "Netty4Listener")
  }
}
