package com.twitter.finagle.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.client.{DynamicTimeout, StackClient}
import com.twitter.finagle.Http.Http2
import com.twitter.finagle.memcached.protocol.{NoOp, Quit}
import com.twitter.finagle.mux
import com.twitter.finagle.server.StackServer
import com.twitter.finagle.service.TimeoutFilter
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.{Http, IndividualRequestTimeoutException, Memcached, Mux, Service, http}
import com.twitter.util.{Await, Future, Timer}
import java.net.InetSocketAddress
import org.scalatest.funsuite.AnyFunSuite

class DynamicTimeoutTest extends AnyFunSuite {

  private def await[T](f: Future[T]): T = Await.result(f, 5.seconds)

  private implicit val timer: Timer = DefaultTimer
  private val serviceSleep = 50.milliseconds

  private[this] def mkService[Req, Rep](rep: Rep): Service[Req, Rep] = {
    new Service[Req, Rep] {
      def apply(req: Req): Future[Rep] =
        Future.sleep(serviceSleep).before { Future.value(rep) }
    }
  }

  def testDynamicTimeouts[Req, Rep](
    name: String,
    stackServer: StackServer[Req, Rep],
    stackClient: StackClient[Req, Rep],
    req: Req,
    rep: Rep
  ): Unit = test(s"$name client can use dynamic timeouts") {
    // with our service's impl, by default, requests will never get
    // a response within this timeout as this is smaller than `svcSleep`.
    val clientWithTimeout = stackClient
      .configured(TimeoutFilter.Param(10.millis))

    val modifiedStack =
      clientWithTimeout.stack.replace(TimeoutFilter.role, DynamicTimeout.perRequestModule[Req, Rep])

    val server = stackServer.serve("localhost:*", mkService(rep))
    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientWithTimeout
      .withStack(modifiedStack)
      .newService(s"${addr.getHostName}:${addr.getPort}", "client")

    // check we get a timeout with the default settings
    intercept[IndividualRequestTimeoutException] {
      await(client(req))
    }

    // now bump the request timeout and get back a 200 response
    DynamicTimeout.letPerRequestTimeout(2.seconds) {
      await(client(req))
    }

    await(client.close())
    await(server.close())
  }

  testDynamicTimeouts(
    "HTTP/1.1",
    Http.server,
    Http.client,
    http.Request(),
    http.Response()
  )

  testDynamicTimeouts(
    "HTTP/2",
    Http.server.configuredParams(Http2),
    Http.client.configuredParams(Http2),
    http.Request(),
    http.Response()
  )

  testDynamicTimeouts(
    "Memcached",
    Memcached.server,
    Memcached.client,
    Quit(),
    NoOp
  )

  testDynamicTimeouts(
    "Mux",
    Mux.server,
    Mux.client,
    mux.Request.empty,
    mux.Response.empty
  )

}
