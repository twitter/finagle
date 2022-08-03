package com.twitter.finagle.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.client.MethodBuilder
import com.twitter.finagle.client.StackClient
import com.twitter.finagle.memcached.protocol.NoOp
import com.twitter.finagle.memcached.protocol.Quit
import com.twitter.finagle.server.StackServer
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.mux
import com.twitter.finagle._
import com.twitter.util.Await
import com.twitter.util.Future
import java.net.InetSocketAddress
import org.scalatest.funsuite.AnyFunSuite

class MethodBuilderTest extends AnyFunSuite {

  private def await[T](f: Future[T]): T = Await.result(f, 15.seconds)

  private implicit val timer = DefaultTimer
  private val serviceSleep = 50.milliseconds

  private[this] def mkService[Req, Rep](rep: Rep): Service[Req, Rep] = {
    new Service[Req, Rep] {
      def apply(req: Req): Future[Rep] =
        Future.sleep(serviceSleep).before { Future.value(rep) }
    }
  }

  private def testTotalTimeout[Req, Rep](
    name: String,
    stackServer: StackServer[Req, Rep],
    stackClient: StackClient[Req, Rep],
    req: Req,
    rep: Rep
  ): Unit = if (!sys.props.contains("SKIP_FLAKY")) {
    test(s"$name client can use total method builder timeouts") {
      val server = stackServer.serve("localhost:*", mkService(rep))
      val addr = server.boundAddress.asInstanceOf[InetSocketAddress]

      val methodBuilder = MethodBuilder.from(s"${addr.getHostName}:${addr.getPort}", stackClient)

      val short = methodBuilder.withTimeout.total(10.millis).newService("short")
      val long = methodBuilder.withTimeout.total(5.seconds).newService("long")

      // check we get a timeout for a client with a short timeout
      intercept[GlobalRequestTimeoutException] {
        await(short(req))
      }

      // check we get a response for a client with a long timeout
      await(long(req))

      val shortClose = short.close()
      await(long.close())
      await(shortClose)
      await(server.close())
    }
  }

  testTotalTimeout(
    "HTTP/1.1",
    Http.server,
    Http.client,
    http.Request(),
    http.Response()
  )

  testTotalTimeout(
    "HTTP/2",
    Http.server.withHttp2,
    Http.client.withHttp2,
    http.Request(),
    http.Response()
  )

  testTotalTimeout(
    "Memcached",
    Memcached.server,
    Memcached.client,
    Quit(),
    NoOp
  )

  testTotalTimeout(
    "Mux",
    Mux.server,
    Mux.client.withSessionQualifier.noFailFast, // we disable failfast to allow retries to
    mux.Request.empty, // smooth over the race with the server bind.
    mux.Response.empty
  )

  private def testPerRequestTimeout[Req, Rep](
    name: String,
    stackServer: StackServer[Req, Rep],
    stackClient: StackClient[Req, Rep],
    req: Req,
    rep: Rep
  ): Unit = if (!sys.props.contains("SKIP_FLAKY")) {
    test(s"$name client can use per request method builder timeouts") {
      val server = stackServer.serve("localhost:*", mkService(rep))
      val addr = server.boundAddress.asInstanceOf[InetSocketAddress]

      val methodBuilder = MethodBuilder.from(s"${addr.getHostName}:${addr.getPort}", stackClient)

      val short = methodBuilder.withTimeout.perRequest(5.millis).newService("short")
      val long = methodBuilder.withTimeout.perRequest(5.seconds).newService("long")

      // check we get a timeout for a client with a short timeout
      intercept[IndividualRequestTimeoutException] {
        await(short(req))
      }

      // check we get a response for a client with a long timeout
      await(long(req))

      val shortClose = short.close()
      await(long.close())
      await(shortClose)
      await(server.close())
    }
  }

  testPerRequestTimeout(
    "HTTP/1.1",
    Http.server,
    Http.client,
    http.Request(),
    http.Response()
  )

  testPerRequestTimeout(
    "HTTP/2",
    Http.server.withHttp2,
    Http.client.withHttp2,
    http.Request(),
    http.Response()
  )

  testPerRequestTimeout(
    "Memcached",
    Memcached.server,
    Memcached.client,
    Quit(),
    NoOp
  )

  testPerRequestTimeout(
    "Mux",
    Mux.server,
    Mux.client,
    mux.Request.empty,
    mux.Response.empty
  )
}
