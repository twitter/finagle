package com.twitter.finagle.http

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.http2.param.PriorKnowledge
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.{Service, Http => FinagleHttp}
import com.twitter.io.Buf
import com.twitter.util.{Await, Future}
import java.net.InetSocketAddress
import org.scalatest.funsuite.AnyFunSuite

class DirectPayloadsLifecycleTest extends AnyFunSuite {

  def doTest(name: String, client: FinagleHttp.Client, server: FinagleHttp.Server): Unit = {
    def assertNonDirect(b: Buf): Unit = b match {
      case Buf.ByteBuffer(byteBuffer) => assert(!byteBuffer.isDirect)
      case _ => () // other cases are guaranteed to be backed by heap buffers
    }

    test(s"[$name] should never leak direct paylods into the user space") {
      val service = new Service[Request, Response] {
        def apply(req: Request): Future[Response] = {
          assertNonDirect(req.content)
          val rep = Response()
          rep.content = Buf.Utf8("." * 10)
          Future.value(rep)
        }
      }

      val s = server
        .withLabel("server")
        .withStatsReceiver(NullStatsReceiver)
        .serve("localhost:*", service)

      val addr = s.boundAddress.asInstanceOf[InetSocketAddress]

      val c = client
        .withStatsReceiver(NullStatsReceiver)
        .newService("%s:%d".format(addr.getHostName, addr.getPort), "client")

      val req = Request()
      req.content = Buf.Utf8("." * 10)
      val rep = Await.result(c(req), 30.seconds)

      assertNonDirect(rep.content)
      Await.ready(s.close().before(s.close()), 30.seconds)
    }
  }

  doTest(
    "HTTP/1.1",
    FinagleHttp.client.withNoHttp2,
    FinagleHttp.server.withNoHttp2
  )

  doTest(
    "HTTP/2",
    FinagleHttp.client.withHttp2,
    FinagleHttp.server.withHttp2
  )

  doTest(
    "HTTP/2 client <-> HTTP/1.1 server",
    FinagleHttp.client.withHttp2,
    FinagleHttp.server
  )

  doTest(
    "Prior Knowledge HTTP/2",
    FinagleHttp.client.withHttp2.configured(PriorKnowledge(true)),
    FinagleHttp.server.withHttp2
  )

  // TODO: Test ALPN
}
