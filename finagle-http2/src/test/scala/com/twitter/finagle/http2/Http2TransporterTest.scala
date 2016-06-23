package com.twitter.finagle.http2

import com.twitter.conversions.time._
import com.twitter.finagle.Status
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Await, Duration, Future, Time}
import java.net.{SocketAddress, InetSocketAddress}
import java.security.cert.Certificate
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Http2TransporterTest extends FunSuite {

  def await[T](f: Future[T], wait: Duration = 1.second) =
    Await.result(f, wait)

  class TestTransport(addr: SocketAddress) extends Transport[Any, Any] {
    def write(req: Any): Future[Unit] = Future.never
    def read(): Future[Any] = Future.never
    def status: Status = Status.Open
    def onClose: Future[Throwable] = Future.never
    def localAddress: SocketAddress = addr
    def remoteAddress: SocketAddress = addr
    def peerCertificate: Option[Certificate] = None
    def close(deadline: Time): Future[Unit] = Future.Unit
  }

  object TestTransporter extends Transporter[Any, Any] {
    def apply(addr: SocketAddress): Future[Transport[Any, Any]] =
      Future.value(new TestTransport(addr))
  }

  test("Http2Transporter caches transports") {
    val transporter = new Http2Transporter(TestTransporter) {
      def cached(addr: SocketAddress) = transporterCache.containsKey(addr)
    }

    val addr = new InetSocketAddress("127.1", 14400)
    val tf = transporter(addr)
    assert(transporter.cached(addr))
    val t = await(tf)
    await(t.close())
    assert(!transporter.cached(addr))
  }
}
