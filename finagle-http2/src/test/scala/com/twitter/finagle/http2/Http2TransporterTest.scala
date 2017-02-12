package com.twitter.finagle.http2

import com.twitter.conversions.time._
import com.twitter.finagle.Status
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.http2.transport.Http2ClientDowngrader
import com.twitter.finagle.transport.{Transport, TransportProxy}
import com.twitter.util.{Await, Duration, Future, Time, Promise}
import io.netty.handler.codec.http.{DefaultFullHttpResponse, HttpVersion,
  HttpResponseStatus, HttpResponse, LastHttpContent}
import java.net.{SocketAddress, InetSocketAddress}
import java.security.cert.Certificate
import io.netty.handler.codec.http.HttpClientUpgradeHandler.UpgradeEvent
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.language.reflectiveCalls

@RunWith(classOf[JUnitRunner])
class Http2TransporterTest extends FunSuite {
  def await[T](f: Future[T], wait: Duration = 1.second) =
    Await.result(f, wait)

  class TestTransport(addr: SocketAddress) extends Transport[Any, Any] {
    private[this] val _onClose = Promise[Throwable]()
    def write(req: Any): Future[Unit] = Future.never
    def read(): Future[Any] = Future.never
    def status: Status = Status.Open
    def onClose: Future[Throwable] = _onClose
    def localAddress: SocketAddress = addr
    def remoteAddress: SocketAddress = addr
    def peerCertificate: Option[Certificate] = None
    def close(deadline: Time): Future[Unit] = {
      _onClose.setValue(new Exception("boom!"))
      Future.Unit
    }
  }

  class BackingTransporter(
      fn: SocketAddress => Transport[Any, Any])
    extends Transporter[Any, Any] {

    var count = 0

    def apply(addr: SocketAddress): Future[Transport[Any, Any]] = {
      count += 1
      Future.value(fn(addr))
    }
  }

  class TestTransporter extends BackingTransporter(new TestTransport(_))

  test("Http2Transporter caches transports") {
    val transporter = new Http2Transporter(new TestTransporter(), new TestTransporter()) {
      def cached(addr: SocketAddress) = transporterCache.containsKey(addr)
    }

    val addr = new InetSocketAddress("127.1", 14400)
    val tf = transporter(addr)
    assert(transporter.cached(addr))
  }

  test("Http2Transporter decaches transport when closed") {
    val transporter = new Http2Transporter(new TestTransporter(), new TestTransporter()) {
      def cached(addr: SocketAddress) = transporterCache.containsKey(addr)
    }

    val addr = new InetSocketAddress("127.1", 14400)
    val tf = transporter(addr)
    assert(transporter.cached(addr))
    val t = await(tf)
    await(t.close())
    assert(!transporter.cached(addr))
  }

  test("Http2Transporter uses http11 for the second outstanding transport preupgrade") {
    val (t1, t2) = (new TestTransporter(), new TestTransporter())
    val transporter = new Http2Transporter(t1, t2)
    val addr = new InetSocketAddress("127.1", 14400)

    await(transporter(addr))
    assert(t1.count == 1)
    assert(t2.count == 0)

    await(transporter(addr))
    assert(t1.count == 1)
    assert(t2.count == 1)
  }

  class UpgradeTransport(
      upgradeRep: UpgradeEvent,
      addr: SocketAddress)
    extends TransportProxy[Any, Any](new TestTransport(addr)) {

    @volatile var count = 0
    def write(msg: Any): Future[Unit] = Future.Done
    def read(): Future[Any] = if (count == 0) {
      count += 1
      Future.value(upgradeRep)
    } else {
      if (upgradeRep == UpgradeEvent.UPGRADE_SUCCESSFUL && count == 1) {
        count += 1
        val rep = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
        Future.value(Http2ClientDowngrader.Message(rep, 1))
      } else if (upgradeRep == UpgradeEvent.UPGRADE_SUCCESSFUL) {
        count += 1
        Future.never
      } else {
        count += 1
        val rep = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
        Future.value(rep)
      }
    }
  }

  class UpgradingTransporter(
      upgradeRep: UpgradeEvent)
    extends BackingTransporter(new UpgradeTransport(upgradeRep, _))

  test("Http2Transporter reuses the http2 transporter postupgrade") {
    val t1 = new UpgradingTransporter(UpgradeEvent.UPGRADE_SUCCESSFUL)
    val t2 = new TestTransporter()
    val transporter = new Http2Transporter(t1, t2)
    val addr = new InetSocketAddress("127.1", 14400)

    val trans = await(transporter(addr))
    assert(t1.count == 1)
    assert(t2.count == 0)
    assert(await(trans.read()).asInstanceOf[HttpResponse].getStatus == HttpResponseStatus.OK)

    await(transporter(addr))
    assert(t1.count == 1)
    assert(t2.count == 0)
  }

  test("Http2Transporter uses the http11 transporter post rejection") {
    val t1 = new UpgradingTransporter(UpgradeEvent.UPGRADE_REJECTED)
    val t2 = new TestTransporter()
    val transporter = new Http2Transporter(t1, t2)
    val addr = new InetSocketAddress("127.1", 14400)

    val trans = await(transporter(addr))
    assert(t1.count == 1)
    assert(t2.count == 0)
    assert(await(trans.read()).asInstanceOf[HttpResponse].getStatus == HttpResponseStatus.OK)

    await(transporter(addr))
    assert(t1.count == 1)
    assert(t2.count == 1)
  }

  test("Http2Transporter marks outstanding transports dead after a successful upgrade") {
    val t1 = new UpgradingTransporter(UpgradeEvent.UPGRADE_SUCCESSFUL)
    val t2 = new TestTransporter()
    val transporter = new Http2Transporter(t1, t2)
    val addr = new InetSocketAddress("127.1", 14400)

    val trans = await(transporter(addr))
    assert(t1.count == 1)
    assert(t2.count == 0)

    val http11Trans = await(transporter(addr))
    assert(t1.count == 1)
    assert(t2.count == 1)
    assert(http11Trans.status == Status.Open)

    assert(await(trans.read()).asInstanceOf[HttpResponse].getStatus == HttpResponseStatus.OK)

    assert(http11Trans.status == Status.Closed)
  }

  test("Http2Transporter keeps outstanding transports alive after a failed upgrade") {
    val t1 = new UpgradingTransporter(UpgradeEvent.UPGRADE_REJECTED)
    val t2 = new TestTransporter()
    val transporter = new Http2Transporter(t1, t2)
    val addr = new InetSocketAddress("127.1", 14400)

    val trans = await(transporter(addr))
    assert(t1.count == 1)
    assert(t2.count == 0)

    val http11Trans = await(transporter(addr))
    assert(t1.count == 1)
    assert(t2.count == 1)
    assert(http11Trans.status == Status.Open)

    assert(await(trans.read()).asInstanceOf[HttpResponse].getStatus == HttpResponseStatus.OK)

    assert(http11Trans.status == Status.Open)
  }

  class FirstFail(f: Future[Transport[Any, Any]]) extends Transporter[Any, Any] {
    var first = true
    var count = 0
    def apply(addr: SocketAddress): Future[Transport[Any, Any]] = {
      count += 1
      if (first) {
        first = false
        f
      } else Future.value(new TestTransport(addr))
    }
  }

  test("Http2Transporter marks outstanding transports dead after a failed connect attempt") {
    val p = Promise[Transport[Any, Any]]()
    val t1 = new FirstFail(p)
    val t2 = new TestTransporter()
    val transporter = new Http2Transporter(t1, t2)
    val addr = new InetSocketAddress("127.1", 14400)

    val fTrans = transporter(addr)
    assert(!fTrans.isDefined)
    assert(t1.count == 1)
    assert(t2.count == 0)

    val trans = await(transporter(addr))
    assert(t1.count == 1)
    assert(t2.count == 1)
    assert(trans.status == Status.Open)

    assert(!fTrans.isDefined)
    val e = new Exception("boom!")
    p.setException(e)
    val actual = intercept[Exception] {
      await(fTrans)
    }
    assert(actual == e)
    assert(trans.status == Status.Closed)
  }

  test("Http2Transporter can try to establish a connection again if a connection failed") {
    val e = new Exception("boom!")
    val t1 = new FirstFail(Future.exception(e))
    val t2 = new TestTransporter()
    val transporter = new Http2Transporter(t1, t2)
    val addr = new InetSocketAddress("127.1", 14400)

    val actual = intercept[Exception] {
      await(transporter(addr))
    }
    assert(actual == e)
    assert(t1.count == 1)
    assert(t2.count == 0)

    await(transporter(addr))
    assert(t1.count == 2)
    assert(t2.count == 0)
  }

  test("Http2Transporter evicts the connection if it dies") {
    val t1 = new UpgradingTransporter(UpgradeEvent.UPGRADE_SUCCESSFUL)
    val t2 = new TestTransporter()
    val transporter = new Http2Transporter(t1, t2)
    val addr = new InetSocketAddress("127.1", 14400)

    val first = await(transporter(addr))
    assert(t1.count == 1)
    assert(t2.count == 0)

    await(first.write(LastHttpContent.EMPTY_LAST_CONTENT))
    assert(await(first.read()).asInstanceOf[HttpResponse].getStatus == HttpResponseStatus.OK)

    val second = await(transporter(addr))
    assert(t1.count == 1)
    assert(t2.count == 0)

    first.close()
    second.close()

    await(transporter(addr))
    assert(t1.count == 2)
    assert(t2.count == 0)
  }
}
