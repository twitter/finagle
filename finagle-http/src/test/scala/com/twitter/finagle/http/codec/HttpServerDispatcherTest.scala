package com.twitter.finagle.http.codec

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Service
import com.twitter.finagle.Status
import com.twitter.finagle.http.Fields
import com.twitter.finagle.http.Request
import com.twitter.finagle.http.Response
import com.twitter.finagle.http.Version
import com.twitter.finagle.http.{Status => HttpStatus}
import com.twitter.finagle.http.StreamTransport
import com.twitter.finagle.netty4.http.Bijections
import com.twitter.finagle.netty4.http.Netty4ServerStreamTransport
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.transport.QueueTransport
import com.twitter.finagle.transport.Transport
import com.twitter.io.Buf
import com.twitter.io.Reader
import com.twitter.io.ReaderDiscardedException
import com.twitter.util.Await
import com.twitter.util.Awaitable
import com.twitter.util.Future
import com.twitter.util.Promise
import io.netty.buffer.Unpooled
import io.netty.handler.codec.http.DefaultHttpContent
import io.netty.handler.codec.http.HttpContent
import io.netty.handler.codec.http.HttpRequest
import io.netty.handler.codec.http.HttpResponse
import io.netty.handler.codec.http.HttpResponseStatus
import io.netty.handler.codec.http.LastHttpContent
import java.nio.charset.StandardCharsets
import org.scalatest.funsuite.AnyFunSuite

// Note: We need a concrete impl to test it so the finagle-http package is most
// appropriate even though the implementation is in finagle-base-http.
class HttpServerDispatcherTest extends AnyFunSuite {
  import HttpServerDispatcherTest._

  private[this] def from(req: Request): HttpRequest =
    Bijections.finagle.requestToNetty(req, req.contentLength)

  private[this] def await[T](t: Awaitable[T]): T = Await.result(t, 15.seconds)

  private[this] def testChunk(trans: Transport[Any, Any], chunk: HttpContent): Unit = {
    val f = trans.read()
    assert(!f.isDefined)
    val expected = chunk.content.duplicate.retain()
    await(trans.write(chunk))
    val c = await(f).asInstanceOf[HttpContent]
    assert(c.content == expected)
  }

  test("invalid message") {
    val (in, out) = mkPair[Any, Any]
    val service = Service.mk { _: Request => Future.value(Response()) }
    val disp = new HttpServerDispatcher(out, service, NullStatsReceiver)

    in.write("invalid")
    await(out.onClose)
    assert(out.status == Status.Closed)
  }

  test("don't clobber service 'Connection: close' headers set by service") {
    val service = Service.mk { _: Request =>
      val resp = Response()
      resp.setContentString("foo")
      resp.headerMap.set(Fields.Connection, "close")
      Future.value(resp)
    }

    val (in, out) = mkPair[Any, Any]
    val disp = new HttpServerDispatcher(out, service, NullStatsReceiver)

    in.write(from(Request("/foo")))
    await(in.read()) match {
      case resp: HttpResponse =>
        assert(resp.status == HttpResponseStatus.OK)
        assert(resp.headers().get(Fields.Connection) == "close")

      case other => fail(s"Received unknown type: ${other.getClass.getSimpleName}")
    }
  }

  test("streaming request body") {
    val service = Service.mk { req: Request => ok(req.reader) }
    val (in, out) = mkPair[Any, Any]
    val disp = new HttpServerDispatcher(out, service, NullStatsReceiver)

    val req = Request()
    req.setChunked(true)
    in.write(from(req))
    await(in.read())

    testChunk(in, chunk("a"))
    testChunk(in, chunk("foo"))
    testChunk(in, LastHttpContent.EMPTY_LAST_CONTENT)
  }

  test("client abort before dispatch") {
    val promise = new Promise[Response]
    val service = Service.mk { _: Request => promise }

    val (in, out) = mkPair[Any, Any]
    val disp = new HttpServerDispatcher(out, service, NullStatsReceiver)

    in.write(from(Request()))

    // Simulate channel closure
    out.close()
    assert(promise.isInterrupted.isDefined)
  }

  test("client abort after dispatch") {
    val req = Request()
    val res = Response()
    val service = Service.mk { _: Request => Future.value(res) }

    val (in, out) = mkPair[Any, Any]
    val disp = new HttpServerDispatcher(out, service, NullStatsReceiver)

    res.setChunked(true)
    in.write(from(req))

    await(in.read())

    // Simulate channel closure
    out.close()
    intercept[ReaderDiscardedException] { await(res.writer.write(Buf.Utf8("."))) }
  }

  test("server response fails mid-stream") {
    val statsReceiver = new InMemoryStatsReceiver()
    val req = Request()
    val res = Response()
    val service = Service.mk { _: Request => Future.value(res) }

    val (in, out) = mkPair[Any, Any]
    val disp = new HttpServerDispatcher(out, service, statsReceiver)

    res.setChunked(true)
    res.writer.fail(new IllegalArgumentException())
    in.write(from(req))

    await(in.read())

    assert(statsReceiver.counters(Seq("stream", "failures")) == 1)
    assert(
      statsReceiver.counters(Seq("stream", "failures", "java.lang.IllegalArgumentException")) == 1)
  }
}

object HttpServerDispatcherTest {
  def mkPair[A: Manifest, B: Manifest]: (Transport[A, B], StreamTransport[Response, Request]) = {
    val inQ = new AsyncQueue[Any]
    val outQ = new AsyncQueue[Any]
    (
      Transport.cast[A, B](new QueueTransport(outQ, inQ)),
      new Netty4ServerStreamTransport(new QueueTransport(inQ, outQ))
    )
  }

  def chunk(content: String): HttpContent = {
    val bytes = content.getBytes(StandardCharsets.UTF_8)
    val buf = Unpooled.buffer(bytes.length)
    buf.writeBytes(bytes)
    new DefaultHttpContent(buf)
  }

  def ok(reader: Reader[Buf]): Future[Response] =
    Future.value(Response(Version.Http11, HttpStatus.Ok, reader))
}
