package com.twitter.finagle.http.codec

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.time._
import com.twitter.finagle.{Service, Status}
import com.twitter.finagle.http.{Fields, Request, Response, Version, Status => HttpStatus}
import com.twitter.finagle.http.exp.StreamTransport
import com.twitter.finagle.netty4.http.{Bijections, Netty4ServerStreamTransport}
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.transport.{QueueTransport, Transport}
import com.twitter.io.{Buf, Reader}
import com.twitter.util.{Await, Awaitable, Future, Promise}
import io.netty.buffer.Unpooled
import io.netty.handler.codec.http.{DefaultHttpContent, HttpContent, HttpRequest, HttpResponse, HttpResponseStatus, LastHttpContent}
import java.nio.charset.StandardCharsets
import org.scalatest.FunSuite

// Note: We need a concrete impl to test it so the finagle-http package is most
// appropriate even though the implementation is in finagle-base-http.
class HttpServerDispatcherTest extends FunSuite {
  import HttpServerDispatcherTest._

  private[this] def from(req: Request): HttpRequest = Bijections.finagle.requestToNetty(req)

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
    val service = Service.mk { _: Request =>
      Future.value(Response())
    }
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
    val service = Service.mk { req: Request =>
      ok(req.reader)
    }
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
    val service = Service.mk { _: Request =>
      promise
    }

    val (in, out) = mkPair[Any, Any]
    val disp = new HttpServerDispatcher(out, service, NullStatsReceiver)

    in.write(from(Request()))

    // Simulate channel closure
    out.close()
    assert(promise.isInterrupted.isDefined)
  }

  test("client abort after dispatch") {
    val req = Request()
    val res = req.response
    val service = Service.mk { _: Request =>
      Future.value(res)
    }

    val (in, out) = mkPair[Any, Any]
    val disp = new HttpServerDispatcher(out, service, NullStatsReceiver)

    req.response.setChunked(true)
    in.write(from(req))

    await(in.read())

    // Simulate channel closure
    out.close()
    intercept[Reader.ReaderDiscarded] { await(res.writer.write(Buf.Utf8("."))) }
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

  def ok(reader: Reader): Future[Response] =
    Future.value(Response(Version.Http11, HttpStatus.Ok, reader))
}
