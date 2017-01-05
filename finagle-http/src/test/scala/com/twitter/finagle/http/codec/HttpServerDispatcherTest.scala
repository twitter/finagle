package com.twitter.finagle.http.codec

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.time._
import com.twitter.finagle.{Service, Status}
import com.twitter.finagle.http
import com.twitter.finagle.http.{Fields, Request, Response, Version}
import com.twitter.finagle.http.netty.Netty3ServerStreamTransport
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.transport.{QueueTransport, Transport}
import com.twitter.io.Reader
import com.twitter.util.{Await, Future, Promise}
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http.{
  DefaultHttpChunk, HttpChunk, HttpResponse, HttpResponseStatus}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HttpServerDispatcherTest extends FunSuite {
  import HttpServerDispatcherTest._

  def testChunk(trans: Transport[Any, Any], chunk: HttpChunk) = {
    val f = trans.read()
    assert(!f.isDefined)
    Await.ready(trans.write(chunk), 5.seconds)
    val c = Await.result(f, 5.seconds).asInstanceOf[HttpChunk]
    assert(c.getContent == chunk.getContent)
  }

  test("invalid message") {
    val (in, out) = mkPair[Any, Any]
    val service = Service.mk { req: Request => Future.value(Response()) }
    val disp = new HttpServerDispatcher(out, service, NullStatsReceiver)

    in.write("invalid")
    Await.ready(out.onClose, 5.seconds)
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

    in.write(Request("/foo").httpMessage)
    Await.result(in.read, 5.seconds) match {
      case resp: HttpResponse =>
        assert(resp.getStatus == HttpResponseStatus.OK)
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
    in.write(req.httpRequest)
    Await.result(in.read, 5.seconds)

    testChunk(in, chunk("a"))
    testChunk(in, chunk("foo"))
    testChunk(in, HttpChunk.LAST_CHUNK)
  }

  test("client abort before dispatch") {
    val promise = new Promise[Response]
    val service = Service.mk { _: Request => promise }

    val (in, out) = mkPair[Any, Any]
    val disp = new HttpServerDispatcher(out, service, NullStatsReceiver)

    in.write(Request().httpRequest)

    // Simulate channel closure
    out.close()
    assert(promise.isInterrupted.isDefined)
  }

  test("client abort after dispatch") {
    val req = Request()
    val res = req.response
    val service = Service.mk { _: Request => Future.value(res) }

    val (in, out) = mkPair[Any, Any]
    val disp = new HttpServerDispatcher(out, service, NullStatsReceiver)

    req.response.setChunked(true)
    in.write(req.httpRequest)

    Await.result(in.read(), 5.seconds)

    // Simulate channel closure
    out.close()
    intercept[Reader.ReaderDiscarded] { Await.result(res.writer.write(buf(".")), 5.seconds) }
  }
}

object HttpServerDispatcherTest {
  def mkPair[A,B] = {
    val inQ = new AsyncQueue[Any]
    val outQ = new AsyncQueue[Any]
    (
      Transport.cast[A,B](new QueueTransport(outQ, inQ)),
      new Netty3ServerStreamTransport(new QueueTransport(inQ, outQ))
    )
  }

  def wrap(msg: String) = ChannelBuffers.wrappedBuffer(msg.getBytes("UTF-8"))
  def buf(msg: String) = ChannelBufferBuf.Owned(wrap(msg))
  def chunk(msg: String) = new DefaultHttpChunk(wrap(msg))

  def ok(reader: Reader): Future[Response] =
    Future.value(Response(Version.Http11, http.Status.Ok, reader))
}
