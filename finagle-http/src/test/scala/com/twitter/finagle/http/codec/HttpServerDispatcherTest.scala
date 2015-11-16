package com.twitter.finagle.http.codec

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.{ChannelClosedException, Service, Status}
import com.twitter.finagle.http
import com.twitter.finagle.http.{BadHttpRequest, Request, Response, Version}
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.transport.{QueueTransport, Transport}
import com.twitter.io.{Reader, Buf}
import com.twitter.util.{Await, Future, Promise}
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http.{HttpChunk, DefaultHttpChunk}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HttpServerDispatcherTest extends FunSuite {
  import HttpServerDispatcherTest._

  def testChunk(trans: Transport[Any, Any], chunk: HttpChunk) = {
    val f = trans.read()
    assert(!f.isDefined)
    Await.ready(trans.write(chunk))
    val c = Await.result(f).asInstanceOf[HttpChunk]
    assert(c.getContent == chunk.getContent)
  }

  test("invalid message") {
    val (in, out) = mkPair[Any, Any]
    val service = Service.mk { req: Request => Future.value(Response()) }
    val disp = new HttpServerDispatcher(out, service)

    in.write("invalid")
    Await.ready(out.onClose)
    assert(out.status == Status.Closed)
  }

  test("bad request") {
    val (in, out) = mkPair[Any, Any]
    val service = Service.mk { req: Request => Future.value(Response()) }
    val disp = new HttpServerDispatcher(out, service)

    in.write(BadHttpRequest(new Exception()))
    Await.result(in.read)
    assert(out.status == Status.Closed)
  }

  test("streaming request body") {
    val service = Service.mk { req: Request => ok(req.reader) }
    val (in, out) = mkPair[Any, Any]
    val disp = new HttpServerDispatcher(out, service)

    val req = Request()
    req.setChunked(true)
    in.write(req.httpRequest)
    Await.result(in.read)

    testChunk(in, chunk("a"))
    testChunk(in, chunk("foo"))
    testChunk(in, HttpChunk.LAST_CHUNK)
  }

  test("client abort before dispatch") {
    val promise = new Promise[Response]
    val service = Service.mk { _: Request => promise }

    val (in, out) = mkPair[Any, Any]
    val disp = new HttpServerDispatcher(out, service)

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
    val disp = new HttpServerDispatcher(out, service)

    req.response.setChunked(true)
    in.write(req.httpRequest)

    Await.result(in.read())

    // Simulate channel closure
    out.close()
    intercept[Reader.ReaderDiscarded] { Await.result(res.writer.write(buf("."))) }
  }
}

object HttpServerDispatcherTest {
  def mkPair[A,B] = {
    val inQ = new AsyncQueue[A]
    val outQ = new AsyncQueue[B]
    (new QueueTransport[A,B](inQ, outQ), new QueueTransport[B,A](outQ, inQ))
  }

  def wrap(msg: String) = ChannelBuffers.wrappedBuffer(msg.getBytes("UTF-8"))
  def buf(msg: String) = ChannelBufferBuf.Owned(wrap(msg))
  def chunk(msg: String) = new DefaultHttpChunk(wrap(msg))

  def ok(reader: Reader): Future[Response] =
    Future.value(Response(Version.Http11, http.Status.Ok, reader))
}
