package com.twitter.finagle.http.codec

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.transport.QueueTransport
import com.twitter.io.{Buf, Reader}
import com.twitter.util.{Await, Future, Promise, Return}
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http.HttpResponseStatus.OK
import org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1
import org.jboss.netty.handler.codec.http.{DefaultHttpChunk, DefaultHttpResponse, HttpChunk}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.mockito.Mockito.{spy, times, verify}

@RunWith(classOf[JUnitRunner])
class HttpClientDispatcherTest extends FunSuite {
  def mkPair[A,B] = {
    val inQ = new AsyncQueue[A]
    val outQ = new AsyncQueue[B]
    (new QueueTransport[A,B](inQ, outQ), new QueueTransport[B,A](outQ, inQ))
  }

  def chunk(content: String) =
    new DefaultHttpChunk(
      ChannelBuffers.wrappedBuffer(content.getBytes("UTF-8")))

  test("streaming request body") {
    val (in, out) = mkPair[Any,Any]
    val disp = new HttpClientDispatcher[Request](in)
    val req = Request()
    req.setChunked(true)
    val f = disp(req)
    assert(!f.isDefined)

    // discard the request immediately
    out.read()

    val r = Response()
    r.setChunked(true)
    out.write(r)
    val res = Await.result(f)

    val c = res.reader.read(Int.MaxValue)
    assert(!c.isDefined)
    req.writer.write(Buf.Utf8("a"))
    out.read() flatMap { c => out.write(c) }
    assert(Await.result(c) === Buf.Utf8("a"))

    val cc = res.reader.read(Int.MaxValue)
    assert(!cc.isDefined)
    req.writer.write(Buf.Utf8("some other thing"))
    out.read() flatMap { c => out.write(c) }
    assert(Await.result(cc) === Buf.Utf8("some other thing"))

    val last = res.reader.read(Int.MaxValue)
    assert(!last.isDefined)
    req.close()
    out.read() flatMap { c => out.write(c) }
    assert(Await.result(last) === Buf.Eof)
  }

  test("invalid message") {
    val (in, out) = mkPair[Any,Any]
    val disp = new HttpClientDispatcher[Request](in)
    out.write("invalid message")
    intercept[IllegalArgumentException] { Await.result(disp(Request())) }
  }

  test("not chunked") {
    val (in, out) = mkPair[Any,Any]
    val disp = new HttpClientDispatcher[Request](in)
    val httpRes = new DefaultHttpResponse(HTTP_1_1, OK)
    val req = Request()
    val f = disp(req)
    assert(Await.result(out.read()) === req)
    out.write(httpRes)
    val res = Await.result(f)
    assert(res.httpResponse === httpRes)
  }

  test("chunked") {
    val (in, out) = mkPair[Any,Any]
    val disp = new HttpClientDispatcher[Request](in)
    val httpRes = new DefaultHttpResponse(HTTP_1_1, OK)
    httpRes.setChunked(true)

    val f = disp(Request())
    out.write(httpRes)
    val reader = Await.result(f).reader

    val c = reader.read(Int.MaxValue)
    out.write(chunk("hello"))
    assert(Await.result(c) === Buf.Utf8("hello"))

    val cc = reader.read(Int.MaxValue)
    out.write(chunk("world"))
    assert(Await.result(cc) === Buf.Utf8("world"))

    out.write(HttpChunk.LAST_CHUNK)
    assert(Await.result(reader.read(Int.MaxValue)) === Buf.Eof)
  }

  test("error mid-chunk") {
    val (in, out) = mkPair[Any,Any]
    val inSpy = spy(in)
    val disp = new HttpClientDispatcher[Request](inSpy)
    val httpRes = new DefaultHttpResponse(HTTP_1_1, OK)
    httpRes.setChunked(true)

    val f = disp(Request())
    out.write(httpRes)
    val reader = Await.result(f).reader

    val c = reader.read(Int.MaxValue)
    out.write(chunk("hello"))
    assert(Await.result(c) === Buf.Utf8("hello"))

    val cc = reader.read(Int.MaxValue)
    out.write("something else")
    intercept[IllegalArgumentException] { Await.result(cc) }
    verify(inSpy, times(1)).close()
  }

  test("upstream interrupt on request triggers discard") {
    val (in, out) = mkPair[Any,Any]
    val inSpy = spy(in)
    val disp = new HttpClientDispatcher[Request](inSpy)

    val discardp = new Promise[Unit]
    val reqIn = Request()
    val req = new Request {
      val httpRequest = reqIn
      override val httpMessage = reqIn
      lazy val remoteSocketAddress = reqIn.remoteSocketAddress
      override val reader = new Reader {
        def read(n: Int) = Future.value(Buf.Eof)
        def discard() {
          discardp.setDone()
        }
      }
    }
    req.setChunked(true)

    val f = disp(req)
    f.raise(new Exception)
    assert(discardp.poll === Some(Return.Unit))
  }
}
