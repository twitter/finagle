package com.twitter.finagle.http.codec

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.http.filter.HttpNackFilter
import com.twitter.finagle.{Address, Http, Name, Service, Status, http}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.http.netty.Netty3ClientStreamTransport
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import com.twitter.finagle.transport.{QueueTransport, Transport}
import com.twitter.io.{Buf, Reader}
import com.twitter.util.{Await, Closable, Duration, Future, Promise, Return, Throw, Time}
import java.net.InetSocketAddress
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.handler.codec.http.HttpResponseStatus.OK
import org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1
import org.jboss.netty.handler.codec.http.{DefaultHttpChunk, DefaultHttpResponse, HttpChunk}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.mockito.Mockito.{spy, times, verify}

object OpTransport {
  sealed trait Op[In, Out]
  case class Write[In, Out](accept: In => Boolean, res: Future[Unit]) extends Op[In, Out]
  case class Read[In, Out](res: Future[Out]) extends Op[In, Out]
  case class Close[In, Out](res: Future[Unit]) extends Op[In, Out]

  def apply[In, Out](ops: Op[In, Out]*) = new OpTransport(ops.toList)

}

class OpTransport[In, Out](_ops: List[OpTransport.Op[In, Out]]) extends Transport[In, Out] {
  import org.scalatest.Assertions._
  import OpTransport._

  var ops = _ops

  def read() = ops match {
    case Read(res) :: rest =>
      ops = rest
      res
    case _ =>
      fail(s"Expected ${ops.headOption}; got read()")
  }

  def write(in: In) = ops match {

    case Write(accept, res) :: rest =>
      if (!accept(in))
        fail(s"Did not accept write $in")

      ops = rest
      res
    case _ =>
      fail(s"Expected ${ops.headOption}; got write($in)")
  }

  def close(deadline: Time) = ops match {
    case Close(res) :: rest =>
      ops = rest
      status = Status.Closed
      res respond {
        case Return(()) =>
          onClose.setValue(new Exception("closed"))
        case Throw(exc) =>
          onClose.setValue(exc)
      }
    case _ =>
      fail(s"Expected ${ops.headOption}; got close($deadline)")
  }

  var status: Status = Status.Open
  val onClose = new Promise[Throwable]
  def localAddress = new java.net.SocketAddress{}
  def remoteAddress = new java.net.SocketAddress{}
  val peerCertificate = None
}

@RunWith(classOf[JUnitRunner])
class HttpClientDispatcherTest extends FunSuite {
  def mkPair() = {
    val inQ = new AsyncQueue[Any]
    val outQ = new AsyncQueue[Any]
    (new Netty3ClientStreamTransport(new QueueTransport[Any, Any](inQ, outQ)),
      new QueueTransport[Any, Any](outQ, inQ))
  }

  def chunk(content: String) =
    new DefaultHttpChunk(
      ChannelBuffers.wrappedBuffer(content.getBytes("UTF-8")))

  private val timeout = Duration.fromSeconds(2)

  test("streaming request body") {
    val (in, out) = mkPair()
    val disp = new HttpClientDispatcher(in, NullStatsReceiver)
    val req = Request()
    req.setChunked(true)
    val f = disp(req)
    assert(!f.isDefined)

    // discard the request immediately
    out.read()

    val r = Response().httpResponse
    r.setChunked(true)
    out.write(r)
    val res = Await.result(f, timeout)

    val c = res.reader.read(Int.MaxValue)
    assert(!c.isDefined)
    req.writer.write(Buf.Utf8("a"))
    out.read() flatMap { c => out.write(c) }
    assert(Await.result(c, timeout) === Some(Buf.Utf8("a")))

    val cc = res.reader.read(Int.MaxValue)
    assert(!cc.isDefined)
    req.writer.write(Buf.Utf8("some other thing"))
    out.read() flatMap { c => out.write(c) }
    assert(Await.result(cc, timeout) === Some(Buf.Utf8("some other thing")))

    val last = res.reader.read(Int.MaxValue)
    assert(!last.isDefined)
    req.close()
    out.read() flatMap { c => out.write(c) }
    assert(Await.result(last, timeout).isEmpty)
  }

  test("invalid message") {
    val (in, out) = mkPair()
    val disp = new HttpClientDispatcher(in, NullStatsReceiver)
    out.write("invalid message")
    intercept[ClassCastException] { Await.result(disp(Request())) }
  }

  test("not chunked") {
    val (in, out) = mkPair()
    val disp = new HttpClientDispatcher(in, NullStatsReceiver)
    val httpRes = new DefaultHttpResponse(HTTP_1_1, OK)
    val req = Request()
    val f = disp(req)
    Await.result(out.read(), timeout)
    out.write(httpRes)
    val res = Await.result(f, timeout)
    assert(res.httpResponse === httpRes)
  }

  test("chunked") {
    val (in, out) = mkPair()
    val disp = new HttpClientDispatcher(in, NullStatsReceiver)
    val httpRes = new DefaultHttpResponse(HTTP_1_1, OK)
    httpRes.setChunked(true)

    val f = disp(Request())
    out.write(httpRes)
    val reader = Await.result(f, timeout).reader

    val c = reader.read(Int.MaxValue)
    out.write(chunk("hello"))
    assert(Await.result(c, timeout) === Some(Buf.Utf8("hello")))

    val cc = reader.read(Int.MaxValue)
    out.write(chunk("world"))
    assert(Await.result(cc, timeout) === Some(Buf.Utf8("world")))

    out.write(HttpChunk.LAST_CHUNK)
    assert(Await.result(reader.read(Int.MaxValue), timeout).isEmpty)
  }

  test("error mid-chunk") {
    val (in, out) = mkPair()
    val inSpy = spy(in)
    val disp = new HttpClientDispatcher(inSpy, NullStatsReceiver)
    val httpRes = new DefaultHttpResponse(HTTP_1_1, OK)
    httpRes.setChunked(true)

    val f = disp(Request())
    out.write(httpRes)
    val reader = Await.result(f, timeout).reader

    val c = reader.read(Int.MaxValue)
    out.write(chunk("hello"))
    assert(Await.result(c, timeout) === Some(Buf.Utf8("hello")))

    val cc = reader.read(Int.MaxValue)
    out.write("something else")
    intercept[IllegalArgumentException] { Await.result(cc, timeout) }
    verify(inSpy, times(1)).close()
  }

  test("upstream interrupt: before write") {
    import OpTransport._

    val writep = new Promise[Unit]
    val readp = new Promise[Unit]
    val transport = OpTransport[Any, Any](
      Read(readp),
      Write(Function.const(true), writep),
      Close(Future.Done))

    val disp = new HttpClientDispatcher(new Netty3ClientStreamTransport(transport), NullStatsReceiver)
    val req = Request()
    req.setChunked(true)

    val f = disp(req)
    val g = req.writer.write(Buf.Utf8(".."))
    assert(transport.status == Status.Open)
    assert(!g.isDefined)
    f.raise(new Exception)
    assert(transport.status == Status.Closed)

    assert(!g.isDefined)
    // Simulate what a real transport would do:
    assert(transport.ops.isEmpty)
    writep.setException(new Exception)

    assert(g.isDefined)
    intercept[Reader.ReaderDiscarded] { Await.result(g, timeout) }
  }

  test("upstream interrupt: during req stream (read)") {
    import OpTransport._

    val readp = new Promise[Nothing]
    val transport = OpTransport[Any, Any](
      // Read the response
      Read(readp),
      // Write the initial request.
      Write(_.isInstanceOf[HttpRequest], Future.Done),
      Close(Future.Done))

    val disp = new HttpClientDispatcher(new Netty3ClientStreamTransport(transport), NullStatsReceiver)
    val req = Request()
    req.setChunked(true)

    val f = disp(req)

    assert(transport.status == Status.Open)
    f.raise(new Exception)
    assert(transport.status == Status.Closed)

    // Simulate what a real transport would do:
    assert(transport.ops.isEmpty)
    readp.setException(new Exception)

    // The reader is now discarded
    intercept[Reader.ReaderDiscarded] {
      Await.result(req.writer.write(Buf.Utf8(".")), timeout)
    }
  }

  test("upstream interrupt: during req stream (write)") {
    import OpTransport._

    val chunkp = new Promise[Unit]
    val transport = OpTransport[Any, Any](
      // Read the response
      Read(Future.never),
      // Write the initial request.
      Write(_.isInstanceOf[HttpRequest], Future.Done),
      // Then we try to write the chunk
      Write(_.isInstanceOf[HttpChunk], chunkp),
      Close(Future.Done))

    val disp = new HttpClientDispatcher(new Netty3ClientStreamTransport(transport), NullStatsReceiver)
    val req = Request()
    req.setChunked(true)

    val f = disp(req)

    // Buffer up for write.
    Await.result(req.writer.write(Buf.Utf8("..")), timeout)

    assert(transport.status == Status.Open)
    f.raise(new Exception)
    assert(transport.status == Status.Closed)

    // Simulate what a real transport would do:
    assert(transport.ops.isEmpty)
    chunkp.setException(new Exception)

    // The reader is now discarded
    intercept[Reader.ReaderDiscarded] {
      Await.result(req.writer.write(Buf.Utf8(".")), timeout)
    }
  }

  test("swallows the body of a HttpNack if it happens to come as a chunked response") {
    new NackCtx {
      def nackBody: Buf = Buf.Utf8("Chunked nack body")

      assert(Await.result(client(request), timeout).status == http.Status.Ok)
      assert(serverSr.counters(Seq("myservice", "nacks")) == 1)
      assert(clientSr.counters(Seq("http", "retries", "requeues")) == 1)

      // reuse connections
      assert(Await.result(client(request), timeout).status == http.Status.Ok)
      assert(clientSr.counters(Seq("http", "connects")) == 1)
      assert(serverSr.counters(Seq("myservice", "nacks")) == 1)

      Closable.all(client, server).close()
    }
  }

  test("fails on excessively large nack response") {
    new NackCtx {
      def nackBody: Buf = Buf.Utf8("Very large" * 1024)

      assert(Await.result(client(request), timeout).status == http.Status.Ok)

      // Should have closed the connection on the first nack
      assert(clientSr.counters(Seq("http", "connects")) == 2)
      assert(serverSr.counters(Seq("myservice", "nacks")) == 1)

      Closable.all(client, server).close()
    }
  }

  // Scaffold for checking nack behavior
  private abstract class NackCtx {
    def nackBody: Buf
    val serverSr = new InMemoryStatsReceiver
    val clientSr = new InMemoryStatsReceiver
    @volatile var needsNack = true
    val service = Service.mk{ _: Request =>
      val resp =
        if (needsNack) {
          needsNack = false
          // simulate a nack response with a chunked body by just sending a chunked body
          serverSr.scope("myservice").counter("nacks").incr()
          val resp = Response(status = HttpNackFilter.ResponseStatus)
          resp.headerMap.set(HttpNackFilter.RetryableNackHeader, "true")
          resp.setChunked(true)
          resp.writer.write(nackBody)
            .before(resp.writer.close())
          resp
        } else {
          val resp = Response()
          resp.contentString = "the body"
          resp
        }

      Future.value(resp)
    }

    val server =
      Http.server
        .withStatsReceiver(serverSr)
        .withLabel("myservice")
        .withStreaming(true)
        .serve(new InetSocketAddress(0), service)
    val client =
      Http.client
        .withStatsReceiver(clientSr)
        .withStreaming(true)
        .newService(Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), "http")

    val request = Request("/")
  }
}

