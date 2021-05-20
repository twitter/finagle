package com.twitter.finagle.http.codec

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.Status
import com.twitter.finagle.http.{Request, Response, Version, Status => HttpStatus}
import com.twitter.finagle.netty4.http.{Bijections, Netty4ClientStreamTransport}
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import com.twitter.finagle.transport.{
  QueueTransport,
  SimpleTransportContext,
  Transport,
  TransportContext
}
import com.twitter.io.{Buf, ReaderDiscardedException}
import com.twitter.util.{Await, Awaitable, Duration, Future, Promise, Return, Throw, Time}
import io.netty.buffer.Unpooled
import io.netty.handler.codec.http.{
  DefaultHttpContent,
  HttpContent,
  HttpRequest,
  HttpUtil,
  LastHttpContent
}
import java.nio.charset.StandardCharsets
import org.mockito.Mockito.{spy, times, verify}
import org.scalatest.funsuite.AnyFunSuite

object OpTransport {
  sealed trait Op[In, Out]
  case class Write[In, Out](accept: In => Boolean, res: Future[Unit]) extends Op[In, Out]
  case class Read[In, Out](res: Future[Out]) extends Op[In, Out]
  case class Close[In, Out](res: Future[Unit]) extends Op[In, Out]

  def apply[In, Out](ops: Op[In, Out]*) = new OpTransport(ops.toList)

}

class OpTransport[In, Out](var ops: List[OpTransport.Op[In, Out]]) extends Transport[In, Out] {
  import OpTransport._
  import org.scalatest.Assertions._

  type Context = TransportContext

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
  val context = new SimpleTransportContext()
}

// Note: We need a concrete impl to test it so the finagle-http package is most
// appropriate even though the implementation is in finagle-base-http.
class HttpClientDispatcherTest extends AnyFunSuite {

  private[this] def await[T](t: Awaitable[T]): T = Await.result(t, Duration.fromSeconds(15))

  private[this] def mkPair() = {
    val inQ = new AsyncQueue[Any]
    val outQ = new AsyncQueue[Any]
    (
      new Netty4ClientStreamTransport(new QueueTransport[Any, Any](inQ, outQ)),
      new QueueTransport[Any, Any](outQ, inQ)
    )
  }

  private[this] def chunk(content: String): HttpContent = {
    val bytes = content.getBytes(StandardCharsets.UTF_8)
    val buf = Unpooled.buffer(bytes.length)
    buf.writeBytes(bytes)
    new DefaultHttpContent(buf)
  }

  test("streaming request body") {
    val (in, out) = mkPair()
    val disp = new HttpClientDispatcher(in, NullStatsReceiver)
    val req = Request()
    req.setChunked(true)
    val f = disp(req)
    assert(!f.isDefined)

    // discard the request immediately
    out.read()

    val r = Bijections.finagle.chunkedResponseToNetty(Response())
    HttpUtil.setTransferEncodingChunked(r, true)
    out.write(r)
    val res = await(f)

    val c = res.reader.read()
    assert(!c.isDefined)
    req.writer.write(Buf.Utf8("a"))
    out.read() flatMap { c => out.write(c) }
    assert(await(c) === Some(Buf.Utf8("a")))

    val cc = res.reader.read()
    assert(!cc.isDefined)
    req.writer.write(Buf.Utf8("some other thing"))
    out.read() flatMap { c => out.write(c) }
    assert(await(cc) === Some(Buf.Utf8("some other thing")))

    val last = res.reader.read()
    assert(!last.isDefined)
    req.close()
    out.read() flatMap { c => out.write(c) }
    assert(await(last).isEmpty)
  }

  test("invalid message") {
    val statsReceiver = new InMemoryStatsReceiver()
    val (in, out) = mkPair()
    val disp = new HttpClientDispatcher(in, statsReceiver)
    out.write("invalid message")
    intercept[IllegalArgumentException] { Await.result(disp(Request())) }

    assert(statsReceiver.counters(Seq("stream", "failures")) == 1)
    assert(
      statsReceiver.counters(Seq("stream", "failures", "java.lang.IllegalArgumentException")) == 1)
  }

  test("not chunked") {
    val (in, out) = mkPair()
    val disp = new HttpClientDispatcher(in, NullStatsReceiver)
    val httpRes = Bijections.finagle.fullResponseToNetty(Response())
    val req = Request()
    val f = disp(req)
    await(out.read())
    out.write(httpRes)
    val res = await(f)
    assert(res.status == HttpStatus.Ok)
    assert(res.version == Version.Http11)
    assert(!res.isChunked)
  }

  test("chunked") {
    val (in, out) = mkPair()
    val disp = new HttpClientDispatcher(in, NullStatsReceiver)
    val httpRes = Bijections.finagle.chunkedResponseToNetty(Response())
    HttpUtil.setTransferEncodingChunked(httpRes, true)

    val f = disp(Request())
    out.write(httpRes)
    val reader = await(f).reader

    val c = reader.read()
    out.write(chunk("hello"))
    assert(await(c) == Some(Buf.Utf8("hello")))

    val cc = reader.read()
    out.write(chunk("world"))
    assert(await(cc) == Some(Buf.Utf8("world")))

    out.write(LastHttpContent.EMPTY_LAST_CONTENT)
    assert(await(reader.read()).isEmpty)
  }

  test("error mid-chunk") {
    val (in, out) = mkPair()
    val inSpy = spy(in)
    val disp = new HttpClientDispatcher(inSpy, NullStatsReceiver)
    val httpRes = Bijections.finagle.chunkedResponseToNetty(Response())
    HttpUtil.setTransferEncodingChunked(httpRes, true)

    val f = disp(Request())
    out.write(httpRes)
    val reader = await(f).reader

    val c = reader.read()
    out.write(chunk("hello"))
    assert(await(c) == Some(Buf.Utf8("hello")))

    val cc = reader.read()
    out.write("something else")
    intercept[IllegalArgumentException] { await(cc) }
    verify(inSpy, times(1)).close()
  }

  test("upstream interrupt: before write") {
    import OpTransport._

    val writep = new Promise[Unit]
    val readp = new Promise[Unit]
    val transport =
      OpTransport[Any, Any](Write(Function.const(true), writep), Read(readp), Close(Future.Done))

    val disp =
      new HttpClientDispatcher(new Netty4ClientStreamTransport(transport), NullStatsReceiver)
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
    intercept[ReaderDiscardedException] { await(g) }
  }

  test("upstream interrupt: during req stream (read)") {
    import OpTransport._

    val readp = new Promise[Nothing]
    val transport = OpTransport[Any, Any](
      // Write the initial request.
      Write(_.isInstanceOf[HttpRequest], Future.Done),
      // Read the response
      Read(readp),
      Close(Future.Done)
    )

    val disp =
      new HttpClientDispatcher(new Netty4ClientStreamTransport(transport), NullStatsReceiver)
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
    intercept[ReaderDiscardedException] {
      await(req.writer.write(Buf.Utf8(".")))
    }
  }

  test("upstream interrupt: during req stream (write)") {
    import OpTransport._

    val chunkp = new Promise[Unit]
    val transport = OpTransport[Any, Any](
      // Write the initial request.
      Write(_.isInstanceOf[HttpRequest], Future.Done),
      // Read the response
      Read(Future.never),
      // Then we try to write the chunk
      Write(_.isInstanceOf[HttpContent], chunkp),
      Close(Future.Done)
    )

    val disp =
      new HttpClientDispatcher(new Netty4ClientStreamTransport(transport), NullStatsReceiver)
    val req = Request()
    req.setChunked(true)

    val f = disp(req)

    // Buffer up for write.
    await(req.writer.write(Buf.Utf8("..")))

    assert(transport.status == Status.Open)
    f.raise(new Exception)
    assert(transport.status == Status.Closed)

    // Simulate what a real transport would do:
    assert(transport.ops.isEmpty)
    chunkp.setException(new Exception)

    // The reader is now discarded
    intercept[ReaderDiscardedException] {
      await(req.writer.write(Buf.Utf8(".")))
    }
  }
}
