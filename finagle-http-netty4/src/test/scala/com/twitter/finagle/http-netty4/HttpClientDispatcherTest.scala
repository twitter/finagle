package com.twitter.finagle.http4

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.time._
import com.twitter.finagle.http.codec.HttpDtab
import com.twitter.finagle.http.{Response, Request}
import com.twitter.finagle.netty4.BufAsByteBuf
import com.twitter.finagle.transport.{Transport, QueueTransport}
import com.twitter.finagle.{WriteException,  Dtab, Status}
import com.twitter.io.{Reader, Buf}
import com.twitter.util._
import io.netty.buffer.Unpooled
import io.netty.handler.codec.http.DefaultFullHttpResponse
import io.netty.handler.codec.{http => NettyHttp}
import java.net.SocketAddress
import java.security.cert.Certificate
import org.junit.runner.RunWith
import org.mockito.Mockito.{spy, times, verify}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HttpClientDispatcherTest extends FunSuite {
  def mkPair[A,B] = {
    val inQ = new AsyncQueue[A]
    val outQ = new AsyncQueue[B]
    (new QueueTransport[A,B](inQ, outQ), new QueueTransport[B,A](outQ, inQ))
  }

  private val timeout = Duration.fromSeconds(2)

  test("streaming request body") {
    val (clientT, serverT) = mkPair[Any,Any]
    val disp = new HttpClientDispatcher(clientT)
    val req = Request()
    req.setChunked(true)
    val f = disp(req)
    assert(!f.isDefined)

    // discard the request immediately
    serverT.read()

    val r: NettyHttp.HttpContent =
      new NettyHttp.DefaultHttpContent(Unpooled.EMPTY_BUFFER)

    serverT.write(r)
    val res = Await.result(f, timeout)

    val c = res.reader.read(Int.MaxValue)
    assert(!c.isDefined)
    req.writer.write(Buf.Utf8("a"))
    serverT.read().flatMap { c => serverT.write(c) }
    assert(Await.result(c, timeout) == Some(Buf.Utf8("a")))

    val cc = res.reader.read(Int.MaxValue)
    assert(!cc.isDefined)
    req.writer.write(Buf.Utf8("some other thing"))
    serverT.read() flatMap { c => serverT.write(c) }
    assert(Await.result(cc, timeout) == Some(Buf.Utf8("some other thing")))

    val last = res.reader.read(Int.MaxValue)
    assert(!last.isDefined)
    req.close()
    serverT.read() flatMap { c => serverT.write(c) }
    assert(Await.result(last, timeout).isEmpty)
  }

  test("invalid message") {
    val (in, out) = mkPair[Any,Any]
    val disp = new HttpClientDispatcher(in)
    out.write("invalid message")
    intercept[IllegalArgumentException] { Await.result(disp(Request()), timeout) }
  }

  def respEquiv(fin: Response, netty: NettyHttp.HttpResponse): Boolean = {
    val convert = Bijections.netty.responseToFinagle(netty)
    convert.status == fin.status &&
    convert.headerMap == fin.headerMap &&
    convert.content == fin.content
  }

  test("unchunked response") {
    val (clientT, serverT) = mkPair[Any,Any]
    val disp = new HttpClientDispatcher(clientT)
    val httpRes =
      new DefaultFullHttpResponse(
        NettyHttp.HttpVersion.HTTP_1_1,
        NettyHttp.HttpResponseStatus.OK,
        Unpooled.wrappedBuffer("hello".getBytes("UTF-8"))
      )
    val req = Request()
    val f = disp(req)
    Await.result(serverT.read(), timeout)
    serverT.write(httpRes)
    val res = Await.result(f, timeout)
    assert(respEquiv(res, httpRes))
  }

  def chunk(content: String): NettyHttp.HttpContent =
    new NettyHttp.DefaultHttpContent(
      BufAsByteBuf.Owned(Buf.Utf8(content))
    )

  test("chunked response") {
    val (in, out) = mkPair[Any,Any]
    val disp = new HttpClientDispatcher(in)
    val httpRes: NettyHttp.HttpContent =
      new NettyHttp.DefaultHttpContent(Unpooled.EMPTY_BUFFER)

    val f = disp(Request())
    out.write(httpRes)
    val responseReader = Await.result(f, timeout).reader

    val c = responseReader.read(Int.MaxValue)
    out.write(chunk("hello"))
    assert(Await.result(c, timeout) == Some(Buf.Utf8("hello")))

    val cc = responseReader.read(Int.MaxValue)
    out.write(chunk("world"))
    assert(Await.result(cc, timeout) == Some(Buf.Utf8("world")))

    out.write(NettyHttp.LastHttpContent.EMPTY_LAST_CONTENT)
    assert(Await.result(responseReader.read(Int.MaxValue), timeout).isEmpty)
  }

  test("error in the middle of a chunked response") {
    val (clientT, serverT) = mkPair[Any,Any]
    val clientTSpy = spy(clientT)
    val disp = new HttpClientDispatcher(clientTSpy)
    val httpRes: NettyHttp.HttpContent =
      new NettyHttp.DefaultHttpContent(Unpooled.EMPTY_BUFFER)

    val f = disp(Request())
    serverT.write(httpRes)
    val responseReader = Await.result(f, timeout).reader

    val c = responseReader.read(Int.MaxValue)
    serverT.write(chunk("hello"))
    assert(Await.result(c, timeout) == Some(Buf.Utf8("hello")))

    val cc = responseReader.read(Int.MaxValue)

    // server writes an invalid message which triggers
    // transport closure and the clientside reader to
    // fail reads.
    serverT.write("invalid message")
    intercept[IllegalArgumentException] { Await.result(cc, timeout) }
    verify(clientTSpy, times(1)).close()
  }

  test("upstream interrupt: before write") {
    import OpTransport._

    val writep = new Promise[Unit]
    val transport: OpTransport[Any, Any] = OpTransport[Any, Any](
      Write(_ => true, writep),
      Close(Future.Done))

    val disp = new HttpClientDispatcher(transport)
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
      // First write the initial request.
      Write(_.isInstanceOf[NettyHttp.HttpRequest], Future.Done),
      // Read the response
      Read(readp),
      Close(Future.Done))

    val disp = new HttpClientDispatcher(transport)
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
      // First write the initial request.
      Write(_.isInstanceOf[NettyHttp.HttpRequest], Future.Done),
      // Read the response
      Read(Future.never),
      // Then we try to write the chunk
      Write(_.isInstanceOf[NettyHttp.HttpContent], chunkp),
      Close(Future.Done))

    val disp = new HttpClientDispatcher(transport)
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

  test("ensure denial of new-style dtab headers") {
    // create a test dispatcher and its transport mechanism
    val (in, out) = mkPair[Any,Any]
    val dispatcher = new HttpClientDispatcher(in)

    // prepare a request and add a correct looking new-style dtab header
    val sentRequest = Request()
    sentRequest.headers().add("dtab-local", "/srv=>/srv#/staging")

    // dispatch the request
    val futureResult = dispatcher(sentRequest)

    // get the netty request out of the other end of the transporter
    val recvNettyReq = Await.result(out.read()).asInstanceOf[NettyHttp.FullHttpRequest]

    // apply the bijection to convert the netty request to a finagle one
    val recvFinagleReq = Bijections.netty.requestToFinagle(recvNettyReq)

    // extract the dtab from the sent request
    val recvDtab = HttpDtab.read(recvFinagleReq).get()

    // send back an http ok to the dispatcher
    val sentResult = new NettyHttp.DefaultHttpResponse(
      NettyHttp.HttpVersion.HTTP_1_1,
      NettyHttp.HttpResponseStatus.OK
    )
    out.write(sentResult)

    // block until the dispatcher presents us with the result
    val recvResult = Await.result(futureResult, timeout)

    // ensure that no dtabs were received
    assert(recvDtab.length == 0)

    // ensure that the sent and received http requests are identical
    assert(respEquiv(recvResult, sentResult))
  }

  test("ensure denial of old-style dtab headers") {
    // create a test dispatcher and its transport mechanism
    val (in, out) = mkPair[Any,Any]
    val dispatcher = new HttpClientDispatcher(in)

    // prepare a request and add a correct looking new-style dtab header
    val sentRequest = Request()
    sentRequest.headers().add("x-dtab-00-a", "/s/foo")
    sentRequest.headers().add("x-dtab-00-b", "/s/bar")

    // dispatch the request
    val futureResult = dispatcher(sentRequest)

    // get the netty request out of the other end of the transporter
    val recvNettyReq = Await.result(out.read()).asInstanceOf[NettyHttp.FullHttpRequest]

    // apply the bijection to convert the netty request to a finagle one
    val recvFinagleReq = Bijections.netty.requestToFinagle(recvNettyReq)

    // extract the dtab from the sent request
    val recvDtab = HttpDtab.read(recvFinagleReq).get()

    // send back an http ok to the dispatcher
    val sentResult = new NettyHttp.DefaultHttpResponse(
      NettyHttp.HttpVersion.HTTP_1_1,
      NettyHttp.HttpResponseStatus.OK
    )
    out.write(sentResult)

    // block until the dispatcher presents us with the result
    val recvResult = Await.result(futureResult, timeout)

    // ensure that no dtabs were received
    assert(recvDtab.length == 0)

    // ensure that the sent and received http requests are identical
    assert(respEquiv(recvResult, sentResult))
  }

  test("ensure transmission of dtab local") {
    Dtab.unwind {
      // create a test dispatcher and its transport mechanism
      val (in, out) = mkPair[Any,Any]
      val dispatcher = new HttpClientDispatcher(in)

      // prepare a request and a simple dtab with one dentry
      val sentRequest = Request()
      val sentDtab = Dtab.read("/s => /srv/smf1")

      // augment dtab.local and dispatch the request
      Dtab.local ++= sentDtab
      val futureResult = dispatcher(sentRequest)

      // get the netty request out of the other end of the transporter
      val recvNettyReq = Await.result(out.read()).asInstanceOf[NettyHttp.FullHttpRequest]

      // apply the bijection to convert the netty request to a finagle one
      val recvFinagleReq = Bijections.netty.requestToFinagle(recvNettyReq)

      // extract the dtab from the sent request
      val recvDtab = HttpDtab.read(recvFinagleReq).get()

      // send back an http ok to the dispatcher
      val sentResult = new NettyHttp.DefaultHttpResponse(
        NettyHttp.HttpVersion.HTTP_1_1,
        NettyHttp.HttpResponseStatus.OK
      )
      out.write(sentResult)

      // block until the dispatcher presents us with the result
      val recvResult = Await.result(futureResult, timeout)

      // ensure that the sent and received dtabs are identical
      assert(sentDtab == recvDtab)

      // ensure that the sent and received http requests are identical
      assert(respEquiv(recvResult, sentResult))
    }
  }

  val failingT = new Transport[Any, Any] {
    val closep = new Promise[Throwable]
    def write(req: Any): Future[Unit] = Future.exception(new RuntimeException("oh no"))

    def remoteAddress: SocketAddress = ???

    def peerCertificate: Option[Certificate] = ???

    def localAddress: SocketAddress = new java.net.SocketAddress{}

    def status: Status = ???

    def read(): Future[Any] = ???

    val onClose: Future[Throwable] = closep

    def close(deadline: Time): Future[Unit] = {
      closep.setException(new Exception("closed"))
      closep.unit
    }
  }

  test("write failures are wrapped as WriteExceptions") {
    val disp = new HttpClientDispatcher(failingT)
    val d = disp(Request())
    intercept[WriteException] { Await.result(d, 2.seconds) }
  }

  val stallT = new Transport[Any, Any] {

    def write(req: Any): Future[Unit] = Future.never

    def remoteAddress: SocketAddress = ???

    def peerCertificate: Option[Certificate] = ???

    def localAddress: SocketAddress = new java.net.SocketAddress{}

    def status: Status = ???

    def read(): Future[Any] = Future.never

    val onClose: Future[Throwable] = Future.exception(new Exception)

    def close(deadline: Time): Future[Unit] = Future.Done

  }
  test("pending requests are failed") {
    val disp = new HttpClientDispatcher(stallT)

    val d1 = disp(Request())
    val d2 = disp(Request())
    val d3 = disp(Request())
    Await.ready(disp.close(), 2.seconds)


    Thread.sleep(2000)
    val x = 123
  }

  object OpTransport {
    sealed trait Op[In, Out]
    case class Write[In, Out](accept: In => Boolean, res: Future[Unit]) extends Op[In, Out]
    case class Read[In, Out](res: Future[Out]) extends Op[In, Out]
    case class Close[In, Out](res: Future[Unit]) extends Op[In, Out]

    def apply[In, Out](ops: Op[In, Out]*) = new OpTransport(ops.toList)

  }

  class OpTransport[In, Out](_ops: List[OpTransport.Op[In, Out]]) extends Transport[In, Out] {
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
}