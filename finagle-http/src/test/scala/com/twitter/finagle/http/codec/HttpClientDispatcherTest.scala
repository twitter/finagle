package com.twitter.finagle.http.codec

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.{TimeoutException, Dtab, Status, Failure}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.http.netty.Bijections._
import com.twitter.finagle.transport.{Transport, QueueTransport}
import com.twitter.io.{Buf, Reader}
import com.twitter.util.{Await, Duration, Time, Promise, Future, Return, Throw}
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
  def mkPair[A,B] = {
    val inQ = new AsyncQueue[A]
    val outQ = new AsyncQueue[B]
    (new QueueTransport[A,B](inQ, outQ), new QueueTransport[B,A](outQ, inQ))
  }

  def chunk(content: String) =
    new DefaultHttpChunk(
      ChannelBuffers.wrappedBuffer(content.getBytes("UTF-8")))

  private val timeout = Duration.fromSeconds(2)

  test("streaming request body") {
    val (in, out) = mkPair[Any,Any]
    val disp = new HttpClientDispatcher(in)
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
    val (in, out) = mkPair[Any,Any]
    val disp = new HttpClientDispatcher(in)
    out.write("invalid message")
    intercept[IllegalArgumentException] { Await.result(disp(Request())) }
  }

  test("not chunked") {
    val (in, out) = mkPair[Any,Any]
    val disp = new HttpClientDispatcher(in)
    val httpRes = new DefaultHttpResponse(HTTP_1_1, OK)
    val req = Request()
    val f = disp(req)
    Await.result(out.read(), timeout)
    out.write(httpRes)
    val res = Await.result(f, timeout)
    assert(res.httpResponse === httpRes)
  }

  test("chunked") {
    val (in, out) = mkPair[Any,Any]
    val disp = new HttpClientDispatcher(in)
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
    val (in, out) = mkPair[Any,Any]
    val inSpy = spy(in)
    val disp = new HttpClientDispatcher(inSpy)
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
    val transport = OpTransport[Any, Any](
      Write(Function.const(true), writep),
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
      Write(_.isInstanceOf[HttpRequest], Future.Done),
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
      Write(_.isInstanceOf[HttpRequest], Future.Done),
      // Read the response
      Read(Future.never),
      // Then we try to write the chunk
      Write(_.isInstanceOf[HttpChunk], chunkp),
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
    val recvNettyReq = Await.result(out.read()).asInstanceOf[HttpRequest]

    // apply the bijection to convert the netty request to a finagle one
    val recvFinagleReq = from[HttpRequest, Request](recvNettyReq)

    // extract the dtab from the sent request
    val recvDtab = HttpDtab.read(recvFinagleReq).get()

    // send back an http ok to the dispatcher
    val sentResult = new DefaultHttpResponse(HTTP_1_1, OK)
    out.write(sentResult)

    // block until the dispatcher presents us with the result
    val recvResult = Await.result(futureResult, timeout)

    // ensure that no dtabs were received
    assert(recvDtab.length == 0)

    // ensure that the sent and received http requests are identical
    assert(recvResult.httpResponse == sentResult)
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
    val recvNettyReq = Await.result(out.read()).asInstanceOf[HttpRequest]

    // apply the bijection to convert the netty request to a finagle one
    val recvFinagleReq = from[HttpRequest, Request](recvNettyReq)

    // extract the dtab from the sent request
    val recvDtab = HttpDtab.read(recvFinagleReq).get()

    // send back an http ok to the dispatcher
    val sentResult = new DefaultHttpResponse(HTTP_1_1, OK)
    out.write(sentResult)

    // block until the dispatcher presents us with the result
    val recvResult = Await.result(futureResult, timeout)

    // ensure that no dtabs were received
    assert(recvDtab.length == 0)

    // ensure that the sent and received http requests are identical
    assert(recvResult.httpResponse == sentResult)
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
      val recvNettyReq = Await.result(out.read()).asInstanceOf[HttpRequest]

      // apply the bijection to convert the netty request to a finagle one
      val recvFinagleReq = from[HttpRequest, Request](recvNettyReq)

      // extract the dtab from the sent request
      val recvDtab = HttpDtab.read(recvFinagleReq).get()

      // send back an http ok to the dispatcher
      val sentResult = new DefaultHttpResponse(HTTP_1_1, OK)
      out.write(sentResult)

      // block until the dispatcher presents us with the result
      val recvResult = Await.result(futureResult, timeout)

      // ensure that the sent and received dtabs are identical
      assert(sentDtab == recvDtab)

      // ensure that the sent and received http requests are identical
      assert(recvResult.httpResponse == sentResult)
    }
  }
}

