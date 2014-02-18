package com.twitter.finagle.http.codec

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.{ChannelClosedException, Service}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.transport.QueueTransport
import com.twitter.io.Reader
import com.twitter.util.{Await, Future, Promise}
import org.jboss.netty.buffer.ChannelBuffers
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HttpServerDispatcherTest extends FunSuite {
  def mkPair[A,B] = {
    val inQ = new AsyncQueue[A]
    val outQ = new AsyncQueue[B]
    (new QueueTransport[A,B](inQ, outQ), new QueueTransport[B,A](outQ, inQ))
  }

  def buf(msg: String) =
    ChannelBufferBuf(ChannelBuffers.wrappedBuffer(msg.getBytes("UTF-8")))

  test("client abort before dispatch") {
    val promise = new Promise[Response]
    val service = Service.mk { _: Request => promise }

    val (in, out) = mkPair[Any, Any]
    val disp = new HttpServerDispatcher[Request](out, service)

    in.write(Request())

    // Simulate channel closure
    out.close()
    assert(promise.isInterrupted.isDefined)
  }

  test("client abort after dispatch") {
    val req = Request()
    val res = req.response
    val service = Service.mk { _: Request => Future.value(res) }

    val (in, out) = mkPair[Any, Any]
    val disp = new HttpServerDispatcher[Request](out, service)

    req.response.setChunked(true)
    in.write(req)

    assert(Await.result(in.read()) === res)

    // Simulate channel closure
    out.close()
    intercept[Reader.ReaderDiscarded] { Await.result(res.writer.write(buf("."))) }
  }
}
