package com.twitter.finagle.http

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.http.codec.Http1ConnectionManager
import com.twitter.finagle.transport.QueueTransport
import com.twitter.finagle.{Status => CoreStatus}
import com.twitter.io.Buf
import com.twitter.io.Pipe
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Throw
import com.twitter.util.Time
import org.scalatest.funsuite.AnyFunSuite

class HttpTransportTest extends AnyFunSuite {

  test("exceptions in connection manager stay within Future context") {
    val exc = new IllegalArgumentException("boo")
    val underlying = new QueueTransport(new AsyncQueue[Request], new AsyncQueue[Response])
    val noop = new IdentityStreamTransport(underlying)
    val trans = new HttpTransport(
      noop,
      new Http1ConnectionManager {

        override def observeMessage(message: Message, onFinish: Future[Unit]): Unit =
          message match {
            case _: Request => throw exc
            case _ => super.observeMessage(message, onFinish)
          }
      }
    )
    val f = trans.write(Request("google.com"))
    assert(f.isDefined)
    assert(f.poll == Some(Throw(exc)))
  }

  test("server closes after stream") {
    val reqq = new AsyncQueue[Request]
    val repq = new AsyncQueue[Response]
    @volatile var closed = false
    val repDone = Promise[Unit]

    val manager = new Http1ConnectionManager
    val underlying = {
      val qTrans = new QueueTransport[Response, Request](repq, reqq)
      new StreamTransportProxy[Response, Request](qTrans) {

        def write(rep: Response): Future[Unit] =
          qTrans.write(rep).before(repDone)

        def read(): Future[Multi[Request]] =
          qTrans.read().map(Multi(_, Future.Unit))

        override def close(d: Time) = {
          closed = true
          Future.Unit
        }
      }
    }
    val trans = new HttpTransport[Response, Request](underlying, manager)

    val Multi(req, _) = {
      val req = Request()
      req.headerMap.set("Connection", "close")
      reqq.offer(req)
      Await.result(trans.read(), 10.seconds)
    }
    assert(!req.isChunked)
    assert(!manager.shouldClose)
    assert(!closed)

    val rw = new Pipe[Buf]()
    val writef = {
      val rep = Response(Version.Http10, Status.Ok, rw)
      rep.setChunked(true)
      trans.write(rep)
    }
    assert(repq.size == 1)
    val rep = Await.result(repq.poll(), 10.seconds)
    assert(rep.isChunked)
    assert(!manager.shouldClose)
    assert(!closed)

    val readf = rep.reader.read()
    assert(readf.poll == None)

    // The request was not a keep alive request.
    // But the connection manager is not idle yet, because the response isn't done.
    // So it shouldn't close.
    assert(!manager.shouldClose)
    assert(trans.status != CoreStatus.Closed)

    repDone.setDone()

    // The request was not a keep alive request.
    // And now the connection manager is idle, because the response is done.
    // So it should close, and the Transport.status should reflect that.
    assert(manager.shouldClose)
    assert(trans.status == CoreStatus.Closed)

    assert(Await.result(rw.close().before(readf), 10.seconds) == None)
    assert(manager.shouldClose)
    assert(closed)
  }
}
