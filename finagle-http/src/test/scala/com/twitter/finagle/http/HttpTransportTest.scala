package com.twitter.finagle.http

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.time._
import com.twitter.finagle.http.codec.ConnectionManager
import com.twitter.finagle.http.exp.{IdentityStreamTransport, Multi, StreamTransportProxy}
import com.twitter.finagle.transport.QueueTransport
import com.twitter.io.Reader
import com.twitter.util.{Await, Time, Throw, Future, Promise}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class HttpTransportTest extends FunSuite {

  test("exceptions in connection manager stay within Future context") {
    val exc = new IllegalArgumentException("boo")
    val underlying = new QueueTransport(new AsyncQueue[Request], new AsyncQueue[Response])
    val noop = new IdentityStreamTransport(underlying)
    val trans = new HttpTransport(noop, new ConnectionManager {
      override def observeRequest(message: Request, onFinish: Future[Unit]) = throw exc
    })
    val f = trans.write(Request("google.com"))
    assert(f.isDefined)
    assert(f.poll == Some(Throw(exc)))
  }

  test("server closes after stream") {
    val reqq = new AsyncQueue[Request]
    val repq = new AsyncQueue[Response]
    @volatile var closed = false
    val repDone = Promise[Unit]

    val manager = new ConnectionManager
    val underlying = {
      val self = new QueueTransport(repq, reqq)
      new StreamTransportProxy[Response, Request](self) {

        def write(rep: Response): Future[Unit] =
          self.write(rep).before(repDone)

        def read(): Future[Multi[Request]] =
          self.read().map(Multi(_, Future.Unit))

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

    val rw = Reader.writable()
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

    val readf = rep.reader.read(Int.MaxValue)
    assert(readf.poll == None)

    repDone.setDone()
    assert(Await.result(rw.close().before(readf), 10.seconds) == None)
    assert(manager.shouldClose)
    assert(closed)
  }
}
