package com.twitter.finagle.http

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.http.codec.ConnectionManager
import com.twitter.finagle.http.exp.IdentityStreamTransport
import com.twitter.finagle.transport.QueueTransport
import com.twitter.util.{Throw, Future}
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
}
