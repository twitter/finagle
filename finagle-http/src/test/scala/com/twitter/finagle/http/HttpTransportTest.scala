package com.twitter.finagle.http

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.http.codec.ConnectionManager
import com.twitter.finagle.transport.QueueTransport
import com.twitter.util.Throw
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class HttpTransportTest extends FunSuite {
  test("exceptions in connection manager stay within Future context") {
    val exc = new IllegalArgumentException("boo")
    val noop = new QueueTransport(new AsyncQueue[Any], new AsyncQueue[Any])
    val trans = new HttpTransport(noop, new ConnectionManager {
      override def observeMessage(message: Any) = throw exc
    })

    val f = trans.write(Unit)
    assert(f.isDefined)
    assert(f.poll == Some(Throw(exc)))
  }
}
