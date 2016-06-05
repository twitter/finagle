package com.twitter.finagle.http.exp

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.time._
import com.twitter.finagle.transport.QueueTransport
import com.twitter.util.{Await, Future}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class IdentityStreamTransportTest extends FunSuite {
  class Ctx {
    val in = new AsyncQueue[Int]
    val out = new AsyncQueue[Int]
    val qTransport = new QueueTransport(in, out)
    val identityTransport = new IdentityStreamTransport(qTransport)
  }

  test("marks the stream as finished reading as soon as we get back a read handle") {
    val ctx = new Ctx
    import ctx._

    out.offer(0)
    assert(Await.result(identityTransport.read(), 5.seconds) == Multi(0, Future.Done))
  }

  test("reads one object at a time from the underlying transport") {
    val ctx = new Ctx
    import ctx._

    out.offer(0)
    out.offer(1)
    assert(Await.result(identityTransport.read(), 5.seconds) == Multi(0, Future.Done))
    assert(Await.result(identityTransport.read(), 5.seconds) == Multi(1, Future.Done))
  }

  test("writes one object at a time to the underlying transport") {
    val ctx = new Ctx
    import ctx._

    val f1 = in.poll()
    assert(!f1.isDefined)
    val f2 = in.poll()
    assert(!f2.isDefined)
    Await.result(identityTransport.write(0), 5.seconds)
    assert(Await.result(f1, 5.seconds) == 0)
    Await.result(identityTransport.write(1), 5.seconds)
    assert(Await.result(f2, 5.seconds) == 1)
  }
}
