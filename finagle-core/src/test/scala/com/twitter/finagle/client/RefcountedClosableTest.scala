package com.twitter.finagle.client

import com.twitter.conversions.time._
import com.twitter.util.{Await, Closable, Future, Time}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RefcountedClosableTest extends FunSuite {

  test("get") {
    val cls: Closable = Closable.nop
    val rc = new RefcountedClosable(cls)
    assert(cls == rc.get)
  }

  test("close is delayed until ref count reaches 0") {
    var closes = 0
    val cls: Closable = new Closable {
      def close(deadline: Time): Future[Unit] = {
        closes += 1
        Future.Done
      }
    }

    val rc = new RefcountedClosable(cls)
    assert(0 == closes)

    rc.open() // ref count = 1
    rc.open() // ref count = 2

    val c1 = rc.close() // ref count = 1
    assert(0 == closes)
    assert(!c1.isDefined)

    val c2 = rc.close() // ref count = 0
    assert(1 == closes)
    Await.ready(c1, 5.seconds)
    Await.ready(c2, 5.seconds)

    // ensure that extra closes don't budge the ref count
    assert(rc.close().isDefined)
    assert(1 == closes)
  }

}
