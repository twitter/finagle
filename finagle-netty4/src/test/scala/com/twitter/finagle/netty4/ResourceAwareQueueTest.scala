package com.twitter.finagle.netty4

import com.twitter.conversions.time._
import com.twitter.util.{Await, Awaitable}
import org.scalatest.FunSuite

class ResourceAwareQueueTest extends FunSuite {

  test("releaseFn sees and transforms pending messages on session close") {
    val seen = collection.mutable.ListBuffer.empty[String]
    val q = new ResourceAwareQueue[String](Int.MaxValue, {
      case m: String => seen.append(m); m.reverse
      case other => fail(s"Unexpected message: $other")
    })
    q.offer("one")
    q.offer("two")
    q.offer("three")
    q.fail(new Exception, discard = false)
    assert(seen.toList == List("one", "two", "three"))

    assert(Await.result(q.poll(), 1.second) == "one".reverse)
    assert(Await.result(q.poll(), 1.second) == "two".reverse)
    assert(Await.result(q.poll(), 1.second) == "three".reverse)
  }

  test("releaseFn sees failed offers") {
    var seen: String = ""

    val q = new ResourceAwareQueue[String](maxPendingOffers = 1, {
      case s: String =>
        seen = s
        ""
      case x =>
        fail(s"Unexpected type $x")
        ""
    })

    assert(q.offer("full")) // backing async queue is now full
    assert(!q.offer("doomed"))
    assert(seen == "doomed")
  }

  test("resilient to synchronous exceptions in release fn") {
    var msgsSeen = 0
    val q = new ResourceAwareQueue[Int](Int.MaxValue, {
      case i if i % 2 == 0 =>
        msgsSeen += 1
        throw new Exception("boom")
      case i =>
        msgsSeen += 1
        i * 10
    })
    (1 to 4).foreach { q.offer(_) }
    q.fail(new Exception, discard = false)

    // because of the release exception we flushed
    // buffered offers
    assert(q.size == 0)
    assert(msgsSeen == 4)
  }

  def await[T](a: Awaitable[T]): T = Await.result(a, 5.second)
}
