package com.twitter.finagle.redis.protocol

import com.twitter.conversions.DurationOps._
import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.redis.RedisResponseTest
import com.twitter.finagle.transport.QueueTransport
import com.twitter.io.Buf
import com.twitter.util.Await
import org.scalacheck.Gen
import org.scalatest.OneInstancePerTest

class StageTransportTest extends RedisResponseTest with OneInstancePerTest {

  val in = new AsyncQueue[Buf]
  val out = new AsyncQueue[Buf]
  val t = new StageTransport(new QueueTransport(out, in))

  test("read") {
    forAll(Gen.nonEmptyListOf(genReply)) { replies =>
      val repliesAsBuf = replies.foldLeft(Buf.Empty)((acc, r) => acc.concat(encodeReply(r)))

      // Write all replies as a giant messy buffer.
      in.offer(repliesAsBuf)

      // Read all replies in order.
      val actual = Iterator.continually(Await.result(t.read(), 5.seconds)).take(replies.size).toList

      assert(replies == actual)
    }
  }
}
