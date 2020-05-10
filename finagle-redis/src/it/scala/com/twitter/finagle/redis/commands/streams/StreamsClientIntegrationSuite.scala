package com.twitter.finagle.redis.commands.streams

import com.twitter.concurrent.ThreadPoolScheduler
import com.twitter.finagle.redis.tags.{ClientTest, RedisTest}
import com.twitter.finagle.redis._
import com.twitter.finagle.redis.util.RedisTestHelper
import com.twitter.io.Buf
import com.twitter.util._
import java.util.concurrent.CountDownLatch
import org.scalactic.source.Position
import org.scalatest.{Inside, Tag}

final class StreamsClientIntegrationSuite extends RedisClientTest with Inside {

  override protected def test(
    testName: String,
    testTags: Tag*
  )(
    f: => Any
  )(
    implicit pos: Position
  ): Unit = {
    RedisTestHelper.redisServerVersion match {
      case Some((m, _, _)) if m >= 5 => super.test(testName, testTags: _*)(f)(pos)
      case _ => ignore(testName)(f)(pos)
    }
  }

  test("Correctly perform stream create/add and read commands", RedisTest, ClientTest) {
    withRedisClient { client =>
      val messageId = result(client.xAdd(bufFoo, None, Map(bufBoo -> bufMoo)))

      val expected = Seq(StreamEntryReply(messageId, Seq((bufBoo, bufMoo))))
      val rangeRes = result(client.xRange(bufFoo, Buf.Utf8("-"), Buf.Utf8("+"), None))

      assert(rangeRes == expected, s"Expected $expected, but got $rangeRes")

      val messageId2 = result(client.xAdd(bufFoo, None, Map(bufBoo -> bufBaz, bufBar -> bufBaz)))

      val expected2 = expected ++ Seq(
        StreamEntryReply(messageId2, Seq((bufBoo, bufBaz), (bufBar, bufBaz)))
      )
      val rangeRes2 = result(client.xRange(bufFoo, Buf.Utf8("-"), Buf.Utf8("+"), None))
      assert(rangeRes2 == expected2, s"Expected $expected2, but got $rangeRes2")
    }
  }

  test("Correctly perform stream delete commands", RedisTest, ClientTest) {
    withRedisClient { client =>
      val stream = Buf.Utf8("a")

      val messageId = result(client.xAdd(stream, None, Map(bufBoo -> bufMoo)))
      val messageId2 = result(client.xAdd(stream, None, Map(bufBoo -> bufMoo)))

      val len1 = result(client.xLen(stream))
      assert(len1 == 2)

      val numDeleted = result(client.xDel(stream, Seq(messageId)))
      assert(numDeleted == 1)

      val len2 = result(client.xLen(stream))
      assert(len2 == 1)

      val expected = Seq(StreamEntryReply(messageId2, Seq((bufBoo, bufMoo))))
      result(client.xRange(stream, Buf.Utf8("-"), Buf.Utf8("+"), None)) match {
        case `expected` =>
        case x => fail(s"Expected $expected, but got $x")
      }
    }
  }

  test("Correctly perform stream range commands", RedisTest, ClientTest) {
    withRedisClient { client =>
      val stream = Buf.Utf8("b")
      val key = Buf.Utf8("key")
      val alpha = ('a' to 'z').map(_.toString).map(Buf.Utf8.apply)

      val ids = alpha.map(a => result(client.xAdd(stream, None, Map(key -> a))))

      val len = result(client.xLen(stream))
      assert(len == 26)

      val expected =
        ids.zip(alpha).flatMap { case (id, a) => Seq(StreamEntryReply(id, Seq(key -> a))) }
      val forward = result(client.xRange(stream, Buf.Utf8("-"), Buf.Utf8("+"), None))

      forward match {
        case `expected` =>
        case _ => fail()
      }

      val expectedRev = expected.reverse
      val reverse = result(client.xRevRange(stream, Buf.Utf8("+"), Buf.Utf8("-"), None))
      reverse match {
        case `expectedRev` =>
        case _ => fail()
      }
    }
  }

  test("Correctly perform stream trim commands", RedisTest, ClientTest) {
    withRedisClient { client =>
      val stream = Buf.Utf8("trim")
      val key = Buf.Utf8("key")
      val alpha = ('a' to 'z').map(_.toString).map(Buf.Utf8.apply)

      val ids = alpha.map(a => result(client.xAdd(stream, None, Map(key -> a))))

      val len = result(client.xLen(stream))
      assert(len == 26)

      val trimmed = result(client.xTrim(stream, 1, exact = true))
      assert(trimmed == 25)

      val len2 = result(client.xLen(stream))
      assert(len2 == 1)

      val read = result(client.xRange(stream, Buf.Utf8("-"), Buf.Utf8("+"), None))
      assert(read.size == 1)
      assert(read.head.id == ids.last)
    }
  }

  test("Correctly perform stream read commands", RedisTest, ClientTest) {
    val scheduler = new ThreadPoolScheduler("redis_test_1")

    try {
      val stream = Buf.Utf8("c")
      val start =
        Buf.Utf8("$") // Only read items that were pushed to the stream after we start listening

      val countdownLatch = new CountDownLatch(1)
      val blockingRead = Promise[Seq[XReadStreamReply]]

      withRedisClient { client =>
        // Start a listener on a separate thread with client 1
        val submit = () =>
          scheduler.submit(new Runnable {
            override def run(): Unit = {
              // Block for max 10 seconds
              blockingRead.become(client.xRead(None, Some(5000), Seq(stream), Seq(start)))
              countdownLatch.countDown()
              result(blockingRead)
            }
          })

        assert(!blockingRead.isDefined)

        // WIth client B, wait until client A is listening and then send 2 messages to the stream
        withRedisClient { client2 =>
          // Start blocking on client A
          submit()
          countdownLatch.await()

          // Send an item on the stream
          val id = result(client2.xAdd(stream, None, Map(bufFoo -> bufBar)))

          val expected = Seq(
            XReadStreamReply(stream, Seq(StreamEntryReply(id, Seq(bufFoo -> bufBar))))
          )

          // Client A should have the item we just pushed
          result(blockingRead) match {
            case `expected` =>
            case x => fail(s"Expected $expected, but got $x")
          }
        }
      }
    } finally {
      scheduler.shutdown()
    }
  }

  test("Correctly send messages to read groups", RedisTest, ClientTest) {
    withRedisClient { client =>
      val stream = Buf.Utf8("d")
      val group = Buf.Utf8("group-1")
      val consumer = Buf.Utf8("consumer-1")
      val zero = new java.lang.Long(0)
      val one = new java.lang.Long(1)

      // Must create stream before adding to the group
      result(client.xAdd(stream, None, Map(bufFoo -> bufBar)))
      result(client.xGroupCreate(stream, group, Buf.Utf8("0")))

      // It ignores anything that existed before the group was created, so seed it now
      result(client.xAdd(stream, None, Map(bufFoo -> bufBar)))

      val read =
        result(client.xReadGroup(group, consumer, Some(1), None, Seq(stream), Seq(Buf.Utf8(">"))))

      read match {
        case Seq(XReadStreamReply(`stream`, Seq(StreamEntryReply(_, Seq((`bufFoo`, `bufBar`)))))) =>
        case other => fail(other.toString)
      }

      val pendingId = read.head.entries.head.id

      val pending1 = result(client.xPending(stream, group))
      val expected1 =
        XPendingAllReply(one, Some(pendingId), Some(pendingId), Seq(consumer -> Buf.Utf8("1")))

      assert(pending1 == expected1, s"Expected $expected1 but got $pending1")

      val pending2 = result(client.xPending(stream, group, Buf.Utf8("-"), Buf.Utf8("+"), 10, None))
      pending2 match {
        case Seq(XPendingRangeReply(`pendingId`, `consumer`, _, `one`)) =>
        case _ => fail()
      }

      assert(result(client.xAck(stream, group, Seq(pendingId))) == 1)

      result(client.xPending(stream, group)) match {
        case XPendingAllReply(`zero`, _, _, Seq()) =>
        case n => fail(s"Unexpected state $n")
      }

      assert(result(client.xPending(stream, group, Buf.Utf8("-"), Buf.Utf8("+"), 10, None)).isEmpty)
    }
  }

  test("Correctly claim pending messages from other consumers", RedisTest, ClientTest) {
    withRedisClient { client =>
      val stream = Buf.Utf8("e")
      val group = Buf.Utf8("group-1")
      val consumer = Buf.Utf8("consumer-1")
      val consumer2 = Buf.Utf8("consumer-2")
      val one = new java.lang.Long(1)

      // Must create stream before adding read group
      val id = result(client.xAdd(stream, None, Map(bufFoo -> bufBar)))

      result(client.xGroupCreate(stream, group, Buf.Utf8("0")))

      val read =
        result(client.xReadGroup(group, consumer, Some(1), None, Seq(stream), Seq(Buf.Utf8("0"))))

      read match {
        case Seq(XReadStreamReply(`stream`, Seq(StreamEntryReply(_, Seq((`bufFoo`, `bufBar`)))))) =>
        case other => fail(other.toString)
      }

      val pendingId = read.head.entries.head.id

      val pending1 = result(client.xPending(stream, group))
      val expected1 =
        XPendingAllReply(one, Some(pendingId), Some(pendingId), Seq(consumer -> Buf.Utf8("1")))

      assert(pending1 == expected1, s"Expected $expected1 but got $expected1")

      val claimed =
        result(client.xClaim(stream, group, consumer2, 0, Seq(id), None, None, false, false))

      assert(claimed == Seq(StreamEntryReply(`id`, Seq(`bufFoo` -> `bufBar`))))
    }
  }
}
