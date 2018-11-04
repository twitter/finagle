package com.twitter.finagle.redis.commands.streams

import com.twitter.concurrent.ThreadPoolScheduler
import com.twitter.conversions.time._
import com.twitter.finagle.redis.{RedisClientTest, XPendingAllReply, XPendingRangeReply}
import com.twitter.finagle.redis.tags.{ClientTest, RedisTest}
import com.twitter.finagle.redis.util.BufToString
import com.twitter.io.Buf
import com.twitter.util.{Await, CountDownLatch, Promise}
import org.scalatest.Inside

final class StreamsClientIntegrationSuite extends RedisClientTest with Inside {
  val TIMEOUT = 5.seconds

  test("Correctly perform stream create/add and read commands", RedisTest, ClientTest) {
    withRedisClient { client =>
      val messageId = Await.result(client.xAdd(bufFoo, None, Map(bufBoo -> bufMoo)), TIMEOUT)

      val expected = Seq((messageId, Seq((bufBoo, bufMoo))))

      Await.result(client.xRange(bufFoo, Buf.Utf8("-"), Buf.Utf8("+"), None), TIMEOUT) match {
        case `expected` =>
        case x => fail(s"Expected $expected, but got $x")
      }

      val messageId2 = Await.result(client.xAdd(bufFoo, None, Map(bufBoo -> bufBaz, bufBar -> bufBaz)), TIMEOUT)

      val expected2 = expected ++ Seq((messageId2, Seq((bufBoo, bufBaz), (bufBar, bufBaz))))

      Await.result(client.xRange(bufFoo, Buf.Utf8("-"), Buf.Utf8("+"), None), TIMEOUT) match {
        case `expected2` =>
        case x => fail(s"Expected $expected, but got $x")
      }
    }
  }

  test("Correctly perform stream delete commands", RedisTest, ClientTest) {
    withRedisClient { client =>
      val stream = Buf.Utf8("a")

      val messageId = Await.result(client.xAdd(stream, None, Map(bufBoo -> bufMoo)), TIMEOUT)
      val messageId2 = Await.result(client.xAdd(stream, None, Map(bufBoo -> bufMoo)), TIMEOUT)

      val len1 = Await.result(client.xLen(stream), TIMEOUT)
      assert(len1 == 2)

      val numDeleted = Await.result(client.xDel(stream, Seq(messageId)), TIMEOUT)
      assert(numDeleted == 1)

      val len2 = Await.result(client.xLen(stream), TIMEOUT)
      assert(len2 == 1)

      val expected = Seq((messageId2, Seq((bufBoo, bufMoo))))
      Await.result(client.xRange(stream, Buf.Utf8("-"), Buf.Utf8("+"), None), TIMEOUT) match {
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

      val ids = alpha.map(a => Await.result(client.xAdd(stream, None, Map(key -> a)), TIMEOUT))

      val len = Await.result(client.xLen(stream), TIMEOUT)
      assert(len == 26)

      val expected = ids.zip(alpha).flatMap { case (id, a) => Seq(id -> Seq(key -> a)) }
      val forward = Await.result(client.xRange(stream, Buf.Utf8("-"), Buf.Utf8("+"), None), TIMEOUT)

      forward match {
        case `expected` =>
        case _ => fail()
      }

      val expectedRev = expected.reverse
      val reverse = Await.result(client.xRevRange(stream, Buf.Utf8("+"), Buf.Utf8("-"), None), TIMEOUT)
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

      val ids = alpha.map(a => Await.result(client.xAdd(stream, None, Map(key -> a)), TIMEOUT))

      val len = Await.result(client.xLen(stream), TIMEOUT)
      assert(len == 26)

      val trimmed = Await.result(client.xTrim(stream, 1, exact = true), TIMEOUT)
      assert(trimmed == 25)

      val len2 = Await.result(client.xLen(stream), TIMEOUT)
      assert(len2 == 1)

      val read = Await.result(client.xRange(stream, Buf.Utf8("-"), Buf.Utf8("+"), None), TIMEOUT)
      assert(read.size == 1)
      assert(read.head._1 == ids.last)
    }
  }

  test("Correctly perform stream read commands", RedisTest, ClientTest) {
    val scheduler = new ThreadPoolScheduler("redis_test_1")

    try {
      val stream = Buf.Utf8("c")
      val start = Buf.Utf8("$") // Only read items that were pushed to the stream after we start listening

      val countdownLatch = new CountDownLatch(1)
      val blockingRead = Promise[Seq[(Buf, Seq[(Buf, Seq[(Buf, Buf)])])]]

      withRedisClient { client =>
        // Start a listener on a separate thread with client 1
        val submit = () => scheduler.submit(new Runnable {
          override def run(): Unit = {
            // Block for max 10 seconds
            blockingRead.become(client.xRead(None, Some(5000), Seq(stream), Seq(start)))
            countdownLatch.countDown()
            Await.result(blockingRead, TIMEOUT)
          }
        })

        assert(!blockingRead.isDefined)

        // WIth client B, wait until client A is listening and then send 2 messages to the stream
        withRedisClient { client2 =>
          // Start blocking on client A
          submit()
          countdownLatch.await()

          // Send an item on the stream
          val id = Await.result(client2.xAdd(stream, None, Map(bufFoo -> bufBar)), TIMEOUT)

          val expected = Seq(
            stream -> Seq(id -> Seq(bufFoo -> bufBar))
          )

          // Client A should have the item we just pushed
          Await.result(blockingRead, TIMEOUT) match {
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

      // Must create stream before adding read group
      Await.result(client.xAdd(stream, None, Map(bufFoo -> bufBar)), TIMEOUT)

      Await.result(client.xGroupCreate(stream, group, Buf.Utf8("0")), TIMEOUT)

      val read = Await.result(client.xReadGroup(group, consumer, Some(1), None, Seq(stream), Seq(Buf.Utf8("0"))), TIMEOUT)

      read match {
        case Seq((`stream`, Seq((_, Seq((`bufFoo`, `bufBar`)))))) =>
        case _ => fail()
      }

      val pendingId = read.head._2.head._1

      val pending1 = Await.result(client.xPending(stream, group), TIMEOUT)

      val expected1 = XPendingAllReply(one, Some(pendingId), Some(pendingId), Seq(consumer -> Buf.Utf8("1")))

      pending1 match {
        case `expected1` =>
        case n => fail(s"Expected $expected1 but got $n")
      }

      val pending2 = Await.result(client.xPending(stream, group, Buf.Utf8("-"), Buf.Utf8("+"), 10, None), TIMEOUT)
      pending2 match {
        case Seq(XPendingRangeReply(`pendingId`, `consumer`, _, `one`)) =>
        case _ => fail()
      }

      assert(Await.result(client.xAck(stream, group, Seq(pendingId))) == 1, TIMEOUT)

      Await.result(client.xPending(stream, group), TIMEOUT) match {
        case XPendingAllReply(`zero`, _, _, Seq()) =>
        case n => fail(s"Unexpected state $n")
      }

      Await.result(client.xPending(stream, group, Buf.Utf8("-"), Buf.Utf8("+"), 10, None), TIMEOUT) match {
        case Seq() =>
        case n => fail(s"Unexpected state $n")
      }
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
      val id = Await.result(client.xAdd(stream, None, Map(bufFoo -> bufBar)), TIMEOUT)

      Await.result(client.xGroupCreate(stream, group, Buf.Utf8("0")), TIMEOUT)

      val read = Await.result(client.xReadGroup(group, consumer, Some(1), None, Seq(stream), Seq(Buf.Utf8("0"))), TIMEOUT)

      read match {
        case Seq((`stream`, Seq((_, Seq((`bufFoo`, `bufBar`)))))) =>
        case _ => fail()
      }

      val pendingId = read.head._2.head._1

      val pending1 = Await.result(client.xPending(stream, group), TIMEOUT)

      val expected1 = XPendingAllReply(one, Some(pendingId), Some(pendingId), Seq(consumer -> Buf.Utf8("1")))

      pending1 match {
        case `expected1` =>
        case n => fail(s"Expected $expected1 but got $n")
      }

      val claimed = Await.result(client.xClaim(stream, group, consumer2, 0, Seq(id), None, None, false, false), TIMEOUT)


      claimed match {
        case Seq((`id`, Seq((`bufFoo`, `bufBar`)))) =>
        case _ => fail()
      }
    }
  }
}
