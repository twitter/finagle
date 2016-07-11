package com.twitter.finagle.redis.integration

import com.twitter.finagle.Service
import com.twitter.finagle.redis.naggati.RedisClientServerIntegrationTest
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.tags.{ClientServerTest, RedisTest}
import com.twitter.io.Buf
import com.twitter.util.Await
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@Ignore
@RunWith(classOf[JUnitRunner])
final class SortedSetClientServerIntegrationSuite extends RedisClientServerIntegrationTest {
  val ZKEY = Buf.Utf8("zkey")
  val ZVAL = List(ZMember(1, Buf.Utf8("one")), ZMember(2, Buf.Utf8("two")), ZMember(3, Buf.Utf8("three")))
  val one = Buf.Utf8("one")
  val two = Buf.Utf8("two")
  val three = Buf.Utf8("three")
  val four = Buf.Utf8("four")

  private def zAdd(client: Service[Command, Reply], key: String, members: ZMember*): Unit = {
    members.foreach { member =>
      assert(Await.result(client(ZAdd(Buf.Utf8(key), List(member)))) == IntegerReply(1))
    }
  }

  private def initialize(client: Service[Command, Reply]): Unit = {
    ZVAL.foreach { zv =>
      assert(Await.result(client(ZAdd(ZKEY, List(zv)))) == IntegerReply(1))
    }
  }

  test("ZADD should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val key = Buf.Utf8("zadd1")
       assert(Await.result(client(ZAdd(key, List(ZMember(1, one))))) == IntegerReply(1))
      assert(Await.result(client(ZAdd(key, List(ZMember(2, two))))) == IntegerReply(1))
      assert(Await.result(client(ZAdd(key, List(ZMember(3, two))))) == IntegerReply(0))
      val expected = List("one", "1", "two", "3")
      assertMBulkReply(client(ZRange(key, 0, -1, WithScores)), expected)
      assertMBulkReply(client(ZRange(key, 0, -1)), List("one", "two"))
    }
  }

  test("ZCARD should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(Set(bufFoo, bufBar))) == StatusReply("OK"))
      initialize(client)
      assert(Await.result(client(ZCard(ZKEY))) == IntegerReply(3))
      assert(Await.result(client(ZCard(Buf.Utf8("nosuchkey")))) == IntegerReply(0))
      assert(Await.result(client(ZCard(bufFoo))).isInstanceOf[ErrorReply])
    }
  }

  test("ZCOUNT should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      initialize(client)
      assert(Await.result(client(ZCount(ZKEY, ZInterval.MIN, ZInterval.MAX))) == IntegerReply(3))
      assert(Await.result(client(ZCount(ZKEY, ZInterval.exclusive(1), ZInterval(3)))) ==
        IntegerReply(2))
    }
  }

  test("ZINCRBY should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      zAdd(client, "zincrby1", ZMember(1, Buf.Utf8("one")), ZMember(2, Buf.Utf8("two")))
      assertBulkReply(client(ZIncrBy(Buf.Utf8("zincrby1"), 2, Buf.Utf8("one"))), "3")
      assertMBulkReply(
        client(ZRange(Buf.Utf8("zincrby1"), 0, -1, WithScores)),
        List("two", "2", "one", "3"))
    }
  }

  test("ZINTERSTORE and ZUNIONSTORE should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val key ="zstore1"
      val key2 = "zstore2"
      val out = Buf.Utf8("out")
      zAdd(client, key, ZMember(1, one), ZMember(2, two))
      zAdd(client, key2, ZMember(1, one), ZMember(2, two), ZMember(3, three))

      assert(Await.result(client(ZInterStore("out", List(key, key2), Weights(2,3)))) ==
        IntegerReply(2))
      assertMBulkReply(
        client(ZRange(out, 0, -1, WithScores)),
        List("one", "5", "two", "10"))

      assert(Await.result(client(ZUnionStore("out", List(key, key2), Weights(2,3)))) ==
        IntegerReply(3))
      assertMBulkReply(
        client(ZRange(out, 0, -1, WithScores)),
        List("one", "5", "three", "9", "two", "10"))
    }
  }

  test("ZRANGE and ZREVRANGE should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      zAdd(client, "zrange1", ZMember(1, one), ZMember(2, two), ZMember(3, three))

      val key = Buf.Utf8("zrange1")
      assertMBulkReply(client(ZRange(key, 0, -1)), List("one", "two", "three"))
      assertMBulkReply(
        client(ZRange(key, 0, -1, WithScores)),
        List("one", "1", "two", "2", "three", "3"))
      assertMBulkReply(client(ZRange(key, 2, 3)), List("three"))
      assertMBulkReply(
        client(ZRange(key, 2, 3, WithScores)),
        List("three", "3"))
      assertMBulkReply(client(ZRange(key, -2, -1)), List("two", "three"))
      assertMBulkReply(
        client(ZRange(key, -2, -1, WithScores)),
        List("two", "2", "three", "3"))

      assertMBulkReply(
        client(ZRevRange(key, 0, -1)),
        List("three", "two", "one"))
      assertMBulkReply(
        client(ZRevRange(key, 2, 3)),
        List("one"))
      assertMBulkReply(
        client(ZRevRange(key, -2, -1)),
        List("two", "one"))
    }
  }

  test("ZRANGEBYSCORE and ZREVRANGEBYSCORE should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val key = Buf.Utf8("zrangebyscore1")
      zAdd(client, "zrangebyscore1", ZMember(1, one), ZMember(2, two), ZMember(3, three))
      assertMBulkReply(
        client(ZRangeByScore(key, ZInterval.MIN, ZInterval.MAX)),
        List("one", "two", "three"))
      assertMBulkReply(
        client(ZRangeByScore(key, ZInterval(1f), ZInterval(2f))),
        List("one", "two"))
      assertMBulkReply(
        client(ZRangeByScore(key, ZInterval.exclusive(1f), ZInterval(2f))),
        List("two"))
      assertMBulkReply(
        client(ZRangeByScore(key, ZInterval.exclusive(1f), ZInterval.exclusive(2f))),
        List())
      assertMBulkReply(
        client(ZRangeByScore(key, ZInterval.MIN, ZInterval.MAX, Limit(1,5))),
        List("two","three"))

      assertMBulkReply(
        client(ZRevRangeByScore(key, ZInterval.MAX, ZInterval.MIN)),
        List("three", "two", "one"))
      assertMBulkReply(
        client(ZRevRangeByScore(key, ZInterval(2f), ZInterval(1f))),
        List("two", "one"))
      assertMBulkReply(
        client(ZRevRangeByScore(key, ZInterval(2f), ZInterval.exclusive(1f))),
        List("two"))
      assertMBulkReply(
        client(ZRevRangeByScore(key, ZInterval.exclusive(2f), ZInterval.exclusive(1f))),
        List())
    }
  }

  test("ZRANK and ZREVRANK should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val key = Buf.Utf8("zrank1")
      zAdd(client, "zrank1", ZMember(1, one), ZMember(2, two), ZMember(3, three))
      assert(Await.result(client(ZRank(key, three))) == IntegerReply(2))
      assert(Await.result(client(ZRank(key, four))) == EmptyBulkReply())
      assert(Await.result(client(ZRevRank(key, one))) == IntegerReply(2))
      assert(Await.result(client(ZRevRank(key, four))) == EmptyBulkReply())
    }
  }

  test("ZREM should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val key = Buf.Utf8("zrem1")
      zAdd(client, "zrem1", ZMember(1, one), ZMember(2, two), ZMember(3, three))
      assert(Await.result(client(ZRem(key, List(two)))) == IntegerReply(1))
      assert(Await.result(client(ZRem(key, List(Buf.Utf8("nosuchmember"))))) == IntegerReply(0))
      assertMBulkReply(
        client(ZRange(key, 0, -1, WithScores)),
        List("one", "1", "three", "3"))
    }
  }

  test("ZREMRANGEBYRANK should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val key = Buf.Utf8("zremrangebyrank1")
      zAdd(client, "zremrangebyrank1", ZMember(1, one), ZMember(2, two), ZMember(3, three))
      assert(Await.result(client(ZRemRangeByRank(key, 0, 1))) == IntegerReply(2))
      assertMBulkReply(
        client(ZRange(key, 0, -1, WithScores)),
        List("three", "3"))
    }
  }

  test("ZREMRANGEBYSCORE should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val key = Buf.Utf8("zremrangebyscore1")
      zAdd(client, "zremrangebyscore1", ZMember(1, one), ZMember(2, two), ZMember(3, three))
      assert(Await.result(client(ZRemRangeByScore(key, ZInterval.MIN, ZInterval.exclusive(2)))) ==
        IntegerReply(1))
      assertMBulkReply(
        client(ZRange(key, 0, -1, WithScores)),
        List("two", "2", "three", "3"))
    }
  }

  test("ZSCORE should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      zAdd(client, "zscore1", ZMember(1, one))
      assertBulkReply(client(ZScore(Buf.Utf8("zscore1"), one)), Buf.Utf8("1"))
    }
  }
}
