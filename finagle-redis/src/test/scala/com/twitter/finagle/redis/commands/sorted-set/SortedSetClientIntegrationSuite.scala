package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.protocol.{Limit, ZInterval}
import com.twitter.finagle.redis.naggati.RedisClientTest
import com.twitter.finagle.redis.tags.{ClientTest, RedisTest}
import com.twitter.finagle.redis.util.{CBToString, StringToChannelBuffer}
import com.twitter.util.Await
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@Ignore
@RunWith(classOf[JUnitRunner])
final class SortedSetClientIntegrationSuite extends RedisClientTest {

  test("Correctly add members and get scores", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.zAdd(foo, 10.5, bar)) == 1)
      assert(Await.result(client.zAdd(foo, 20.1, baz)) == 1)
      assert(Await.result(client.zScore(foo, bar)).get == 10.5)
      assert(Await.result(client.zScore(foo, baz)).get == 20.1)
    }
  }

  test("Correctly add multiple members and get scores", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.zAddMulti(foo, Seq((10.5, bar), (20.1, baz)))) == 2)
      assert(Await.result(client.zScore(foo, bar)).get == 10.5)
      assert(Await.result(client.zScore(foo, baz)).get == 20.1)
    }
  }

  test("Correctly add members and get the zcount", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.zAdd(foo, 10, bar)) == 1)
      assert(Await.result(client.zAdd(foo, 20, baz)) == 1)
      assert(Await.result(client.zCount(foo, ZInterval(0), ZInterval(30))) == 2)
      assert(Await.result(client.zCount(foo, ZInterval(40), ZInterval(50))) == 0)
    }
  }

  test("Correctly get the zRangeByScore", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.zAdd(foo, 10, bar)) == 1)
      assert(Await.result(client.zAdd(foo, 20, baz)) == 1)
      for (left <- Await.result(client.zRangeByScore(foo, ZInterval(0), ZInterval(30), true,
        Some(Limit(0, 5)))).left) {
        assert(CBToString.fromTuplesWithDoubles(left.asTuples) == (Seq(("bar", 10), ("baz", 20))))
      }
      for (left <- Await.result(client.zRangeByScore(foo, ZInterval(30), ZInterval(0), true,
        Some(Limit(0, 5)))).left) {
        assert(left.asTuples == Seq())
      }
    }
  }

  test("Correctly get cardinality and remove members", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.zAdd(foo, 10, bar)) == 1)
      assert(Await.result(client.zAdd(foo, 20, baz)) == 1)
      assert(Await.result(client.zCard(foo)) == 2)
      assert(Await.result(client.zRem(foo, Seq(bar, baz))) == 2)
    }
  }

  test("Correctly get zRevRange", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.zAdd(foo, 10, bar)) == 1)
      assert(Await.result(client.zAdd(foo, 20, baz)) == 1)
      for (right <- Await.result(client.zRevRange(foo, 0, -1, false)).right)
        assert(CBToString.fromList(right.toList) == Seq("baz", "bar"))
    }
  }

  test("Correctly get zRevRangeByScore", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.zAdd(foo, 10, bar)) == 1)
      assert(Await.result(client.zAdd(foo, 20, baz)) == 1)
      for (left <- Await.result(client.zRevRangeByScore(foo, ZInterval(10), ZInterval(0), true,
        Some(Limit(0, 1)))).left) {
        assert(CBToString.fromTuplesWithDoubles(left.asTuples) == Seq(("bar", 10)))
      }
      for (left <- Await.result(client.zRevRangeByScore(foo, ZInterval(0), ZInterval(10), true,
        Some(Limit(0, 1)))).left) {
        assert(left.asTuples == Seq())
      }
      for (left <- Await.result(client.zRevRangeByScore(foo, ZInterval(0), ZInterval(0), true,
        Some(Limit(0, 1)))).left) {
        assert(left.asTuples == Seq())
      }
    }
  }

  test("Correctly add members and zIncr, then zIncr a nonmember", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.zAdd(foo, 10, bar)) == 1)
      assert(Await.result(client.zIncrBy(foo, 10, bar)) == Some(20))
      assert(Await.result(client.zIncrBy(foo, 10, baz)) == Some(10))
    }
  }

  test("Correctly get zRange", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.zAdd(foo, 10, bar)) == 1)
      assert(Await.result(client.zAdd(foo, 20, baz)) == 1)
      assert(Await.result(client.zAdd(foo, 30, boo)) == 1)
      for (right <- Await.result(client.zRange(foo, 0, -1, false)).right)
        assert(CBToString.fromList(right.toList) == List("bar", "baz", "boo"))
      for (right <- Await.result(client.zRange(foo, 2, 3, false)).right)
        assert(CBToString.fromList(right.toList) == List("boo"))
      for (right <- Await.result(client.zRange(foo, -2, -1, false)).right)
        assert(CBToString.fromList(right.toList) == List("baz", "boo"))
    }
  }

  test("Correctly get zRank", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.zAdd(foo, 10, bar)) == 1)
      assert(Await.result(client.zAdd(foo, 20, baz)) == 1)
      assert(Await.result(client.zAdd(foo, 30, boo)) == 1)
      assert(Await.result(client.zRank(foo, boo)) == Some(2))
      assert(Await.result(client.zRank(foo, moo)) == None)
    }
  }

  test("Correctly get zRemRangeByRank", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.zAdd(foo, 10, bar)) == 1)
      assert(Await.result(client.zAdd(foo, 20, baz)) == 1)
      assert(Await.result(client.zAdd(foo, 30, boo)) == 1)
      assert(Await.result(client.zRemRangeByRank(foo, 0, 1)) == 2)
      for (right <- Await.result(client.zRange(foo, 0, -1, false)).right)
        assert(CBToString.fromList(right.toList) == List("boo"))
    }
  }

  test("Correctly get zRemRangeByScore", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.zAdd(foo, 10, bar)) == 1)
      assert(Await.result(client.zAdd(foo, 20, baz)) == 1)
      assert(Await.result(client.zAdd(foo, 30, boo)) == 1)
      assert(Await.result(client.zRemRangeByScore(foo, ZInterval(10), ZInterval(20))) == 2)
      for (right <- Await.result(client.zRange(foo, 0, -1, false)).right)
        assert(CBToString.fromList(right.toList) == List("boo"))
    }
  }

  test("Correctly get zRevRank", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.zAdd(foo, 10, bar)) == 1)
      assert(Await.result(client.zAdd(foo, 20, baz)) == 1)
      assert(Await.result(client.zAdd(foo, 30, boo)) == 1)
      assert(Await.result(client.zRevRank(foo, boo)) == Some(0))
      assert(Await.result(client.zRevRank(foo, moo)) == None)
    }
  }
}
