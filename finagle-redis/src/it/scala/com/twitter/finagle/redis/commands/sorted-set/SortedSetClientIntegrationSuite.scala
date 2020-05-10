package com.twitter.finagle.redis.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.redis.protocol.{Limit, ZInterval}
import com.twitter.finagle.redis.RedisClientTest
import com.twitter.finagle.redis.tags.{ClientTest, RedisTest}
import com.twitter.finagle.redis.util.BufToString
import com.twitter.io.Buf
import com.twitter.util.Await

final class SortedSetClientIntegrationSuite extends RedisClientTest {

  val TIMEOUT = 5.seconds

  test("Correctly add members and get scores", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.zAdd(bufFoo, 10.5, bufBar)) == 1)
      assert(Await.result(client.zAdd(bufFoo, 20.1, bufBaz)) == 1)
      assert(Await.result(client.zScore(bufFoo, bufBar)).get == 10.5)
      assert(Await.result(client.zScore(bufFoo, bufBaz)).get == 20.1)
    }
  }

  test("Correctly add multiple members and get scores", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.zAddMulti(bufFoo, Seq((10.5, bufBar), (20.1, bufBaz)))) == 2)
      assert(Await.result(client.zScore(bufFoo, bufBar)).get == 10.5)
      assert(Await.result(client.zScore(bufFoo, bufBaz)).get == 20.1)
    }
  }

  test("Correctly add members and get the zcount", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.zAdd(bufFoo, 10L, bufBar)) == 1)
      assert(Await.result(client.zAdd(bufFoo, 20L, bufBaz)) == 1)
      assert(Await.result(client.zCount(bufFoo, ZInterval(0), ZInterval(30))) == 2)
      assert(Await.result(client.zCount(bufFoo, ZInterval(40), ZInterval(50))) == 0)
    }
  }

  test("Correctly get the zRangeByScore", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.zAdd(bufFoo, 10, bufBar)) == 1)
      assert(Await.result(client.zAdd(bufFoo, 20, bufBaz)) == 1)
      for (left <-
          Await
            .result(
              client.zRangeByScore(bufFoo, ZInterval(0), ZInterval(30), true, Some(Limit(0, 5)))
            )
            .left) {
        assert(left.asTuples == Seq((bufBar, 10), (bufBaz, 20)))
      }
      for (left <-
          Await
            .result(
              client.zRangeByScore(bufFoo, ZInterval(30), ZInterval(0), true, Some(Limit(0, 5)))
            )
            .left) {
        assert(left.asTuples == Seq())
      }
    }
  }

  test("Correctly get cardinality and remove members", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.zAdd(bufFoo, 10, bufBar)) == 1)
      assert(Await.result(client.zAdd(bufFoo, 20, bufBaz)) == 1)
      assert(Await.result(client.zCard(bufFoo)) == 2)
      assert(Await.result(client.zRem(bufFoo, Seq(bufBar, bufBaz))) == 2)
    }
  }

  test("Correctly get zRevRange", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.zAdd(bufFoo, 10, bufBar)) == 1)
      assert(Await.result(client.zAdd(bufFoo, 20, bufBaz)) == 1)
      for (right <- Await.result(client.zRevRange(bufFoo, 0, -1, false)).right)
        assert(right.toList == Seq(bufBaz, bufBar))
    }
  }

  test("Correctly get zRevRangeByScore", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.zAdd(bufFoo, 10, bufBar)) == 1)
      assert(Await.result(client.zAdd(bufFoo, 20, bufBaz)) == 1)
      for (left <-
          Await
            .result(
              client.zRevRangeByScore(bufFoo, ZInterval(10), ZInterval(0), true, Some(Limit(0, 1)))
            )
            .left) {
        assert(left.asTuples == Seq((bufBar, 10)))
      }
      for (left <-
          Await
            .result(
              client.zRevRangeByScore(bufFoo, ZInterval(0), ZInterval(10), true, Some(Limit(0, 1)))
            )
            .left) {
        assert(left.asTuples == Seq())
      }
      for (left <-
          Await
            .result(
              client.zRevRangeByScore(bufFoo, ZInterval(0), ZInterval(0), true, Some(Limit(0, 1)))
            )
            .left) {
        assert(left.asTuples == Seq())
      }
    }
  }

  test("Correctly add members and zIncr, then zIncr a nonmember", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.zAdd(bufFoo, 10, bufBar)) == 1)
      assert(Await.result(client.zIncrBy(bufFoo, 10, bufBar)) == Some(20))
      assert(Await.result(client.zIncrBy(bufFoo, 10, bufBaz)) == Some(10))
    }
  }

  test("Correctly get zRange", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.zAdd(bufFoo, 10, bufBar)) == 1)
      assert(Await.result(client.zAdd(bufFoo, 20, bufBaz)) == 1)
      assert(Await.result(client.zAdd(bufFoo, 30, bufBoo)) == 1)
      for (right <- Await.result(client.zRange(bufFoo, 0, -1, false)).right)
        assert(right.toList == List(bufBar, bufBaz, bufBoo))
      for (right <- Await.result(client.zRange(bufFoo, 2, 3, false)).right)
        assert(right.toList == List(bufBoo))
      for (right <- Await.result(client.zRange(bufFoo, -2, -1, false)).right)
        assert(right.toList == List(bufBaz, bufBoo))
    }
  }

  test("Correctly get zRank", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.zAdd(bufFoo, 10, bufBar)) == 1)
      assert(Await.result(client.zAdd(bufFoo, 20, bufBaz)) == 1)
      assert(Await.result(client.zAdd(bufFoo, 30, bufBoo)) == 1)
      assert(Await.result(client.zRank(bufFoo, bufBoo)) == Some(2))
      assert(Await.result(client.zRank(bufFoo, bufMoo)) == None)
    }
  }

  test("Correctly get zRemRangeByRank", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.zAdd(bufFoo, 10, bufBar)) == 1)
      assert(Await.result(client.zAdd(bufFoo, 20, bufBaz)) == 1)
      assert(Await.result(client.zAdd(bufFoo, 30, bufBoo)) == 1)
      assert(Await.result(client.zRemRangeByRank(bufFoo, 0, 1)) == 2)
      for (right <- Await.result(client.zRange(bufFoo, 0, -1, false)).right)
        assert(right.toList == List(bufBoo))
    }
  }

  test("Correctly get zRemRangeByScore", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.zAdd(bufFoo, 10, bufBar)) == 1)
      assert(Await.result(client.zAdd(bufFoo, 20, bufBaz)) == 1)
      assert(Await.result(client.zAdd(bufFoo, 30, bufBoo)) == 1)
      assert(Await.result(client.zRemRangeByScore(bufFoo, ZInterval(10), ZInterval(20))) == 2)
      for (right <- Await.result(client.zRange(bufFoo, 0, -1, false)).right)
        assert(right.toList == List(bufBoo))
    }
  }

  test("Correctly get zRevRank", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.zAdd(bufFoo, 10, bufBar)) == 1)
      assert(Await.result(client.zAdd(bufFoo, 20, bufBaz)) == 1)
      assert(Await.result(client.zAdd(bufFoo, 30, bufBoo)) == 1)
      assert(Await.result(client.zRevRank(bufFoo, bufBoo)) == Some(0))
      assert(Await.result(client.zRevRank(bufFoo, bufMoo)) == None)
    }
  }

  ignore("Correctly perform a zscan operation", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.zAdd(bufFoo, 10, bufBar))
      Await.result(client.zAdd(bufFoo, 20, bufBaz))
      val res = Await.result(client.zScan(bufFoo, 0L, None, None), TIMEOUT)
      assert(BufToString(res(1)) == "bar")
      val withCount = Await.result(client.zScan(bufFoo, 0L, Some(2L), None), TIMEOUT)
      assert(BufToString(withCount(0)) == "0")
      assert(BufToString(withCount(1)) == "bar")
      assert(BufToString(withCount(2)) == "boo")
      val pattern = Buf.Utf8("b*")
      val withPattern = Await.result(client.zScan(bufFoo, 0L, None, Some(pattern)), TIMEOUT)
      assert(BufToString(withCount(0)) == "0")
      assert(BufToString(withCount(1)) == "bar")
      assert(BufToString(withCount(2)) == "boo")
    }
  }
}
