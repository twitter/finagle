package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.naggati.RedisClientTest
import com.twitter.finagle.redis.tags.{RedisTest, ClientTest}
import com.twitter.util.Await
import com.twitter.finagle.redis.util.{CBToString, StringToChannelBuffer}
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@Ignore
@RunWith(classOf[JUnitRunner])
final class HashClientIntegrationSuite extends RedisClientTest {

  test("Correctly perform hash set and get commands", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hSet(foo, bar, baz))
      assert(CBToString(Await.result(client.hGet(foo, bar)).get) == "baz")
      assert(Await.result(client.hGet(foo, boo)) == None)
      assert(Await.result(client.hGet(bar, baz)) == None)
    }
  }

  test("Correctly perform hash set and get an empty value", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hSet(foo, bar, StringToChannelBuffer("")))
      assert(CBToString(Await.result(client.hGet(foo, bar)).get) == "")
    }
  }

  test("Correctly perform hash and field exists", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hSet(foo, bar, baz))
      assert(Await.result(client.hExists(foo, bar)) == true)
      assert(Await.result(client.hExists(foo, baz)) == false)
    }
  }

  test("Correctly delete a single field", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hSet(foo, bar, baz))
      assert(Await.result(client.hDel(foo, Seq(bar))) == 1)
      assert(Await.result(client.hDel(foo, Seq(baz))) == 0)
    }
  }

  test("Correctly delete multiple fields", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hSet(foo, bar, baz))
      Await.result(client.hSet(foo, boo, moo))
      assert(Await.result(client.hDel(foo, Seq(bar, boo))) == 2)
    }
  }

  test("Correctly get multiple values", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hSet(foo, bar, baz))
      Await.result(client.hSet(foo, boo, moo))
      assert(CBToString.fromList(
        Await.result(client.hMGet(foo, Seq(bar, boo))).toList) == Seq("baz", "moo"))
    }
  }

  test("Correctly set multiple values", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hMSet(foo, Map(baz -> bar, moo -> boo)))
      assert(CBToString.fromList(
        Await.result(client.hMGet(foo, Seq(baz, moo))).toList) == Seq("bar", "boo"))
    }
  }

  test(
    "Correctly set multiple values one of which is an empty string value",
    RedisTest,
    ClientTest
    ) {
    withRedisClient { client =>
      Await.result(client.hMSet(foo, Map(baz -> bar, moo -> StringToChannelBuffer(""))))
      assert(CBToString.fromList(
        Await.result(client.hMGet(foo, Seq(baz, moo))).toList) == Seq("bar", ""))
    }
  }

  test("Correctly get multiple values at once", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hSet(foo, bar, baz))
      Await.result(client.hSet(foo, boo, moo))
      assert(CBToString.fromTuples(
        Await.result(client.hGetAll(foo))) == Seq(("bar", "baz"), ("boo", "moo")))
    }
  }

  test("Correctly get multiple values including one empty string", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hSet(foo, bar, StringToChannelBuffer("")))
      Await.result(client.hSet(foo, boo, moo))
      assert(CBToString.fromTuples(
        Await.result(client.hGetAll(foo))) == Seq(("bar", ""), ("boo", "moo")))
    }
  }

  test("Correctly increment a value", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hIncrBy(foo, num, 4L))
      assert(Await.result(client.hGet(foo, num)) == Some(StringToChannelBuffer(4L.toString)))
      Await.result(client.hIncrBy(foo, num, 4L))
      assert(Await.result(client.hGet(foo, num)) == Some(StringToChannelBuffer(8L.toString)))
    }
  }

  test("Correctly do a setnx", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hDel(foo, Seq(bar)))
      assert(Await.result(client.hSetNx(foo,bar, baz)) == 1)
      assert(Await.result(client.hSetNx(foo,bar, moo)) == 0)
      assert(CBToString(Await.result(client.hGet(foo, bar)).get) == "baz")
    }
  }

  test("Correctly get all the values", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.del(Seq(foo)))
      Await.result(client.hMSet(foo, Map(baz -> bar, moo -> boo)))
      assert(Await.result(client.hVals(foo)).map(CBToString(_)) == Seq("bar", "boo"))
    }
  }

  ignore("Correctly perform an hscan operation", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hSet(foo, bar, baz))
      Await.result(client.hSet(foo, boo, moo))
      val res = Await.result(client.hScan(foo, 0, None, None))
      assert(CBToString(res(1)) == "bar")
      val withCount = Await.result(client.hScan(foo, 0, Some(2), None))
      assert(CBToString(withCount(0)) == "0")
      assert(CBToString(withCount(1)) == "bar")
      assert(CBToString(withCount(2)) == "boo")
      val pattern = StringToChannelBuffer("b*")
      val withPattern = Await.result(client.hScan(foo, 0, None, Some(pattern)))
      assert(CBToString(withCount(0)) == "0")
      assert(CBToString(withCount(1)) == "bar")
      assert(CBToString(withCount(2)) == "boo")
    }
  }
}
