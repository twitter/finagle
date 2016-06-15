package com.twitter.finagle.redis.integration

import com.twitter.conversions.time._
import com.twitter.finagle.redis.naggati.RedisClientTest
import com.twitter.finagle.redis.tags.{ClientTest, RedisTest}
import com.twitter.util.Await
import com.twitter.finagle.redis.util.{CBToString, StringToChannelBuffer}

import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@Ignore
@RunWith(classOf[JUnitRunner])
final class HashClientIntegrationSuite extends RedisClientTest {

  val TIMEOUT = 5.seconds

  test("Correctly perform hash set and get commands", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hSet(foo, bar, baz), TIMEOUT)
      assert(CBToString(Await.result(client.hGet(foo, bar), TIMEOUT).get) == "baz")
      assert(Await.result(client.hGet(foo, boo), TIMEOUT) == None)
      assert(Await.result(client.hGet(bar, baz), TIMEOUT) == None)
    }
  }

  test("Correctly perform hash set and get an empty value", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hSet(foo, bar, StringToChannelBuffer("")), TIMEOUT)
      assert(CBToString(Await.result(client.hGet(foo, bar), TIMEOUT).get) == "")
    }
  }

  test("Correctly perform hash and field exists", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hSet(foo, bar, baz), TIMEOUT)
      assert(Await.result(client.hExists(foo, bar), TIMEOUT) == true)
      assert(Await.result(client.hExists(foo, baz), TIMEOUT) == false)
    }
  }

  test("Correctly delete a single field", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hSet(foo, bar, baz), TIMEOUT)
      assert(Await.result(client.hDel(foo, Seq(bar)), TIMEOUT) == 1)
      assert(Await.result(client.hDel(foo, Seq(baz)), TIMEOUT) == 0)
    }
  }

  test("Correctly delete multiple fields", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hSet(foo, bar, baz), TIMEOUT)
      Await.result(client.hSet(foo, boo, moo), TIMEOUT)
      assert(Await.result(client.hDel(foo, Seq(bar, boo)), TIMEOUT) == 2)
    }
  }

  test("Correctly get multiple values", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hSet(foo, bar, baz), TIMEOUT)
      Await.result(client.hSet(foo, boo, moo), TIMEOUT)
      assert(CBToString.fromList(
        Await.result(client.hMGet(foo, Seq(bar, boo)), TIMEOUT).toList) == Seq("baz", "moo"))
    }
  }

  test("Correctly set multiple values", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hMSet(foo, Map(baz -> bar, moo -> boo)), TIMEOUT)
      assert(CBToString.fromList(
        Await.result(client.hMGet(foo, Seq(baz, moo))).toList) == Seq("bar", "boo"), TIMEOUT)
    }
  }

  test(
    "Correctly set multiple values one of which is an empty string value",
    RedisTest,
    ClientTest
    ) {
    withRedisClient { client =>
      Await.result(client.hMSet(foo, Map(baz -> bar, moo -> StringToChannelBuffer(""))), TIMEOUT)
      assert(CBToString.fromList(
        Await.result(client.hMGet(foo, Seq(baz, moo))).toList) == Seq("bar", ""), TIMEOUT)
    }
  }

  test("Correctly get multiple values at once", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hSet(foo, bar, baz), TIMEOUT)
      Await.result(client.hSet(foo, boo, moo), TIMEOUT)
      assert(CBToString.fromTuples(
        Await.result(client.hGetAll(foo))) == Seq(("bar", "baz"), ("boo", "moo")), TIMEOUT)
    }
  }

  test("Correctly get multiple values including one empty string", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hSet(foo, bar, StringToChannelBuffer("")), TIMEOUT)
      Await.result(client.hSet(foo, boo, moo), TIMEOUT)
      assert(CBToString.fromTuples(
        Await.result(client.hGetAll(foo), TIMEOUT)) == Seq(("bar", ""), ("boo", "moo")))
    }
  }

  test("Correctly increment a value", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hIncrBy(foo, num, 4L), TIMEOUT)
      assert(Await.result(client.hGet(foo, num), TIMEOUT) == Some(StringToChannelBuffer(4L.toString)))
      Await.result(client.hIncrBy(foo, num, 4L), TIMEOUT)
      assert(Await.result(client.hGet(foo, num), TIMEOUT) == Some(StringToChannelBuffer(8L.toString)))
    }
  }

  test("Correctly do a setnx", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hDel(foo, Seq(bar)))
      assert(Await.result(client.hSetNx(foo,bar, baz), TIMEOUT) == 1)
      assert(Await.result(client.hSetNx(foo,bar, moo), TIMEOUT) == 0)
      assert(CBToString(Await.result(client.hGet(foo, bar), TIMEOUT).get) == "baz")
    }
  }

  test("Correctly get all the values", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.dels(Seq(bufFoo)), TIMEOUT)
      Await.result(client.hMSet(foo, Map(baz -> bar, moo -> boo)), TIMEOUT)
      assert(Await.result(client.hVals(foo), TIMEOUT).map(CBToString(_)) == Seq("bar", "boo"))
    }
  }

  test("Correctly count fields", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hMSet(foo, Map(baz -> bar, moo -> boo)), TIMEOUT)
      assert(Await.result(client.hLen(foo), TIMEOUT) == 2)
      Await.result(client.hDel(foo, Seq(baz)), TIMEOUT)
      assert(Await.result(client.hLen(foo), TIMEOUT) == 1)
      assert(Await.result(client.hLen(boo), TIMEOUT) == 0)
    }
  }

  ignore("Correctly perform an hscan operation", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hSet(foo, bar, baz))
      Await.result(client.hSet(foo, boo, moo))
      val res = Await.result(client.hScan(foo, 0, None, None), TIMEOUT)
      assert(CBToString(res(1)) == "bar")
      val withCount = Await.result(client.hScan(foo, 0, Some(2), None), TIMEOUT)
      assert(CBToString(withCount(0)) == "0")
      assert(CBToString(withCount(1)) == "bar")
      assert(CBToString(withCount(2)) == "boo")
      val pattern = StringToChannelBuffer("b*")
      val withPattern = Await.result(client.hScan(foo, 0, None, Some(pattern)), TIMEOUT)
      assert(CBToString(withCount(0)) == "0")
      assert(CBToString(withCount(1)) == "bar")
      assert(CBToString(withCount(2)) == "boo")
    }
  }
}
