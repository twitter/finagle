package com.twitter.finagle.redis.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.redis.RedisClientTest
import com.twitter.finagle.redis.tags.{ClientTest, RedisTest}
import com.twitter.finagle.redis.util.BufToString
import com.twitter.io.Buf
import com.twitter.util.Await

final class HashClientIntegrationSuite extends RedisClientTest {

  val TIMEOUT = 5.seconds

  test("Correctly perform hash set and get commands", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hSet(bufFoo, bufBar, bufBaz), TIMEOUT)

      assert(BufToString(Await.result(client.hGet(bufFoo, bufBar), TIMEOUT).get) == "baz")
      assert(Await.result(client.hGet(bufFoo, bufBoo), TIMEOUT) == None)
      assert(Await.result(client.hGet(bufBar, bufBaz), TIMEOUT) == None)
    }
  }

  test("Correctly perform hash set and get an empty value", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hSet(bufFoo, bufBar, Buf.Empty), TIMEOUT)
      assert(BufToString(Await.result(client.hGet(bufFoo, bufBar), TIMEOUT).get) == "")
    }
  }

  test("Correctly perform hash and field exists", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hSet(bufFoo, bufBar, bufBaz), TIMEOUT)
      assert(Await.result(client.hExists(bufFoo, bufBar), TIMEOUT) == true)
      assert(Await.result(client.hExists(bufFoo, bufBaz), TIMEOUT) == false)
    }
  }

  test("Correctly delete a single field", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hSet(bufFoo, bufBar, bufBaz), TIMEOUT)
      assert(Await.result(client.hDel(bufFoo, Seq(bufBar)), TIMEOUT) == 1)
      assert(Await.result(client.hDel(bufFoo, Seq(bufBaz)), TIMEOUT) == 0)
    }
  }

  test("Correctly delete multiple fields", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hSet(bufFoo, bufBar, bufBaz), TIMEOUT)
      Await.result(client.hSet(bufFoo, bufBoo, bufMoo), TIMEOUT)
      assert(Await.result(client.hDel(bufFoo, Seq(bufBar, bufBoo)), TIMEOUT) == 2)
    }
  }

  test("Correctly get multiple values", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hSet(bufFoo, bufBar, bufBaz), TIMEOUT)
      Await.result(client.hSet(bufFoo, bufBoo, bufMoo), TIMEOUT)
      val result = Await.result(client.hMGet(bufFoo, Seq(bufBar, bufBoo)), TIMEOUT).toList

      assert(result.map(Buf.Utf8.unapply).flatten == Seq("baz", "moo"))
    }
  }

  test("Correctly set multiple values", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hMSet(bufFoo, Map(bufBaz -> bufBar, bufMoo -> bufBoo)), TIMEOUT)
      val result = Await.result(client.hMGet(bufFoo, Seq(bufBaz, bufMoo)), TIMEOUT).toList

      assert(result.map(Buf.Utf8.unapply).flatten == Seq("bar", "boo"))
    }
  }

  ignore("Correctly merge lkeys with destination and set expiry", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hMSet(bufFoo, Map(bufBaz -> bufBar)), TIMEOUT)
      Await.result(client.pExpire(bufFoo, 10000), TIMEOUT)
      var result = Await.result(client.hMGet(bufFoo, Seq(bufBaz)), TIMEOUT).toList
      var ttl = Await.result(client.pTtl(bufFoo), TIMEOUT).get
      assert(result.map(Buf.Utf8.unapply).flatten == Seq("bar"))
      assert(ttl > 0)

      Await.result(client.hMergeEx(bufFoo, Map(bufBaz -> bufBoo, bufMoo -> bufBoo), 90000), TIMEOUT)
      result = Await.result(client.hMGet(bufFoo, Seq(bufBaz, bufMoo)), TIMEOUT).toList
      ttl = Await.result(client.pTtl(bufFoo), TIMEOUT).get
      assert(result.map(Buf.Utf8.unapply).flatten == Seq("bar", "boo")) //baz's value is unchanged
      assert(ttl > 10000 && ttl < 90000) // ttl is updated only if a field was added
    }
  }

  ignore("Correctly merge lkeys with destination without expiry", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hMSet(bufFoo, Map(bufBaz -> bufBar)), TIMEOUT)
      var result = Await.result(client.hMGet(bufFoo, Seq(bufBaz)), TIMEOUT).toList
      assert(result.map(Buf.Utf8.unapply).flatten == Seq("bar"))

      Await.result(client.hMergeEx(bufFoo, Map(bufBaz -> bufBoo, bufMoo -> bufBoo), -1), TIMEOUT)
      result = Await.result(client.hMGet(bufFoo, Seq(bufBaz, bufMoo)), TIMEOUT).toList
      var ttl = Await.result(client.pTtl(bufFoo), TIMEOUT).get
      assert(ttl == -1)
      assert(result.map(Buf.Utf8.unapply).flatten == Seq("bar", "boo")) //baz's value is unchanged
    }
  }

  ignore("TTL updated on merge only if a field was added.", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hMSet(bufFoo, Map(bufBaz -> bufBar)), TIMEOUT)
      Await.result(client.pExpire(bufFoo, 10000), TIMEOUT)
      var result = Await.result(client.hMGet(bufFoo, Seq(bufBaz)), TIMEOUT).toList
      var ttl = Await.result(client.pTtl(bufFoo), TIMEOUT).get

      assert(result.map(Buf.Utf8.unapply).flatten == Seq("bar"))
      assert(ttl > 0 && ttl < 10000)

      Await.result(client.hMergeEx(bufFoo, Map(bufBaz -> bufMoo, bufMoo -> bufBoo), 90000), TIMEOUT)
      result = Await.result(client.hMGet(bufFoo, Seq(bufBaz, bufMoo)), TIMEOUT).toList
      ttl = Await.result(client.pTtl(bufFoo), TIMEOUT).get
      assert(result.map(Buf.Utf8.unapply).flatten == Seq("bar", "boo")) // only boo is added
      assert(ttl > 10000 && ttl < 90000) // ttl updated.
    }
  }

  test(
    "Correctly set multiple values one of which is an empty string value",
    RedisTest,
    ClientTest
  ) {
    withRedisClient { client =>
      Await.result(client.hMSet(bufFoo, Map(bufBaz -> bufBar, bufMoo -> Buf.Empty)), TIMEOUT)
      val result = Await.result(client.hMGet(bufFoo, Seq(bufBaz, bufMoo)), TIMEOUT).toList

      assert(result.map(Buf.Utf8.unapply).flatten == Seq("bar", ""))
    }
  }

  test("Correctly get multiple values at once", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hSet(bufFoo, bufBar, bufBaz), TIMEOUT)
      Await.result(client.hSet(bufFoo, bufBoo, bufMoo), TIMEOUT)
      val result = Await.result(client.hGetAll(bufFoo), TIMEOUT)

      assert(
        result.map({ case (a, b) => Buf.Utf8.unapply(a).get -> Buf.Utf8.unapply(b).get }) ==
          Seq(("bar", "baz"), ("boo", "moo"))
      )
    }
  }

  test("Correctly get multiple values including one empty string", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hSet(bufFoo, bufBar, Buf.Empty), TIMEOUT)
      Await.result(client.hSet(bufFoo, bufBoo, bufMoo), TIMEOUT)
      val result = Await.result(client.hGetAll(bufFoo), TIMEOUT)

      assert(result == Seq(bufBar -> Buf.Utf8(""), bufBoo -> bufMoo))
    }
  }

  test("Correctly increment a value", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hIncrBy(bufFoo, bufNum, 4L), TIMEOUT)
      assert(Await.result(client.hGet(bufFoo, bufNum), TIMEOUT) == Some(Buf.Utf8(4L.toString)))
      Await.result(client.hIncrBy(bufFoo, bufNum, 4L), TIMEOUT)
      assert(Await.result(client.hGet(bufFoo, bufNum), TIMEOUT) == Some(Buf.Utf8(8L.toString)))
    }
  }

  test("Correctly do a setnx", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hDel(bufFoo, Seq(bufBar)))
      assert(Await.result(client.hSetNx(bufFoo, bufBar, bufBaz), TIMEOUT) == 1)
      assert(Await.result(client.hSetNx(bufFoo, bufBar, bufMoo), TIMEOUT) == 0)
      assert(BufToString(Await.result(client.hGet(bufFoo, bufBar), TIMEOUT).get) == "baz")
    }
  }

  test("Correctly get all the values", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.dels(Seq(bufFoo)), TIMEOUT)
      Await.result(client.hMSet(bufFoo, Map(bufBaz -> bufBar, bufMoo -> bufBoo)), TIMEOUT)
      assert(Await.result(client.hVals(bufFoo), TIMEOUT).map(BufToString(_)) == Seq("bar", "boo"))
    }
  }

  test("Correctly count fields", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hMSet(bufFoo, Map(bufBaz -> bufBar, bufMoo -> bufBoo)), TIMEOUT)
      assert(Await.result(client.hLen(bufFoo), TIMEOUT) == 2)
      Await.result(client.hDel(bufFoo, Seq(bufBaz)), TIMEOUT)
      assert(Await.result(client.hLen(bufFoo), TIMEOUT) == 1)
      assert(Await.result(client.hLen(bufBoo), TIMEOUT) == 0)
    }
  }

  test("Correctly perform an hscan operation", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hSet(bufFoo, bufBar, bufBaz), TIMEOUT)
      Await.result(client.hSet(bufFoo, bufBoo, bufMoo), TIMEOUT)

      val res = Await.result(client.hScan(bufFoo, 0L, None, None), TIMEOUT)
      val resList = res.flatMap(Buf.Utf8.unapply)
      assert(resList == Seq("0", "bar", "baz", "boo", "moo"))

      val withCount = Await.result(client.hScan(bufFoo, 0L, Some(2L), None), TIMEOUT)
      val withCountList = withCount.flatMap(Buf.Utf8.unapply)
      assert(withCountList == Seq("0", "bar", "baz", "boo", "moo"))

      val pattern = Buf.Utf8("bo*")
      val withPattern = Await.result(client.hScan(bufFoo, 0L, None, Some(pattern)), TIMEOUT)
      val withMatchList = withPattern.flatMap(Buf.Utf8.unapply)
      assert(withMatchList == Seq("0", "boo", "moo"))
    }
  }

  test("Correctly get the length of values of a field", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.hSet(bufFoo, bufBar, bufBaz), TIMEOUT)
      Await.result(client.hSet(bufFoo, bufBoo, Buf.U32LE(12)), TIMEOUT)
      Await.result(client.hSet(bufFoo, bufMoo, Buf.Empty), TIMEOUT)

      val res1 = Await.result(client.hStrlen(bufFoo, bufBar), TIMEOUT)
      assert(res1 == 3)

      val res2 = Await.result(client.hStrlen(bufFoo, bufBoo), TIMEOUT)
      assert(res2 == 4)

      val res3 = Await.result(client.hStrlen(bufFoo, bufMoo), TIMEOUT)
      assert(res3 == 0)
    }
  }
}
