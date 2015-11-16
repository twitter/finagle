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
final class KeyClientIntegrationSuite extends RedisClientTest {

  test("Correctly perform the DEL command", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.set(foo, bar))
      Await.result(client.del(Seq(foo)))
      assert(Await.result(client.get(foo)) == None)
    }
  }

  test("Correctly perform the DUMP command", RedisTest, ClientTest) {
    withRedisClient { client =>
      val k = StringToChannelBuffer("mykey")
      val v = StringToChannelBuffer("10")
      val expectedBytes: Array[Byte] = Array(0, -64, 10, 6, 0, -8, 114, 63, -59, -5, -5, 95, 40)

      Await.result(client.set(k, v))
      assert(Await.result(client.dump(k)).fold(fail("Expected result for DUMP"))(_.array) ==
        expectedBytes)
      Await.result(client.del(Seq(foo)))
      assert(Await.result(client.dump(foo)) == None)
    }
  }

  // Once the scan/hscan pull request gets merged into Redis master,
  // the tests can be uncommented.
  ignore("Correctly perform the SCAN command", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.set(foo, bar))
      Await.result(client.set(baz, boo))
      assert(CBToString(Await.result(client.scan(0, None, None)).apply(1)) == "baz")

      val withCount = Await.result(client.scan(0, Some(10), None))
      assert(CBToString(withCount(0)) == "0")
      assert(CBToString(withCount(1)) == "baz")
      assert(CBToString(withCount(2)) == "foo")

      val pattern = StringToChannelBuffer("b*")
      val withPattern = Await.result(client.scan(0, None, Some(pattern)))
      assert(CBToString(withPattern(0)) == "0")
      assert(CBToString(withPattern(1)) == "baz")
    }
  }

  test("Correctly perform the EXISTS command", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.set(foo, bar))
      assert(Await.result(client.exists(foo)) == true)
    }
  }

  test("Correctly perform the TTL command", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.set(foo, bar))
      val time = 20L

      assert(Await.result(client.expire(foo, time)) == true)

      val result = Await.result(client.ttl(foo)) match {
        case Some(num) => num
        case None      => fail("Could not retrieve key for TTL test")
      }
      assert(result <= time)
    }
  }

  test("Correctly perform the EXPIREAT command", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.set(foo, bar))

      // TODO: this isn't actually a TTL, which means that the second assertion
      // below is true for uninteresting reasons.
      val ttl = System.currentTimeMillis() + 20000L

      assert(Await.result(client.expireAt(foo, ttl)) == true)

      val result = Await.result(client.ttl(foo)) match {
        case Some(num) => num
        case None      => fail("Could not retrieve key for TTL")
      }
      assert(result <= ttl)
    }
  }

  test("Correctly perform the MOVE command", RedisTest, ClientTest) {
    withRedisClient { client =>
      val fromDb = 14
      val toDb   = 15
      Await.result(client.select(toDb))
      Await.result(client.del(Seq(foo)))
      Await.result(client.select(fromDb))

      // This following fails with an exceptions since bar is not a database.
      // assert(Await.result(client.move(foo, bar)) == false)

      Await.result(client.set(foo, bar))
      assert(Await.result(client.move(foo, StringToChannelBuffer(toDb.toString))) == true)

      Await.result(client.del(Seq(foo))) // clean up
    }
  }

  test("Correctly perform the PEXPIRE & PTL commands", RedisTest, ClientTest) {
    withRedisClient { client =>
      val ttl = 100000L
      Await.result(client.set(foo, bar))
      assert(Await.result(client.pExpire(foo, ttl)) == true)

      val result = Await.result(client.pTtl(foo)) match {
        case Some(num) => num
        case None      => fail("Could not retrieve pTtl for key")
      }
      assert(result <= ttl)
    }
  }

  test("Correctly perform the PEXPIREAT & PTL commands", RedisTest, ClientTest) {
    withRedisClient { client =>
      val horizon = 20000L
      val ttl = System.currentTimeMillis() + horizon
      Await.result(client.set(foo, bar))
      assert(Await.result(client.pExpireAt(foo, ttl)) == true)

      val result = Await.result(client.pTtl(foo)) match {
        case Some(num) => num
        case None      => fail("Could not retrieve pTtl for key")
      }
      assert(result <= horizon)
    }
  }
}
