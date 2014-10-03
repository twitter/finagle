package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.naggati.RedisClientTest
import com.twitter.finagle.redis.tags.{RedisTest, ClientTest}
import com.twitter.util.Await
import com.twitter.finagle.redis.util.{CBToString, StringToChannelBuffer}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class KeyClientIntegrationSuite extends RedisClientTest {

  test("Correctly perform the DEL command", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.set(foo, bar))
      Await.result(client.del(Seq(foo)))
      val actualResult = Await.result(client.get(foo))
      val expectedResult = None
      assert(actualResult === expectedResult)
    }
  }

  test("Correctly perform the EXISTS command", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.set(foo, bar))
      val actualResult = Await.result(client.exists(foo))
      val expectedResult = true
      assert(actualResult === expectedResult)
    }
  }

  test("Correctly perform the TTL command", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.set(foo, bar))
      val time = 20L

      val actualExpireEvent = Await.result(client.expire(foo, time))
      val expectedExpireEvent = true
      assert(actualExpireEvent === expectedExpireEvent)

      val result = Await.result(client.ttl(foo)) match {
        case Some(num) => num
        case None      => fail("Could not retrieve key for TTL test")
      }
      val actualResult = if(result <= time) true else false
      val expectedResult = true
      assert(actualResult === expectedResult)
    }
  }

  test("Correctly perform the EXPIREAT command", RedisTest, ClientTest) {
    withRedisClient { client =>
      val ttl = System.currentTimeMillis() + 20000L
      Await.result(client.set(foo, bar))
      val actualExpireAtEvent = Await.result(client.expireAt(foo, ttl))
      val expectedExpireAtEvent = true
      assert(actualExpireAtEvent === expectedExpireAtEvent)

      val result = Await.result(client.ttl(foo)) match {
        case Some(num) => num
        case None      => fail("Could not retrieve key for TTL")
      }
      val actualResult = if(result <= ttl) true else false
      val expectedResult = true
      assert(actualResult === expectedResult)
    }
  }

  test("Correctly perform the MOVE command", RedisTest, ClientTest) {
    withRedisClient { client =>
      val fromDb = 14
      val toDb   = 15
      Await.result(client.select(toDb))
      Await.result(client.del(Seq(foo)))
      Await.result(client.select(fromDb))

      val actualEmptyKeyResult = Await.result(client.move(foo, bar))
      val expectedEmptyKeyResult = false
      assert(actualEmptyKeyResult === expectedEmptyKeyResult)

      Await.result(client.set(foo, bar))
      val actualResult = Await.result(client.move(foo, StringToChannelBuffer(toDb.toString)))
      val expectedResult = true
      assert(actualResult === expectedResult)

      Await.result(client.del(Seq(foo))) // clean up
    }
  }

  test("Correctly perform the PEXPIRE & PTL commands", RedisTest, ClientTest) {
    withRedisClient { client =>
      val ttl = 100000L
      Await.result(client.set(foo, bar))
      val didSetPEXPIRE = Await.result(client.pExpire(foo, ttl))
      val expectedPEXPIREResult = true
      assert(didSetPEXPIRE === expectedPEXPIREResult)

      val result = Await.result(client.pTtl(foo)) match {
        case Some(num) => num
        case None      => fail("Could not retrieve pTtl for key")
      }
      val actualResult = if(result <= ttl) true else false
      val expectedResult = true
      assert(actualResult === expectedResult)
    }
  }

  test("Correctly perform the PEXPIREAT & PTL commands", RedisTest, ClientTest) {
    withRedisClient { client =>
      val horizon = 20000L
      val ttl = System.currentTimeMillis() + horizon
      Await.result(client.set(foo, bar))
      val didSetPEXPIREAT = Await.result(client.pExpireAt(foo, ttl))
      val expectedPEXPIREATResult = true
      assert(didSetPEXPIREAT === expectedPEXPIREATResult)

      val result = Await.result(client.pTtl(foo)) match {
        case Some(num) => num
        case None      => fail("Could not retrieve pTtl for key")
      }
      val actualResult = if(result <= horizon) true else false
      val expectedResult = true
      assert(actualResult === expectedResult)
    }
  }
}
