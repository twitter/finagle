package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.naggati.FinagleRedisClientSuite
import com.twitter.finagle.redis.tags.{RedisTest, SlowTest}
import com.twitter.util.Await
import com.twitter.finagle.redis.util.{CBToString, StringToChannelBuffer}

final class SimpleClientSuite extends FinagleRedisClientSuite {

  test("Correctly perform the APPEND command", RedisTest, SlowTest) {
    withRedisClient { client =>
      Await.result(client.set(foo, bar))
      val actualResult = Await.result(client.append(foo, baz))
      val expectedResult = 6
      assert(actualResult === expectedResult)
    }
  }

  test("Correctly perform the DECRBY command", RedisTest, SlowTest) {
    withRedisClient { client =>
      Await.result(client.set(foo, StringToChannelBuffer("21")))
      val actualResult = Await.result(client.decrBy(foo, 2))
      val expectedResult = 19
      assert(actualResult === expectedResult)
    }
  }

  test("Correctly perform the DEL command", RedisTest, SlowTest) {
    withRedisClient { client =>
      Await.result(client.set(foo, bar))
      Await.result(client.del(Seq(foo)))
      val actualResult = Await.result(client.get(foo))
      val expectedResult = None
      assert(actualResult === expectedResult)
    }
  }

  test("Correctly perform the EXISTS command", RedisTest, SlowTest) {
    withRedisClient { client =>
      Await.result(client.set(foo, bar))
      val actualResult = Await.result(client.exists(foo))
      val expectedResult = true
      assert(actualResult === expectedResult)
    }
  }

  test("Correctly perform the GETRANGE command", RedisTest, SlowTest) {
    withRedisClient { client =>
      Await.result(client.set(foo, StringToChannelBuffer("boing")))
      val actualResult = CBToString(Await.result(client.getRange(foo, 0, 2)).get)
      val expectedResult = "boi"
      assert(actualResult === expectedResult)
    }
  }

  test("Correctly perform the GET & SET commands", RedisTest, SlowTest) {
    withRedisClient { client =>
      val actualEmptyGetResult = Await.result(client.get(foo))
      val expectedEmptyGetResult = None
      assert(actualEmptyGetResult === expectedEmptyGetResult)

      Await.result(client.set(foo, bar))
      val actualGetResult = CBToString(Await.result(client.get(foo)).get)
      val expectedGetResult = "bar"
      assert(actualGetResult === expectedGetResult)
    }
  }

  test("Correctly perform the FLUSH command", RedisTest, SlowTest) {
    withRedisClient { client =>
      Await.result(client.set(foo, bar))
      Await.result(client.flushDB())
      val actualResult = Await.result(client.get(foo))
      val expectedResult = None
      assert(actualResult === expectedResult)
    }
  }

  test("Correctly perform the SELECT command", RedisTest, SlowTest) {
    withRedisClient { client =>
      val actualResult =Await.result(client.select(1))
      val expectedResult = ()
      assert(actualResult === expectedResult)
    }
  }

  test("Correctly perform the INFO command", RedisTest, SlowTest) {
    withRedisClient { client =>
      val info = new String(Await.result(client.info()).get.array, "UTF8")
      val hasServer = info.contains("# Server")
      assert(hasServer === true)
      val hasRedisVersion = info.contains("redis_version:")
      assert(hasRedisVersion === true)
      val hasClients = info.contains("# Clients")
      assert(hasClients === true)

      val cpu = new String(Await.result(client.info(StringToChannelBuffer("cpu"))).get.array, "UTF8")
      val hasCpu = cpu.contains("# CPU")
      assert(hasCpu === true)
      val hasUsedCpuSys = cpu.contains("used_cpu_sys:")
      assert(hasUsedCpuSys === true)
      val cpuHasRedisVersion = cpu.contains("redis_version:")
      assert(cpuHasRedisVersion === false)
    }
  }

  test("Correctly perform the QUIT command", RedisTest, SlowTest) {
    withRedisClient { client =>
      val actualResult = Await.result(client.quit())
      val expectedResult = ()
      assert(actualResult === expectedResult)
    }
  }

  test("Correctly perform the TTL command", RedisTest, SlowTest) {
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

  test("Correctly perform the EXPIREAT command", RedisTest, SlowTest) {
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

  test("Correctly perform the MOVE command", RedisTest, SlowTest) {
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
}
