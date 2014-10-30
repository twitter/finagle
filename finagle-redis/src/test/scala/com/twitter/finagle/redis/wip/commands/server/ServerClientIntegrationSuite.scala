package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.naggati.RedisClientTest
import com.twitter.finagle.redis.tags.{RedisTest, ClientTest}
import com.twitter.util.Await
import com.twitter.finagle.redis.util.{CBToString, StringToChannelBuffer}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class ServerClientIntegrationSuite extends RedisClientTest {

  test("Correctly perform the FLUSHALL command", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.select(15))
      Await.result(client.set(foo, bar))
      Await.result(client.select(1))
      Await.result(client.flushAll())
      Await.result(client.select(15))

      val actualResult = Await.result(client.get(foo))
      val expectedResult = None
      assert(actualResult === expectedResult)
    }
  }

  test("Correctly perform the FLUSHDB command", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.set(foo, bar))
      Await.result(client.flushDB())
      val actualResult = Await.result(client.get(foo))
      val expectedResult = None
      assert(actualResult === expectedResult)
    }
  }

  test("Correctly perform the INFO command", RedisTest, ClientTest) {
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
}
