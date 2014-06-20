package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.naggati.FinagleRedisClientSuite
import com.twitter.finagle.redis.tags.{RedisTest, SlowTest}
import com.twitter.util.Await
import com.twitter.finagle.redis.util.{CBToString, StringToChannelBuffer}

final class ServerClientIntegrationSuite extends FinagleRedisClientSuite {

  test("Correctly perform the FLUSHDB command", RedisTest, SlowTest) {
    withRedisClient { client =>
      Await.result(client.set(foo, bar))
      Await.result(client.flushDB())
      val actualResult = Await.result(client.get(foo))
      val expectedResult = None
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
}
