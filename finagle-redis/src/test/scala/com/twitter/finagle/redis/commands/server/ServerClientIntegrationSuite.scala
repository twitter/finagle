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
final class ServerClientIntegrationSuite extends RedisClientTest {

  test("Correctly perform the FLUSHALL command", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.select(15))
      Await.result(client.set(foo, bar))
      Await.result(client.select(1))
      Await.result(client.flushAll())
      Await.result(client.select(15))

      assert(Await.result(client.get(foo)) == None)
    }
  }

  test("Correctly perform the FLUSHDB command", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.set(foo, bar))
      Await.result(client.flushDB())
      assert(Await.result(client.get(foo)) == None)
    }
  }

  test("Correctly perform the INFO command", RedisTest, ClientTest) {
    withRedisClient { client =>
      val info = new String(Await.result(client.info()).get.array, "UTF8")
      assert(info.contains("# Server") == true)
      assert(info.contains("redis_version:") == true)
      assert(info.contains("# Clients") == true)

      val cpuCB = StringToChannelBuffer("cpu")
      val cpu = new String(Await.result(client.info(cpuCB)).get.array, "UTF8")
      assert(cpu.contains("# CPU") == true)
      assert(cpu.contains("used_cpu_sys:") == true)
      assert(cpu.contains("redis_version:") == false)
    }
  }
}
