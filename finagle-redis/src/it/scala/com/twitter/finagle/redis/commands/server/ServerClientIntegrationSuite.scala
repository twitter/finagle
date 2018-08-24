package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.RedisClientTest
import com.twitter.finagle.redis.tags.{ClientTest, RedisTest}
import com.twitter.util.Await
import com.twitter.finagle.redis.util.BufToString
import com.twitter.io.Buf

final class ServerClientIntegrationSuite extends RedisClientTest {

  test("Correctly perform the FLUSHALL command", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.select(15))
      Await.result(client.set(bufFoo, bufBar))
      Await.result(client.select(1))
      Await.result(client.flushAll())
      Await.result(client.select(15))

      assert(Await.result(client.get(bufFoo)) == None)
    }
  }

  test("Correctly perform the FLUSHDB command", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.set(bufFoo, bufBar))
      Await.result(client.flushDB())
      assert(Await.result(client.get(bufFoo)) == None)
    }
  }

  test("Correctly perform the DBSIZE command", RedisTest, ClientTest) {
    withRedisClient { client =>
      //add 1 key
      Await.result(client.set(bufFoo, bufBar))
      var size = Await.result(client.dbSize())
      assert(size == 1)
      //second key
      Await.result(client.set(bufBaz, bufBar))
      size = Await.result(client.dbSize())
      assert(size == 2)

      //clear all
      Await.result(client.flushDB())
      size = Await.result(client.dbSize())
      assert(size == 0)

    }
  }

  test("Correctly perform the INFO command", RedisTest, ClientTest) {
    withRedisClient { client =>
      val info = BufToString(Await.result(client.info()).get)
      assert(info.contains("# Server") == true)
      assert(info.contains("redis_version:") == true)
      assert(info.contains("# Clients") == true)

      val cpuCB = Buf.Utf8("cpu")
      val cpu = BufToString(Await.result(client.info(cpuCB)).get)
      assert(cpu.contains("# CPU") == true)
      assert(cpu.contains("used_cpu_sys:") == true)
      assert(cpu.contains("redis_version:") == false)
    }
  }
}
