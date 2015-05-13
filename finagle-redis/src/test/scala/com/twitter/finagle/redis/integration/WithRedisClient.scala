package com.twitter.finagle.redis.integration

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.redis.util.RedisCluster
import com.twitter.finagle.redis.{Client, Redis}
import com.twitter.util.Await

trait WithRedisClient {
  protected def startRedisCluster = {
    RedisCluster.start()
  }
  protected def stopRedisCluster = {
    RedisCluster.stop()
  }

  protected def redisHostAddresses() = {
    RedisCluster.hostAddresses()
  }

  protected def newRedisClient() = {
    val client = Client(
      ClientBuilder()
        .codec(new Redis())
        .hosts(redisHostAddresses())
        .hostConnectionLimit(1)
        .build()
    )
    Await.result(client.flushDB())
    client
  }

  protected def withRedisClient(testCode: Client => Any) = {
    val client = newRedisClient()
    try {
      testCode(client)
    }
    finally {
      client.release
    }
  }

}
