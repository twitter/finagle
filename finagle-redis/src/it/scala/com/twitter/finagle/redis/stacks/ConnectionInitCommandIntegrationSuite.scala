package com.twitter.finagle.redis.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.redis.tags.{ClientTest, RedisTest}
import com.twitter.finagle.redis.util.RedisCluster
import com.twitter.finagle.redis.{RedisClientTest, ServerError}
import com.twitter.finagle.{Redis, redis}
import com.twitter.util.{Await, Future}

final class ConnectionInitCommandIntegrationSuite extends RedisClientTest {

  def await[A](a: Future[A]): A = Await.result(a, 5.seconds)

  test("Correctly perform using database", RedisTest, ClientTest) {
    val client1 = redis.Client(
      Redis.client
        .withDatabase(1)
        .newClient(RedisCluster.hostAddresses())
    )
    val client2 = redis.Client(
      Redis.client
        .withDatabase(2)
        .newClient(RedisCluster.hostAddresses())
    )
    await(client1.flushAll())
    await(client2.flushAll())
    try {
      val sameKey = bufFoo
      await(client1.set(sameKey, bufBar))
      await(client2.set(sameKey, bufBaz))
      assert(await(client1.get(sameKey)).get == bufBar)
      assert(await(client2.get(sameKey)).get == bufBaz)
    } finally {
      await(client1.close())
      await(client2.close())
    }
  }

  test("Correctly perform auth command", RedisTest, ClientTest) {
    val client = redis.Client(
      Redis.client
        .withPassword(bufFoo)
        .newClient(RedisCluster.hostAddresses())
    )
    try {
      val th = intercept[ServerError] {
        await(client.get(bufFoo))
      }
      assert(th.message.contains("ERR Client sent AUTH, but no password is set"))
    } finally {
      await(client.close())
    }
  }
}
