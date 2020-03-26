package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis._
import com.twitter.finagle.redis.tags.{RedisTest, ClientTest}
import com.twitter.util.{Await, Return}

final class ConnectionClientIntegrationSuite extends RedisClientTest {

  test("Correctly perform the SELECT command", RedisTest, ClientTest) {
    withRedisClient { client => assert(Await.result(client.select(1).liftToTry) == Return.Unit) }
  }

  test("Correctly perform the QUIT command", RedisTest, ClientTest) {
    withRedisClient { client => assert(Await.result(client.quit().liftToTry) == Return.Unit) }
  }

  test("Correctly perform the PING command without arguments", RedisTest, ClientTest) {
    withRedisClient { client => assert(Await.result(client.ping().liftToTry) == Return.Unit) }
  }
}
