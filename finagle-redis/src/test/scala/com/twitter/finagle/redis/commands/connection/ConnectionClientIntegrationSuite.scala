package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.naggati.RedisClientTest
import com.twitter.finagle.redis.tags.{RedisTest, ClientTest}
import com.twitter.util.Await
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@Ignore
@RunWith(classOf[JUnitRunner])
final class ConnectionClientIntegrationSuite extends RedisClientTest {

  test("Correctly perform the SELECT command", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.select(1)) == ((): Unit))
    }
  }

  test("Correctly perform the QUIT command", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.quit()) == ((): Unit))
    }
  }
}
