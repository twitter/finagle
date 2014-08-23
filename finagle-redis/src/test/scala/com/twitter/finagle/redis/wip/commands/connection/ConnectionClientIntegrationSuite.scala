package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.naggati.FinagleRedisClientTest
import com.twitter.finagle.redis.tags.{RedisTest, ClientTest}
import com.twitter.util.Await
import com.twitter.finagle.redis.util.{CBToString, StringToChannelBuffer}

final class ConnectionClientIntegrationSuite extends FinagleRedisClientTest {

  test("Correctly perform the SELECT command", RedisTest, ClientTest) {
    withRedisClient { client =>
      val actualResult =Await.result(client.select(1))
      val expectedResult = ()
      assert(actualResult === expectedResult)
    }
  }

  test("Correctly perform the QUIT command", RedisTest, ClientTest) {
    withRedisClient { client =>
      val actualResult = Await.result(client.quit())
      val expectedResult = ()
      assert(actualResult === expectedResult)
    }
  }
}
