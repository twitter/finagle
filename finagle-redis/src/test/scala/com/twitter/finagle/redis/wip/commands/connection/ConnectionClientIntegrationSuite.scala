package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.naggati.FinagleRedisClientSuite
import com.twitter.finagle.redis.tags.{RedisTest, SlowTest}
import com.twitter.util.Await
import com.twitter.finagle.redis.util.{CBToString, StringToChannelBuffer}

final class ConnectionClientIntegrationSuite extends FinagleRedisClientSuite {

  test("Correctly perform the SELECT command", RedisTest, SlowTest) {
    withRedisClient { client =>
      val actualResult =Await.result(client.select(1))
      val expectedResult = ()
      assert(actualResult === expectedResult)
    }
  }

  test("Correctly perform the QUIT command", RedisTest, SlowTest) {
    withRedisClient { client =>
      val actualResult = Await.result(client.quit())
      val expectedResult = ()
      assert(actualResult === expectedResult)
    }
  }
}
