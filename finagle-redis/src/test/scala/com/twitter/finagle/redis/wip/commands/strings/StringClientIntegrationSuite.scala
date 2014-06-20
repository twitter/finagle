package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.naggati.FinagleRedisClientSuite
import com.twitter.finagle.redis.tags.{RedisTest, SlowTest}
import com.twitter.util.Await
import com.twitter.finagle.redis.util.{CBToString, StringToChannelBuffer}

final class StringClientIntegrationSuite extends FinagleRedisClientSuite {

  test("Correctly perform the APPEND command", RedisTest, SlowTest) {
    withRedisClient { client =>
      Await.result(client.set(foo, bar))
      val actualResult = Await.result(client.append(foo, baz))
      val expectedResult = 6
      assert(actualResult === expectedResult)
    }
  }

  test("Correctly perform the DECRBY command", RedisTest, SlowTest) {
    withRedisClient { client =>
      Await.result(client.set(foo, StringToChannelBuffer("21")))
      val actualResult = Await.result(client.decrBy(foo, 2))
      val expectedResult = 19
      assert(actualResult === expectedResult)
    }
  }

  test("Correctly perform the GETRANGE command", RedisTest, SlowTest) {
    withRedisClient { client =>
      Await.result(client.set(foo, StringToChannelBuffer("boing")))
      val actualResult = CBToString(Await.result(client.getRange(foo, 0, 2)).get)
      val expectedResult = "boi"
      assert(actualResult === expectedResult)
    }
  }

  test("Correctly perform the GET & SET commands", RedisTest, SlowTest) {
    withRedisClient { client =>
      val actualEmptyGetResult = Await.result(client.get(foo))
      val expectedEmptyGetResult = None
      assert(actualEmptyGetResult === expectedEmptyGetResult)

      Await.result(client.set(foo, bar))
      val actualGetResult = CBToString(Await.result(client.get(foo)).get)
      val expectedGetResult = "bar"
      assert(actualGetResult === expectedGetResult)
    }
  }

}
