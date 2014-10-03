package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.naggati.RedisClientTest
import com.twitter.finagle.redis.tags.{RedisTest, ClientTest}
import com.twitter.util.Await
import com.twitter.finagle.redis.util.{CBToString, StringToChannelBuffer}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class ConnectionClientIntegrationSuite extends RedisClientTest {

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
