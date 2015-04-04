package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.naggati.RedisClientTest
import com.twitter.finagle.redis.protocol.BitOp
import com.twitter.finagle.redis.tags.{RedisTest, ClientTest}
import com.twitter.util.Await
import com.twitter.finagle.redis.util.{CBToString, StringToChannelBuffer}
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@Ignore
@RunWith(classOf[JUnitRunner])
final class HyperLogLogClientIntegrationSuite extends RedisClientTest {

  test("Correctly perform the PFADD command", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.pfAdd(foo, List(bar)))
    }
  }
}
