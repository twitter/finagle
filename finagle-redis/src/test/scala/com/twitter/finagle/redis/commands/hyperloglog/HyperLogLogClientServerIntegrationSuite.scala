package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.naggati.RedisClientServerIntegrationTest
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.tags.{ClientServerTest, RedisTest}
import com.twitter.finagle.redis.util.{BytesToString, StringToChannelBuffer}
import com.twitter.util.Await
import org.jboss.netty.buffer.ChannelBuffer
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@Ignore
@RunWith(classOf[JUnitRunner])
final class HyperLogLogClientServerIntegrationSuite extends RedisClientServerIntegrationTest {
  implicit def convertToChannelBuffer(s: String): ChannelBuffer = StringToChannelBuffer(s)

  test("PFADD should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(PFAdd(foo, List(bar)))) === IntegerReply(1))
    }
  }
}
