package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.naggati.FinagleRedisClientServerIntegrationTest
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.tags.{ClientServerTest, RedisTest}
import com.twitter.util.Await

final class ServerClientServerIntegrationSuite extends FinagleRedisClientServerIntegrationTest {

  test("FLUSHALL should return a StatusReply(\"OK\")", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val actualFlushAllReply = Await.result(client(FlushAll))
      val expectedFlushAllReply = OKStatusReply
      assert(actualFlushAllReply === expectedFlushAllReply)
    }
  }
}
