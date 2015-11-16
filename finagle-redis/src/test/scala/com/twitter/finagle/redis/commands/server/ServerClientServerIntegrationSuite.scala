package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.naggati.RedisClientServerIntegrationTest
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.tags.{ClientServerTest, RedisTest}
import com.twitter.util.Await
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@Ignore
@RunWith(classOf[JUnitRunner])
final class ServerClientServerIntegrationSuite extends RedisClientServerIntegrationTest {

  test("FLUSHALL should return a StatusReply(\"OK\")", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(FlushAll)) == OKStatusReply)
    }
  }
}
