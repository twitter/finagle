package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.naggati.RedisClientServerIntegrationTest
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.tags.{ClientServerTest, RedisTest}
import com.twitter.finagle.redis.util.StringToChannelBuffer
import com.twitter.util.{Await, Future}
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
      assert(Await.result(client(PFAdd(foo, List(bar)))) == IntegerReply(1))
    }
  }

  test("PFCOUNT should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val pfCountResult = client(PFAdd("foo", List("bar", "baz")))
        .flatMap(_ => client(PFCount(List(StringToChannelBuffer("foo")))))
      assert(Await.result(pfCountResult) == IntegerReply(2))
    }
  }

  test("PFMERGE should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val setup = List(PFAdd("foo", List("bar")), PFAdd("bar", List("baz"))) map client
      val pfMergeResult = Future.collect(setup).flatMap(_ => client(PFMerge("baz", List("foo", "bar"))))
      assert(Await.result(pfMergeResult) == OKStatusReply)
    }
  }
}
