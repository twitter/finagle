package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.naggati.RedisClientServerIntegrationTest
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.tags.{ClientServerTest, RedisTest}
import com.twitter.io.Buf
import com.twitter.util.{Await, Future}
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@Ignore
@RunWith(classOf[JUnitRunner])
final class HyperLogLogClientServerIntegrationSuite extends RedisClientServerIntegrationTest {

  test("PFADD should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(PFAdd(bufFoo, List(bufBar)))) == IntegerReply(1))
    }
  }

  test("PFCOUNT should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val pfCountResult = client(PFAdd(Buf.Utf8("foo"), List(Buf.Utf8("bar"), Buf.Utf8("baz"))))
        .flatMap(_ => client(PFCount(List(Buf.Utf8("foo")))))
      assert(Await.result(pfCountResult) == IntegerReply(2))
    }
  }

  test("PFMERGE should work correctly", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val setup = List(
        PFAdd(Buf.Utf8("foo"), List(Buf.Utf8("bar"))),
        PFAdd(Buf.Utf8("bar"), List(Buf.Utf8("baz")))
      ).map(client)
      val pfMergeResult = Future.collect(setup).flatMap(_ =>
        client(PFMerge(Buf.Utf8("baz"), List(Buf.Utf8("foo"), Buf.Utf8("bar"))))
      )

      assert(Await.result(pfMergeResult) == OKStatusReply)
    }
  }
}
