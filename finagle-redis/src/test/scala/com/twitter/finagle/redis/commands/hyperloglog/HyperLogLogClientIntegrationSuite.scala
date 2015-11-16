package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.naggati.RedisClientTest
import com.twitter.finagle.redis.tags.{ClientTest, RedisTest}
import com.twitter.util.{Future, Await}

import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@Ignore
@RunWith(classOf[JUnitRunner])
final class HyperLogLogClientIntegrationSuite extends RedisClientTest {

  test("Correctly perform the PFADD command", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.pfAdd(foo, List(bar))))
    }
  }

  test("Correctly perform the PFCOUNT command", RedisTest, ClientTest) {
    withRedisClient { client =>
      val pfCountResult = client.pfAdd(foo, List(bar, baz)).flatMap(_ => client.pfCount(List(foo)))
      assert(Await.result(pfCountResult) == 2)
    }
  }

  test("Correctly perform the PFMERGE command", RedisTest, ClientTest) {
    withRedisClient { client =>
      val addHll = List((foo, List(bar, baz)), (bar, List(foo, baz))) map (client.pfAdd _).tupled
      val pfMergeResult = Future.collect(addHll).flatMap(_ => client.pfMerge(baz, List(foo, bar)))
      assert(Await.result(pfMergeResult) == ())
    }
  }
}
