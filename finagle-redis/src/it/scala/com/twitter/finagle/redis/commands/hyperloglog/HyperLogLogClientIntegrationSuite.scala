package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.RedisClientTest
import com.twitter.finagle.redis.tags.{ClientTest, RedisTest}
import com.twitter.util.{Await, Future, Return}

final class HyperLogLogClientIntegrationSuite extends RedisClientTest {

  test("Correctly perform the PFADD command", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.pfAdd(bufFoo, List(bufBar))).booleanValue)
    }
  }

  test("Correctly perform the PFCOUNT command", RedisTest, ClientTest) {
    withRedisClient { client =>
      val pfCountResult =
        client.pfAdd(bufFoo, List(bufBar, bufBaz)).flatMap(_ => client.pfCount(List(bufFoo)))
      assert(Await.result(pfCountResult) == 2)
    }
  }

  test("Correctly perform the PFMERGE command", RedisTest, ClientTest) {
    withRedisClient { client =>
      val addHll =
        List(
          (bufFoo, List(bufBar, bufBaz)),
          (bufBar, List(bufFoo, bufBaz))) map (client.pfAdd _).tupled
      val pfMergeResult =
        Future.collect(addHll).flatMap(_ => client.pfMerge(bufBaz, List(bufFoo, bufBar)))
      assert(Await.result(pfMergeResult.liftToTry) == Return.Unit)
    }
  }
}
