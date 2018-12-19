package com.twitter.finagle.redis.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.redis.ServerError
import com.twitter.finagle.redis.RedisClientTest
import com.twitter.finagle.redis.tags.{ClientTest, RedisTest}
import com.twitter.io.Buf
import com.twitter.util.{Await, Return}

final class TopologyClientIntegrationSuite extends RedisClientTest {

  val TIMEOUT = 10.seconds

  protected val bufKey = Buf.Utf8("1234")
  protected val bufVal = Buf.Utf8("5")
  protected val bufKeyNonNumeric = Buf.Utf8("asdf")
  protected val bufValNonNumeric = Buf.Utf8("g")
  protected val bufValLarge = Buf.Utf8("99999") // a value >= #databases (configured in redis)

  ignore("Correctly perform TOPOLOGYADD", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.topologyAdd(bufKey, bufVal).liftToTry, TIMEOUT) == Return.Unit)
    }
  }

  ignore("Correctly perform TOPOLOGYGET after TOPOLOGYADD", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.topologyAdd(bufKey, bufVal).liftToTry, TIMEOUT) == Return.Unit)
      assert(Await.result(client.topologyGet(bufKey), TIMEOUT) == Some(bufVal))
    }
  }

  ignore("Correctly perform TOPOLOGYDELETE", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.topologyDelete(bufKey).liftToTry, TIMEOUT) == Return.Unit)
    }
  }

  ignore("Correctly perform TOPOLOGYGET after TOPOLOGYDELETE", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.topologyDelete(bufKey).liftToTry, TIMEOUT) == Return.Unit)
      assert(Await.result(client.topologyGet(bufKey).liftToTry, TIMEOUT) == Return.None)
    }
  }

  test("Throw a ServerError for non-numeric key to TOPOLOGYADD", RedisTest, ClientTest) {
    withRedisClient { client =>
      intercept[ServerError] {
        Await.result(client.topologyAdd(bufKeyNonNumeric, bufVal), TIMEOUT)
      }
    }
  }

  test("Throw a ServerError for non-numeric val to TOPOLOGYADD", RedisTest, ClientTest) {
    withRedisClient { client =>
      intercept[ServerError] {
        Await.result(client.topologyAdd(bufKey, bufValNonNumeric), TIMEOUT)
      }
    }
  }

  test("Throw a ServerError for non-numeric key and val to TOPOLOGYADD", RedisTest, ClientTest) {
    withRedisClient { client =>
      intercept[ServerError] {
        Await.result(client.topologyAdd(bufKeyNonNumeric, bufValNonNumeric), TIMEOUT)
      }
    }
  }

  test("Throw a ServerError for large int val to TOPOLOGYADD", RedisTest, ClientTest) {
    withRedisClient { client =>
      intercept[ServerError] {
        Await.result(client.topologyAdd(bufKey, bufValLarge), TIMEOUT)
      }
    }
  }
}
