package com.twitter.finagle.redis.integration

import com.twitter.conversions.time._
import com.twitter.finagle.redis.ServerError
import com.twitter.finagle.redis.naggati.RedisClientTest
import com.twitter.finagle.redis.tags.{RedisTest, ClientTest}
import com.twitter.finagle.redis.util.StringToBuf
import com.twitter.util.{Return, Await}
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@Ignore
@RunWith(classOf[JUnitRunner])
final class TopologyClientIntegrationSuite extends RedisClientTest {

  val TIMEOUT = 10.seconds

  protected val bufKey = StringToBuf("1234")
  protected val bufVal = StringToBuf("5")
  protected val bufKeyNonNumeric = StringToBuf("asdf")
  protected val bufValNonNumeric = StringToBuf("g")
  protected val bufValLarge = StringToBuf("99999")  // a value >= #databases (configured in redis)

  test("Correctly perform TOPOLOGYADD", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.topologyAdd(bufKey, bufVal).liftToTry, TIMEOUT) == Return.Unit)
    }
  }

  test("Correctly perform TOPOLOGYGET after TOPOLOGYADD", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.topologyAdd(bufKey, bufVal).liftToTry, TIMEOUT) == Return.Unit)
      assert(Await.result(client.topologyGet(bufKey), TIMEOUT) == Some(bufVal))
    }
  }

  test("Correctly perform TOPOLOGYDELETE", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.topologyDelete(bufKey).liftToTry, TIMEOUT) == Return.Unit)
    }
  }

  test("Correctly perform TOPOLOGYGET after TOPOLOGYDELETE", RedisTest, ClientTest) {
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