package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.naggati.RedisClientTest
import com.twitter.finagle.redis.tags.{ClientTest, RedisTest}
import com.twitter.util.Await
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@Ignore
@RunWith(classOf[JUnitRunner])
final class ListClientIntegrationSuite extends RedisClientTest {

  val IndexFailureMessage = "Unknown failure calling Index"
  val PopFailureMessage   = "Failure popping element from list"

  test("Correctly push elements onto a list", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.lPush(foo, List(bar))) == 1)
      assert(Await.result(client.lPush(foo, List(baz))) == 2)
    }
  }

  test("Correctly pop elements off a list", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.lPush(foo, List(bar))) == 1)
      assert(Await.result(client.lPush(foo, List(baz))) == 2)
      assert(Await.result(client.lPop(foo)).getOrElse(fail(PopFailureMessage)) == baz)
      assert(Await.result(client.lPop(foo)).getOrElse(fail(PopFailureMessage)) == bar)
    }
  }

  test("Correctly measure length of an actively changing list", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = foo

      assert(Await.result(client.lLen(key)) == 0)
      assert(Await.result(client.lPush(foo, List(bar))) == 1, "Failed to insert list item.")
      assert(Await.result(client.lLen(key)) == 1)
      assert(Await.result(client.lPop(key)).getOrElse(fail(PopFailureMessage)) ==
        string2ChanBuf("bar"))
      assert(Await.result(client.lLen(key)) == 0)
    }
  }

  test("Correctly index elements of an actively changing list", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = string2ChanBuf("lindex")

      assert(Await.result(client.lIndex(key, 0)) == None)
      assert(Await.result(client.lPush(key, List(bar))) == 1)
      assert(Await.result(client.lIndex(key, 0)).getOrElse(fail(IndexFailureMessage)) == bar)
      assert(Await.result(client.lPush(key, List(baz))) == 2)
      assert(Await.result(client.lIndex(key, 0)).getOrElse(fail(IndexFailureMessage)) == baz)
      assert(Await.result(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == baz)
      assert(Await.result(client.lIndex(key, 0)).getOrElse(fail(IndexFailureMessage)) == bar)
      assert(Await.result(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == bar)
    }
  }

  test("Correctly insert before & after a pushed element", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = string2ChanBuf("linsert")
      val PivotFailureMessage = "Pivot not found"

      assert(Await.result(client.lPush(key, List(moo))) == 1)
      assert(
        Await.result(client.lInsertAfter(key, moo, bar)).getOrElse(fail(PivotFailureMessage)) == 2)
      assert(
        Await.result(
          client.lInsertBefore(key, moo, foo)).getOrElse(fail(PivotFailureMessage)) == 3)
      assert(Await.result(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == foo)
      assert(Await.result(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == moo)
      assert(Await.result(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == bar)
    }
  }

  test("Correctly Push -->> Remove -->> Pop elements in order", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = string2ChanBuf("lremove")

      assert(Await.result(client.lPush(key, List(bar))) == 1)
      assert(Await.result(client.lPush(key, List(baz))) == 2)
      assert(Await.result(client.lRem(key, 1, baz)) == 1)
      assert(Await.result(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == bar)
    }
  }

  test("Correctly push members, set one, then pop them off in order", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = string2ChanBuf("lset")

      assert(Await.result(client.lPush(key, List(bar))) == 1)
      assert(Await.result(client.lPush(key, List(baz))) == 2)

      /*
       * We ingloriously side effect here as the API returns no verification,
       * if the key isn't found in the List a ServerError is thrown
      */
      Await.result(client.lSet(key, 0, moo))

      assert(Await.result(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == moo)
      assert(Await.result(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == bar)
    }
  }

  test("Correctly push members, examine the range, then pop them off", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = string2ChanBuf("lrange")

      assert(Await.result(client.lPush(key, List(bar))) == 1)
      assert(Await.result(client.lPush(key, List(baz))) == 2)
      assert(Await.result(client.lRange(key, 0, -1)) == List(baz, bar))
      assert(Await.result(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == baz)
      assert(Await.result(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == bar)
    }
  }

  test("Correctly push members, the poll members queue style (RPOP)", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = string2ChanBuf("rpop")

      assert(Await.result(client.lPush(key, List(bar))) == 1)
      assert(Await.result(client.lPush(key, List(baz))) == 2)
      assert(Await.result(client.rPop(key)).getOrElse(fail(PopFailureMessage)) == bar)
      assert(Await.result(client.rPop(key)).getOrElse(fail(PopFailureMessage)) == baz)
    }
  }

  test("Correctly push then pop members from the HEAD (RPUSH RPOP)", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = string2ChanBuf("rpush")

      assert(Await.result(client.rPush(key, List(bar))) == 1)
      assert(Await.result(client.rPush(key, List(baz))) == 2)
      assert(Await.result(client.rPop(key)).getOrElse(fail(PopFailureMessage)) == baz)
      assert(Await.result(client.rPop(key)).getOrElse(fail(PopFailureMessage)) == bar)
    }
  }

  test("Correctly push and trim members, then pop the two remaining", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = string2ChanBuf("ltrim")

      assert(Await.result(client.lPush(key, List(bar))) == 1)
      assert(Await.result(client.lPush(key, List(baz))) == 2)
      assert(Await.result(client.lPush(key, List(boo))) == 3)

      /*
       * We ingloriously side effect here as the API returns no verification,
       * if the key isn't found in the List a ServerError is thrown
      */
      Await.result(client.lTrim(key, 0, 1))

      assert(Await.result(client.lPush(key, List(moo))) == 3)

      /*
       * We ingloriously side effect here as the API returns no verification,
       * if the key isn't found in the List a ServerError is thrown
      */
      Await.result(client.lTrim(key, 0, 1))

      assert(Await.result(client.rPop(key)).getOrElse(fail(PopFailureMessage)) == boo)
      assert(Await.result(client.rPop(key)).getOrElse(fail(PopFailureMessage)) == moo)
    }
  }
}
