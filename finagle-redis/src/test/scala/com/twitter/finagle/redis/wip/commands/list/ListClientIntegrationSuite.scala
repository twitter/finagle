package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.naggati.RedisClientTest
import com.twitter.finagle.redis.tags.{ClientTest, RedisTest}
import com.twitter.util.Await
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class ListClientIntegrationSuite extends RedisClientTest {

  val IndexFailureMessage = "Unknown failure calling Index"
  val PopFailureMessage   = "Failure popping element from list"

  test("Correctly push elements onto a list", RedisTest, ClientTest) {
    withRedisClient { client =>
      val actualFirstLPushResult = Await.result(client.lPush(foo, List(bar)))
      val expectedFirstLPushResult = 1
      assert(actualFirstLPushResult === expectedFirstLPushResult)

      val actualSecondLPushResult = Await.result(client.lPush(foo, List(baz)))
      val expectedSecondLPushResult = 2
      assert(actualSecondLPushResult === expectedSecondLPushResult)
    }
  }

  test("Correctly pop elements off a list", RedisTest, ClientTest) {
    withRedisClient { client =>
      val actualFirstLPushResult = Await.result(client.lPush(foo, List(bar)))
      val expectedFirstLPushResult = 1
      assert(actualFirstLPushResult === expectedFirstLPushResult)
      val actualSecondLPushResult = Await.result(client.lPush(foo, List(baz)))
      val expectedSecondLPushResult = 2
      assert(actualSecondLPushResult === expectedSecondLPushResult)

      val actualFirstLPopResult = Await.result(client.lPop(foo))
        .getOrElse(fail(PopFailureMessage))
      val expectedFirstLPopResult = baz
      assert(actualFirstLPopResult === expectedFirstLPopResult)

      val actualSecondLPopResult = Await.result(client.lPop(foo))
        .getOrElse(fail(PopFailureMessage))
      val expectedSecondLPopResult = bar
      assert(actualSecondLPopResult === expectedSecondLPopResult)
    }
  }

  test("Correctly measure length of an actively changing list", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = foo

      val actualFirstLLenResult = Await.result(client.lLen(key))
      val expectedFirstLLenResult = 0
      assert(actualFirstLLenResult === expectedFirstLLenResult)

      assert(Await.result(client.lPush(foo, List(bar))) === 1, "Failed to insert list item.")
      val actualSecondLLenResult = Await.result(client.lLen(key))
      val expectedSecondLLenResult = 1
      assert(actualSecondLLenResult === expectedSecondLLenResult)

      val actuallPopResult = Await.result(client.lPop(key))
        .getOrElse(fail(PopFailureMessage))
      val expectedlPopResult = string2ChanBuf("bar")
      assert(actuallPopResult === expectedlPopResult)

      val actualFourthLLenResult = Await.result(client.lLen(key))
      val expectedFourthLLenResult = 0
      assert(actualFourthLLenResult === expectedFourthLLenResult)
    }
  }

  test("Correctly index elements of an actively changing list", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = string2ChanBuf("lindex")

      val actualFirstLIndexResult = Await.result(client.lIndex(key, 0))
      val expectedFirstLIndexResult = None
      assert(actualFirstLIndexResult === expectedFirstLIndexResult)

      val actualFirstLPushResult = Await.result(client.lPush(key, List(bar)))
      val expectedFirstLPushResult = 1
      assert(actualFirstLPushResult === expectedFirstLPushResult)

      val actualSecondLIndexResult = Await.result(client.lIndex(key, 0))
        .getOrElse(fail(IndexFailureMessage))
      val expectedSecondLIndexResult = bar
      assert(actualSecondLIndexResult === expectedSecondLIndexResult)

      val actualSecondLPushResult = Await.result(client.lPush(key, List(baz)))
      val expectedSecondLPushResult = 2
      assert(actualSecondLPushResult === expectedSecondLPushResult)

      val actualThirdLIndexResult = Await.result(client.lIndex(key, 0))
        .getOrElse(fail(IndexFailureMessage))
      val expectedThirdLIndexResult = baz
      assert(actualThirdLIndexResult === expectedThirdLIndexResult)

      val actualFirstLPopResult = Await.result(client.lPop(key))
        .getOrElse(fail(PopFailureMessage))
      val expectedFirstLPopResult = baz
      assert(actualFirstLPopResult === expectedFirstLPopResult)

      val actualFourthLIndexResult = Await.result(client.lIndex(key, 0))
        .getOrElse(fail(IndexFailureMessage))
      val expectedFourthLIndexResult = bar
      assert(actualFourthLIndexResult === expectedFourthLIndexResult)

      val actualSecondLPopResult = Await.result(client.lPop(key))
        .getOrElse(fail(PopFailureMessage))
      val expectedSecondLPopResult = bar
      assert(actualSecondLPopResult === expectedSecondLPopResult)
    }
  }

  test("Correctly insert before & after a pushed element", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = string2ChanBuf("linsert")
      val PivotFailureMessage = "Pivot not found"

      val actualFirstLPushResult = Await.result(client.lPush(key, List(moo)))
      val expectedFirstLPushResult = 1
      assert(actualFirstLPushResult === expectedFirstLPushResult)

      val actualFirstInsertAfterResult = Await.result(client.lInsertAfter(key, moo, bar))
        .getOrElse(fail(PivotFailureMessage))
      val expectedFirstInsertAfterResult = 2
      assert(actualFirstInsertAfterResult === expectedFirstInsertAfterResult)

      val actualFirstInsertBeforeResult = Await.result(client.lInsertBefore(key, moo, foo))
        .getOrElse(fail(PivotFailureMessage))
      val expectedFirstInsertBeforeResult = 3
      assert(actualFirstInsertBeforeResult === expectedFirstInsertBeforeResult)

      val actualFirstLPopResult= Await.result(client.lPop(key))
        .getOrElse(fail(PopFailureMessage))
      val expectedFirstLPopResult = foo
      assert(actualFirstLPopResult === expectedFirstLPopResult)

      val actualSecondLPopResult= Await.result(client.lPop(key))
        .getOrElse(fail(PopFailureMessage))
      val expectedSecondLPopResult = moo
      assert(actualSecondLPopResult === expectedSecondLPopResult)

      val actualThirdLPopResult= Await.result(client.lPop(key))
        .getOrElse(fail(PopFailureMessage))
      val expectedThirdLPopResult = bar
      assert(actualThirdLPopResult === expectedThirdLPopResult)
    }
  }

  test("Correctly Push -->> Remove -->> Pop elements in order", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = string2ChanBuf("lremove")

      val actualFirstLPushResult = Await.result(client.lPush(key, List(bar)))
      val expectedFirstLPushResult = 1
      assert(actualFirstLPushResult === expectedFirstLPushResult)

      val actualSecondLPushResult = Await.result(client.lPush(key, List(baz)))
      val expectedSecondLPushResult = 2
      assert(actualSecondLPushResult === expectedSecondLPushResult)

      val actualLRemoveResult = Await.result(client.lRem(key, 1, baz))
      val expectedRemoveResult = 1
      assert(actualLRemoveResult === expectedRemoveResult)

      val actualLPopResult = Await.result(client.lPop(key))
        .getOrElse(fail(PopFailureMessage))
      val expectedLPopResult = bar
      assert(actualLPopResult === expectedLPopResult)
    }
  }

  test("Correctly push members, set one, then pop them off in order", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = string2ChanBuf("lset")

      val actualFirstLPushResult = Await.result(client.lPush(key, List(bar)))
      val expectedFirstLPushResult = 1
      assert(actualFirstLPushResult === expectedFirstLPushResult)

      val actualSecondLPushResult = Await.result(client.lPush(key, List(baz)))
      val expectedSecondLPushResult = 2
      assert(actualSecondLPushResult === expectedSecondLPushResult)

      /*
       * We ingloriously side effect here as the API returns no verification,
       * if the key isn't found in the List a ServerError is thrown
      */
      Await.result(client.lSet(key, 0, moo))

      val actualFirstLPopResult = Await.result(client.lPop(key))
        .getOrElse(fail(PopFailureMessage))
      val expectedFirstLPopResult = moo
      assert(actualFirstLPopResult === expectedFirstLPopResult)

      val actualSecondLPopResult = Await.result(client.lPop(key))
        .getOrElse(fail(PopFailureMessage))
      val expectedSecondLPopResult = bar
      assert(actualSecondLPopResult === expectedSecondLPopResult)
    }
  }

  test("Correctly push members, examine the range, then pop them off", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = string2ChanBuf("lrange")

      val actualFirstLPushResult = Await.result(client.lPush(key, List(bar)))
      val expectedFirstLPushResult = 1
      assert(actualFirstLPushResult === expectedFirstLPushResult)

      val actualSecondLPushResult = Await.result(client.lPush(key, List(baz)))
      val expectedSecondLPushResult = 2
      assert(actualSecondLPushResult === expectedSecondLPushResult)

      val actualLRangeResult = Await.result(client.lRange(key, 0, -1))
      val expectedLRangeResult = List(baz, bar)
      assert(actualLRangeResult === expectedLRangeResult)

      val actualFirstLPopResult = Await.result(client.lPop(key))
        .getOrElse(fail(PopFailureMessage))
      val expectedFirstLPopResult = baz
      assert(actualFirstLPopResult === expectedFirstLPopResult)

      val actualSecondLPopResult = Await.result(client.lPop(key))
        .getOrElse(fail(PopFailureMessage))
      val expectedSecondLPopResult = bar
      assert(actualSecondLPopResult === expectedSecondLPopResult)
    }
  }

  test("Correctly push members, the poll members queue style (RPOP)", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = string2ChanBuf("rpop")

      val actualFirstLPushResult = Await.result(client.lPush(key, List(bar)))
      val expectedFirstLPushResult = 1
      assert(actualFirstLPushResult === expectedFirstLPushResult)

      val actualSecondLPushResult = Await.result(client.lPush(key, List(baz)))
      val expectedSecondLPushResult = 2
      assert(actualSecondLPushResult === expectedSecondLPushResult)

      val actualFirstRPopResult = Await.result(client.rPop(key))
        .getOrElse(fail(PopFailureMessage))
      val expectedFirstRPopResult = bar
      assert(actualFirstRPopResult === expectedFirstRPopResult)

      val actualSecondRPopResult = Await.result(client.rPop(key))
        .getOrElse(fail(PopFailureMessage))
      val expectedSecondRPopResult = baz
      assert(actualSecondRPopResult === expectedSecondRPopResult)
    }
  }

  test("Correctly push then pop members from the HEAD (RPUSH RPOP)", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = string2ChanBuf("rpush")

      val actualFirstRPushResult = Await.result(client.rPush(key, List(bar)))
      val expectedFirstRPushResult = 1
      assert(actualFirstRPushResult === expectedFirstRPushResult)

      val actualSecondRPushResult = Await.result(client.rPush(key, List(baz)))
      val expectedSecondRPushResult = 2
      assert(actualSecondRPushResult === expectedSecondRPushResult)

      val actualFirstRPopResult = Await.result(client.rPop(key))
        .getOrElse(fail(PopFailureMessage))
      val expectedFirstRPopResult = baz
      assert(actualFirstRPopResult === expectedFirstRPopResult)

      val actualSecondRPopResult = Await.result(client.rPop(key))
        .getOrElse(fail(PopFailureMessage))
      val expectedSecondRPopResult = bar
      assert(actualSecondRPopResult === expectedSecondRPopResult)
    }
  }

  ignore("Correctly push and trim members, then pop the two remaining", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = string2ChanBuf("ltrim")

      val actualFirstLPushResult = Await.result(client.lPush(key, List(bar)))
      val expectedFirstLPushResult = 1
      assert(actualFirstLPushResult === expectedFirstLPushResult)

      val actualSecondLPushResult = Await.result(client.lPush(key, List(baz)))
      val expectedSecondLPushResult = 2
      assert(actualSecondLPushResult === expectedSecondLPushResult)

      val actualThirdLPushResult = Await.result(client.lPush(key, List(moo)))
      val expectedThirdLPushResult = 3
      assert(actualThirdLPushResult === expectedThirdLPushResult)

      /*
       * We ingloriously side effect here as the API returns no verification,
       * if the key isn't found in the List a ServerError is thrown
      */
      Await.result(client.lTrim(key, 0, 1))

      val actualFourthLPushResult = Await.result(client.lPush(key, List(moo)))
      val expectedFourthLPushResult = 3
      assert(actualFourthLPushResult === expectedFourthLPushResult)

      /*
       * We ingloriously side effect here as the API returns no verification,
       * if the key isn't found in the List a ServerError is thrown
      */
      Await.result(client.lTrim(key, 0, 1))

      val actualFirstRPopResult = Await.result(client.lPop(key))
        .getOrElse(fail(PopFailureMessage))
      val expectedFirstRPopResult = boo
      assert(actualFirstRPopResult === expectedFirstRPopResult)

      val actualSecondRPopResult = Await.result(client.lPop(key))
        .getOrElse(fail(PopFailureMessage))
      val expectedSecondRPopResult = moo
      assert(actualSecondRPopResult === expectedSecondRPopResult)
    }
    /*"push members, trimming as we go.  then pop off the two remaining." in {
      val key = StringToChannelBuffer("ltrim")
      Await.result(client.lPush(key, List(bar))) mustEqual 1
      Await.result(client.lPush(key, List(baz))) mustEqual 2
      Await.result(client.lPush(key, List(boo))) mustEqual 3
      Await.result(client.lTrim(key, 0, 1))
      Await.result(client.lPush(key, List(moo))) mustEqual 3
      Await.result(client.lTrim(key, 0, 1))
      Await.result(client.rPop(key)) map (CBToString(_) mustEqual "boo")
      Await.result(client.rPop(key)) map (CBToString(_) mustEqual "moo")
    }*/
  }
}
