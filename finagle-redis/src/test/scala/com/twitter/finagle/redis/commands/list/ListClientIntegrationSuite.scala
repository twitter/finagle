package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.RedisClientTest
import com.twitter.finagle.redis.tags.{ClientTest, RedisTest}
import com.twitter.io.Buf
import com.twitter.util.Await
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@Ignore
@RunWith(classOf[JUnitRunner])
final class ListClientIntegrationSuite extends RedisClientTest {

  val IndexFailureMessage = "Unknown failure calling Index"
  val PopFailureMessage = "Failure popping element from list"

  test("Correctly push elements onto a list", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.lPush(bufFoo, List(bufBar))) == 1)
      assert(Await.result(client.lPush(bufFoo, List(bufBaz))) == 2)
    }
  }

  test("Correctly pop elements off a list", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.lPush(bufFoo, List(bufBar))) == 1)
      assert(Await.result(client.lPush(bufFoo, List(bufBaz))) == 2)
      assert(Await.result(client.lPop(bufFoo)).getOrElse(fail(PopFailureMessage)) == bufBaz)
      assert(Await.result(client.lPop(bufFoo)).getOrElse(fail(PopFailureMessage)) == bufBar)
    }
  }

  test("Correctly measure length of an actively changing list", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = bufFoo

      assert(Await.result(client.lLen(key)) == 0)
      assert(Await.result(client.lPush(bufFoo, List(bufBar))) == 1, "Failed to insert list item.")
      assert(Await.result(client.lLen(key)) == 1)
      assert(Await.result(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == bufBar)
      assert(Await.result(client.lLen(key)) == 0)
    }
  }

  test("Correctly index elements of an actively changing list", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = Buf.Utf8("lindex")

      assert(Await.result(client.lIndex(key, 0L)) == None)
      assert(Await.result(client.lPush(key, List(bufBar))) == 1)
      assert(Await.result(client.lIndex(key, 0L)).getOrElse(fail(IndexFailureMessage)) == bufBar)
      assert(Await.result(client.lPush(key, List(bufBaz))) == 2)
      assert(Await.result(client.lIndex(key, 0L)).getOrElse(fail(IndexFailureMessage)) == bufBaz)
      assert(Await.result(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == bufBaz)
      assert(Await.result(client.lIndex(key, 0L)).getOrElse(fail(IndexFailureMessage)) == bufBar)
      assert(Await.result(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == bufBar)
    }
  }

  test("Correctly insert before & after a pushed element", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = Buf.Utf8("linsert")
      val PivotFailureMessage = "Pivot not found"

      assert(Await.result(client.lPush(key, List(bufMoo))) == 1)
      assert(
        Await
          .result(client.lInsertAfter(key, bufMoo, bufBar))
          .getOrElse(fail(PivotFailureMessage)) == 2
      )
      assert(
        Await
          .result(client.lInsertBefore(key, bufMoo, bufFoo))
          .getOrElse(fail(PivotFailureMessage)) == 3
      )
      assert(Await.result(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == bufFoo)
      assert(Await.result(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == bufMoo)
      assert(Await.result(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == bufBar)
    }
  }

  test("Correctly Push -->> Remove -->> Pop elements in order", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = Buf.Utf8("lremove")

      assert(Await.result(client.lPush(key, List(bufBar))) == 1)
      assert(Await.result(client.lPush(key, List(bufBaz))) == 2)
      assert(Await.result(client.lRem(key, 1L, bufBaz)) == 1)
      assert(Await.result(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == bufBar)
    }
  }

  test("Correctly push members, set one, then pop them off in order", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = Buf.Utf8("lset")

      assert(Await.result(client.lPush(key, List(bufBar))) == 1)
      assert(Await.result(client.lPush(key, List(bufBaz))) == 2)

      /*
       * We ingloriously side effect here as the API returns no verification,
       * if the key isn't found in the List a ServerError is thrown
       */
      Await.result(client.lSet(key, 0L, bufMoo))

      assert(Await.result(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == bufMoo)
      assert(Await.result(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == bufBar)
    }
  }

  test("Correctly push members, examine the range, then pop them off", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = Buf.Utf8("lrange")

      assert(Await.result(client.lPush(key, List(bufBar))) == 1)
      assert(Await.result(client.lPush(key, List(bufBaz))) == 2)
      assert(Await.result(client.lRange(key, 0L, -1L)) == List(bufBaz, bufBar))
      assert(Await.result(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == bufBaz)
      assert(Await.result(client.lPop(key)).getOrElse(fail(PopFailureMessage)) == bufBar)
    }
  }

  test("Correctly push members, the poll members queue style (RPOP)", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = Buf.Utf8("rpop")

      assert(Await.result(client.lPush(key, List(bufBar))) == 1)
      assert(Await.result(client.lPush(key, List(bufBaz))) == 2)
      assert(Await.result(client.rPop(key)).getOrElse(fail(PopFailureMessage)) == bufBar)
      assert(Await.result(client.rPop(key)).getOrElse(fail(PopFailureMessage)) == bufBaz)
    }
  }

  test("Correctly push then pop members from the HEAD (RPUSH RPOP)", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = Buf.Utf8("rpush")

      assert(Await.result(client.rPush(key, List(bufBar))) == 1)
      assert(Await.result(client.rPush(key, List(bufBaz))) == 2)
      assert(Await.result(client.rPop(key)).getOrElse(fail(PopFailureMessage)) == bufBaz)
      assert(Await.result(client.rPop(key)).getOrElse(fail(PopFailureMessage)) == bufBar)
    }
  }

  test("Correctly push and trim members, then pop the two remaining", RedisTest, ClientTest) {
    withRedisClient { client =>
      val key = Buf.Utf8("ltrim")

      assert(Await.result(client.lPush(key, List(bufBar))) == 1)
      assert(Await.result(client.lPush(key, List(bufBaz))) == 2)
      assert(Await.result(client.lPush(key, List(bufBoo))) == 3)

      /*
       * We ingloriously side effect here as the API returns no verification,
       * if the key isn't found in the List a ServerError is thrown
       */
      Await.result(client.lTrim(key, 0L, 1L))

      assert(Await.result(client.lPush(key, List(bufMoo))) == 3)

      /*
       * We ingloriously side effect here as the API returns no verification,
       * if the key isn't found in the List a ServerError is thrown
       */
      Await.result(client.lTrim(key, 0L, 1L))

      assert(Await.result(client.rPop(key)).getOrElse(fail(PopFailureMessage)) == bufBoo)
      assert(Await.result(client.rPop(key)).getOrElse(fail(PopFailureMessage)) == bufMoo)
    }
  }
}
