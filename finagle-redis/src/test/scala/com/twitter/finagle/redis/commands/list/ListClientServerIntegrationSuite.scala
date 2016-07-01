package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.naggati.RedisClientServerIntegrationTest
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.tags.{ClientServerTest, RedisTest}
import com.twitter.finagle.redis.util.BufToString
import com.twitter.util.Await
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@Ignore
@RunWith(classOf[JUnitRunner])
final class ListClientServerIntegrationSuite extends RedisClientServerIntegrationTest {

  test("LLEN should return the length of the list", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(LPush(bufFoo, List(bufBar)))) == IntegerReply(1))

      assert(Await.result(client(LLen(bufFoo))) == IntegerReply(1))

      assert(Await.result(client(LPush(bufFoo, List(bufBar)))) == IntegerReply(2))

      assert(Await.result(client(LLen(bufFoo))) == IntegerReply(2))
    }
  }

  test("LINDEX should get an element from a list by its index", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(LPush(bufFoo, List(bufBar)))) == IntegerReply(1))

      assertBulkReply(client(LIndex(bufFoo, 0)), bufBar)

      assert(Await.result(client(LPush(bufFoo, List(bufBaz)))) == IntegerReply(2))

      assertBulkReply(client(LIndex(bufFoo, 0)), bufBaz)

      assertBulkReply(client(LIndex(bufFoo, 1)), bufBar)
    }
  }

  test("LINSERT should insert an element before or after another element in a list",
    ClientServerTest, RedisTest) {
      withRedisClient { client =>
        assert(Await.result(client(LPush(bufFoo, List(bufBar)))) == IntegerReply(1))

        assert(Await.result(client(LInsert(bufFoo, "BEFORE", bufBar, bufMoo))) == IntegerReply(2))
      }
  }

  test("LPOP should remove and get the first element in a list", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(LPush(bufFoo, List(bufBar)))) == IntegerReply(1))

      assert(Await.result(client(LPush(bufFoo, List(bufMoo)))) == IntegerReply(2))

      assertBulkReply(client(LPop(bufFoo)), bufMoo)

      assertBulkReply(client(LPop(bufFoo)), bufBar)
    }
  }

  test("LPUSH should prepend one or multiple values to a list", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(LPush(bufFoo, List(bufBar)))) == IntegerReply(1))

      assert(Await.result(client(LLen(bufFoo))) == IntegerReply(1))

      assert(Await.result(client(LPush(bufFoo, List(bufBaz)))) == IntegerReply(2))

      assert(Await.result(client(LLen(bufFoo))) == IntegerReply(2))
    }
  }

  test("LREM should remove elements from a list", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(LPush(bufFoo, List(bufBar)))) == IntegerReply(1))

      assert(Await.result(client(LPush(bufFoo, List(bufBar)))) == IntegerReply(2))

      assert(Await.result(client(LRem(bufFoo, 1, bufBar))) == IntegerReply(1))

      assertMBulkReply(client(LRange(bufFoo, 0, -1)), List(BufToString(bufBar)))
    }
  }

  test("LSET should et the value of an element in a list by its index", ClientServerTest,
    RedisTest) {
      withRedisClient { client =>
        assert(Await.result(client(LPush(bufFoo, List(bufBar)))) == IntegerReply(1))

        assert(Await.result(client(LPush(bufFoo, List(bufBar)))) == IntegerReply(2))

        assert(Await.result(client(LSet(bufFoo, 1, bufBaz))) == OKStatusReply)

        assertMBulkReply(client(LRange(bufFoo, 0, -1)), List(BufToString(bufBar), BufToString(bufBaz)))
      }
  }

  test("LRANGE should get a range of elements from a list", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(LPush(bufFoo, List(bufBar)))) == IntegerReply(1))

      assert(Await.result(client(LPush(bufFoo, List(bufBar)))) == IntegerReply(2))

      assertMBulkReply(client(LRange(bufFoo, 0, -1)), List(BufToString(bufBar), BufToString(bufBar)))
    }
  }

  test("RPOP should remove and get the last element in a list", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(LPush(bufFoo, List(bufBar)))) == IntegerReply(1))

      assert(Await.result(client(LPush(bufFoo, List(bufBaz)))) == IntegerReply(2))

      assertBulkReply(client(RPop(bufFoo)), bufBar)

      assertBulkReply(client(RPop(bufFoo)), bufBaz)
    }
  }

  test("RPUSH should append one or multiple values to a list", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(RPush(bufFoo, List(bufMoo)))) == IntegerReply(1))

      assert(Await.result(client(RPush(bufFoo, List(bufMoo)))) == IntegerReply(2))
    }
  }

  test("LTRIM should trim a list to the specified range", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(LPush(bufFoo, List(bufMoo)))) == IntegerReply(1))

      assert(Await.result(client(LPush(bufFoo, List(bufMoo)))) == IntegerReply(2))

      assert(Await.result(client(LPush(bufFoo, List(bufBaz)))) == IntegerReply(3))

      assert(Await.result(client(LTrim(bufFoo, 0, 1))) == OKStatusReply)

      assert(Await.result(client(LPush(bufFoo, List(bufBar)))) == IntegerReply(3))

      assert(Await.result(client(LTrim(bufFoo, 0, 1))) == OKStatusReply)

      assertMBulkReply(client(LRange(bufFoo, 0, -1)), List(BufToString(bufBar), BufToString(bufBaz)))
    }
  }
}
