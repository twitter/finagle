package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.naggati.RedisClientServerIntegrationTest
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.tags.{ClientServerTest, RedisTest}
import com.twitter.util.Await
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@Ignore
@RunWith(classOf[JUnitRunner])
final class ListClientServerIntegrationSuite extends RedisClientServerIntegrationTest {

  test("LLEN should return the length of the list", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(LPush(foo, List(bar)))) == IntegerReply(1))

      assert(Await.result(client(LLen(foo))) == IntegerReply(1))

      assert(Await.result(client(LPush(foo, List(bar)))) == IntegerReply(2))

      assert(Await.result(client(LLen(foo))) == IntegerReply(2))
    }
  }

  test("LINDEX should get an element from a list by its index", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(LPush(foo, List(bar)))) == IntegerReply(1))

      assertBulkReply(client(LIndex(foo, 0)), chanBuf2String(bar))

      assert(Await.result(client(LPush(foo, List(baz)))) == IntegerReply(2))

      assertBulkReply(client(LIndex(foo, 0)), chanBuf2String(baz))

      assertBulkReply(client(LIndex(foo, 1)), chanBuf2String(bar))
    }
  }

  test("LINSERT should insert an element before or after another element in a list",
    ClientServerTest, RedisTest) {
      withRedisClient { client =>
        assert(Await.result(client(LPush(foo, List(bar)))) == IntegerReply(1))

        assert(Await.result(client(LInsert(foo, "BEFORE", bar, moo))) == IntegerReply(2))
      }
  }

  test("LPOP should remove and get the first element in a list", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(LPush(foo, List(bar)))) == IntegerReply(1))

      assert(Await.result(client(LPush(foo, List(moo)))) == IntegerReply(2))

      assertBulkReply(client(LPop(foo)), chanBuf2String(moo))

      assertBulkReply(client(LPop(foo)), chanBuf2String(bar))
    }
  }

  test("LPUSH should prepend one or multiple values to a list", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(LPush(foo, List(bar)))) == IntegerReply(1))

      assert(Await.result(client(LLen(foo))) == IntegerReply(1))

      assert(Await.result(client(LPush(foo, List(baz)))) == IntegerReply(2))

      assert(Await.result(client(LLen(foo))) == IntegerReply(2))
    }
  }

  test("LREM should remove elements from a list", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(LPush(foo, List(bar)))) == IntegerReply(1))

      assert(Await.result(client(LPush(foo, List(bar)))) == IntegerReply(2))

      assert(Await.result(client(LRem(foo, 1, bar))) == IntegerReply(1))

      assertMBulkReply(client(LRange(foo, 0, -1)), List(chanBuf2String(bar)))
    }
  }

  test("LSET should et the value of an element in a list by its index", ClientServerTest,
    RedisTest) {
      withRedisClient { client =>
        assert(Await.result(client(LPush(foo, List(bar)))) == IntegerReply(1))

        assert(Await.result(client(LPush(foo, List(bar)))) == IntegerReply(2))

        assert(Await.result(client(LSet(foo, 1, baz))) == OKStatusReply)

        assertMBulkReply(client(LRange(foo, 0, -1)), List(chanBuf2String(bar), chanBuf2String(baz)))
      }
  }

  test("LRANGE should get a range of elements from a list", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(LPush(foo, List(bar)))) == IntegerReply(1))

      assert(Await.result(client(LPush(foo, List(bar)))) == IntegerReply(2))

      assertMBulkReply(client(LRange(foo, 0, -1)), List(chanBuf2String(bar), chanBuf2String(bar)))
    }
  }

  test("RPOP should remove and get the last element in a list", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(LPush(foo, List(bar)))) == IntegerReply(1))

      assert(Await.result(client(LPush(foo, List(baz)))) == IntegerReply(2))

      assertBulkReply(client(RPop(foo)), chanBuf2String(bar))

      assertBulkReply(client(RPop(foo)), chanBuf2String(baz))
    }
  }

  test("RPUSH should append one or multiple values to a list", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(RPush(foo, List(moo)))) == IntegerReply(1))

      assert(Await.result(client(RPush(foo, List(boo)))) == IntegerReply(2))
    }
  }

  test("LTRIM should trim a list to the specified range", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(LPush(foo, List(moo)))) == IntegerReply(1))

      assert(Await.result(client(LPush(foo, List(boo)))) == IntegerReply(2))

      assert(Await.result(client(LPush(foo, List(baz)))) == IntegerReply(3))

      assert(Await.result(client(LTrim(foo, 0, 1))) == OKStatusReply)

      assert(Await.result(client(LPush(foo, List(bar)))) == IntegerReply(3))

      assert(Await.result(client(LTrim(foo, 0, 1))) == OKStatusReply)

      assertMBulkReply(client(LRange(foo, 0, -1)), List(chanBuf2String(bar), chanBuf2String(baz)))
    }
  }
}
