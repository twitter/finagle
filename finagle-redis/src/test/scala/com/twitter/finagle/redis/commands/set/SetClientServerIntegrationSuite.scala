package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.naggati.RedisClientServerIntegrationTest
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util.ReplyFormat
import com.twitter.finagle.redis.tags.{ClientServerTest, RedisTest}
import com.twitter.util.Await
import scala.collection.{Set => CollectionSet}
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@Ignore
@RunWith(classOf[JUnitRunner])
final class SetClientServerIntegrationSuite extends RedisClientServerIntegrationTest {

  private[this] val key              = string2ChanBuf("member")
  private[this] val addMemErrMessage = "Could not add a member to the set"

  test("SADD should return the number of elements that were added to the set", ClientServerTest,
    RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(SAdd(key, List(foo)))) == IntegerReply(1), addMemErrMessage)
      assert(Await.result(client(SAdd(key, List(foo)))) == IntegerReply(0), "Added unknown " +
        "member to set")
      assert(Await.result(client(SAdd(key, List(bar)))) == IntegerReply(1), addMemErrMessage)
    }
  }

  test("SMEMBERS should return an array of all elements in the set", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(SAdd(key, List(foo)))) == IntegerReply(1), addMemErrMessage)
      assert(Await.result(client(SAdd(key, List(bar)))) == IntegerReply(1), addMemErrMessage)
      Await.result(client(SMembers(key))) match {
        case MBulkReply(message) => {
          val messageSet = ReplyFormat.toString(message).toSet
          assert(messageSet == CollectionSet("foo", "bar"))
        }
        case EmptyMBulkReply()   => fail("Should not have recieved an EmptyMBulkReply")
        case _                   => fail("Received incorrect reply type")
      }
    }
  }

  test("SISMEMBER should return a 1 if a member is an element of the set", ClientServerTest,
    RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(SAdd(key, List(foo)))) == IntegerReply(1), addMemErrMessage)
      assert(Await.result(client(SIsMember(key, foo))) == IntegerReply(1), "Could not find member")
      assert(Await.result(client(SAdd(key, List(bar)))) == IntegerReply(1), addMemErrMessage)
      assert(Await.result(client(SIsMember(key, bar))) == IntegerReply(1), "Could not find member")
    }
  }

  test("SISMEMBER should return a 0 if a member is not an element of the set", ClientServerTest,
    RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(SIsMember(key, foo))) == IntegerReply(0), "Found member where " +
        "none was expected")
      assert(Await.result(client(SIsMember(key, bar))) == IntegerReply(0), "Found member where " +
        "none was expected")
    }
  }


  test("SCARD should return the cardinality of the set, or 0 if key does not exist", RedisTest,
    ClientServerTest) {
    withRedisClient { client =>
      assert(Await.result(client(SCard(key))) == IntegerReply(0), "Found member where none was " +
        "expected")

      assert(Await.result(client(SAdd(key, List(foo)))) == IntegerReply(1), addMemErrMessage)
      assert(Await.result(client(SCard(key))) == IntegerReply(1), "Found incorrect cardinality")

      assert(Await.result(client(SAdd(key, List(moo)))) == IntegerReply(1), addMemErrMessage)
      assert(Await.result(client(SCard(key))) == IntegerReply(2), "Found incorrect cardinality")
    }
  }

  test("SREM should return the number of members that were removed from the set", ClientServerTest,
    RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(SAdd(key, List(baz)))) == IntegerReply(1), addMemErrMessage)
      assert(Await.result(client(SAdd(key, List(moo)))) == IntegerReply(1), addMemErrMessage)

      assert(Await.result(client(SRem(key, List(moo)))) == IntegerReply(1), "Could not remove " +
        "element from set")
      assert(Await.result(client(SIsMember(key,moo))) == IntegerReply(0), "Found member where " +
        "none was expected")
    }
  }

  test("SPOP should return the removed element, or 0 when key does not exist", ClientServerTest,
    RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(SAdd(key, List(baz)))) == IntegerReply(1), addMemErrMessage)
      assert(Await.result(client(SAdd(key, List(bar)))) == IntegerReply(1), addMemErrMessage)

      assert(Await.result(client(SCard(key))) == IntegerReply(2), "Found incorrect cardinality")
      Await.result(client(SPop(key)))
      assert(Await.result(client(SCard(key))) == IntegerReply(1), "Found incorrect cardinality")
      Await.result(client(SPop(key)))
      assert(Await.result(client(SCard(key))) == IntegerReply(0), "Found incorrect cardinality")
    }
  }
}
