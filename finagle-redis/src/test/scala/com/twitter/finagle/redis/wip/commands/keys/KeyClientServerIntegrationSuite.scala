package com.twitter.finagle.redis.integration

import com.twitter.conversions.time._
import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.naggati.RedisClientServerIntegrationTest
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.tags.{ClientServerTest, RedisTest}
import com.twitter.util.{Await, Future, Time}
import org.jboss.netty.buffer.ChannelBuffer
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class KeyClientServerIntegrationSuite extends RedisClientServerIntegrationTest {

  test("DELETE two keys", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val actualSetReply0 = Await.result(client(Set(foo, bar)))
      assert(actualSetReply0 === OKStatusReply)

      val actualSetReply1 = Await.result(client(Set(moo, baz)))
      assert(actualSetReply1 === OKStatusReply)

      val actualDeletedReply = Await.result(client(Del(List(foo, moo))))
      val expectedDeletedReply = IntegerReply(2)
      assert(actualDeletedReply === expectedDeletedReply)
    }
  }

  test("DELETE should throw ClientError when given null list", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      intercept[ClientError] {
        Await.result(client(Del(null:List[ChannelBuffer])))
      }
    }
  }

  test("DELETE should throw ClientError when given an empty List", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      intercept[ClientError] {
        Await.result(client(Del(List[ChannelBuffer]())))
      }
    }
  }

  test("EXISTS should return an IntegerReply of 0 for a non-existent key",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val actualReply = Await.result(client(Exists(string2ChanBuf("nosuchkey"))))
      val expectedReply = IntegerReply(0)
      assert(actualReply === expectedReply)
    }
  }

  test("EXISTS should return and IntegerReply of 1 for an existing key",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val actualSetReply = Await.result(client(Set(foo, bar)))
      assert(actualSetReply === OKStatusReply)

      val actualReply = Await.result(client(Exists(foo)))
      val expectedReply = IntegerReply(1)
      assert(actualReply === expectedReply)
    }
  }

  test("EXISTS should throw ClientError when given a null ChannelBuffer",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      intercept[ClientError] {
        Await.result(client(Exists(null:ChannelBuffer)))
      }
    }
  }

  test("EXPIRE should return an IntegerReply of 0 to verify a INVALID timeout was set",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val actualExpireReply = Await.result(client(Expire(baz, 30)))
      val expectedExpireReply = IntegerReply(0)
      assert(actualExpireReply === expectedExpireReply)
    }
  }

  test("EXPIRE should return an IntegerReply of 1 to verify a VALID timeout was set",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val actualSetReply = Await.result(client(Set(foo, bar)))
      assert(actualSetReply === OKStatusReply)

      val actualExpireReply = Await.result(client(Expire(foo, 30)))
      val expectedExpireReply = IntegerReply(1)
      assert(actualExpireReply === expectedExpireReply)
    }
  }

  test("EXPIRE should throw ClientError when given a null key", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      intercept[ClientError] {
        Await.result(client(Expire(null:ChannelBuffer, 30)))
      }
    }
  }

  test("EXPIREAT should return an IntegerReply of 0 to verify a INVALID timeout was set",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val actualExpireReply = Await.result(client(ExpireAt(boo, Time.now + 3600.seconds)))
      val expectedExpireReply = IntegerReply(0)
      assert(actualExpireReply === expectedExpireReply)
    }
  }

  test("EXPIREAT should return an IntegerReply of 1 to verify a VALID timeout was set",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val actualSetReply = Await.result(client(Set(foo, bar)))
      assert(actualSetReply === OKStatusReply)

      val actualExpireReply = Await.result(client(ExpireAt(foo, Time.now + 3600.seconds)))
      val expectedExpireReply = IntegerReply(1)
      assert(actualExpireReply === expectedExpireReply)
    }
  }

  test("EXPIREAT should throw ClientError when given a null key", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      intercept[ClientError] {
        Await.result(client(ExpireAt(null:ChannelBuffer, Time.now + 3600.seconds)))
      }
    }
  }

  test("KEYS should return a list of all keys in the database that match the provided pattern",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val actualSetReply = Await.result(client(Set(foo, bar)))
      assert(actualSetReply === OKStatusReply)

      val request = client(Keys(string2ChanBuf("*")))
      val expects = List("foo")
      assertMBulkReply(request, expects, true)
    }
  }

  test("MOVE should throw ClientError when given an empty or null key",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val toDBDoesNotMatter = string2ChanBuf("71")
      val blankKey = string2ChanBuf("")
      intercept[ClientError] {
        Await.result(client(Move(blankKey, toDBDoesNotMatter)))
      }
      val nullKey = null:ChannelBuffer
      intercept[ClientError] {
        Await.result(client(Move(nullKey, toDBDoesNotMatter)))
      }
    }
  }

  test("MOVE should throw ClientError when given an empty or null toDatabase",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val blankToDb = string2ChanBuf("")
      intercept[ClientError] {
        Await.result(client(Move(moo, blankToDb)))
      }
      val nullToDb = null:ChannelBuffer
      intercept[ClientError] {
        Await.result(client(Move(moo, nullToDb)))
      }
    }
  }

  test("MOVE should return an Integer Reply of 1 to verify a key was moved",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val fromDb = 14
      val toDb = 15
      assert(Await.result(client(Select(toDb))) === OKStatusReply)
      Await.result(client(Del(List(baz))))
      assert(Await.result(client(Select(fromDb))) === OKStatusReply)

      val actualSetReply = Await.result(client(Set(baz, bar)))
      assert(actualSetReply === OKStatusReply)

      val actualMoveReply = Await.result(client(Move(baz, string2ChanBuf(toDb.toString))))
      val expectedMoveReply = IntegerReply(1)
      assert(actualMoveReply === expectedMoveReply)
    }
  }

  test("MOVE should return an Integer Reply of 0 to show a MOVE was not completed",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      Await.result(client(Select(1)))
      val toDb = string2ChanBuf("14")
      val actualMoveReply= Await.result(client(Move(moo, toDb)))
      val expectedMoveReply = IntegerReply(0)
      assert(actualMoveReply === expectedMoveReply)
    }
  }

  test("PERSIST should return an IntegerReply of 0 when no key is found",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val expectedReply = IntegerReply(0)
      val actualReply = Await.result(client(Persist(string2ChanBuf("nosuchKey"))))
      assert(actualReply === expectedReply)
    }
  }

  test("PERSIST should return an IntegerReply of 0 when a found key has no associated timeout",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val actualSetReply = Await.result(client(Set(foo, bar)))
      assert(actualSetReply === OKStatusReply)

      val expectedReply = IntegerReply(0)
      val actualReply = Await.result(client(Persist(foo)))
      assert(actualReply === expectedReply)
    }
  }

  test("PERSIST should return an IntegerReply of 1 when removing an associated timeout",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val actualSetReply = Await.result(client(Set(baz, bar)))
      assert(actualSetReply === OKStatusReply)
      val actualExpireReply = Await.result(client(Expire(baz, 30)))
      val expectedExpireReply = IntegerReply(1)
      assert(actualExpireReply === expectedExpireReply, "FATAL could not expire existing key")

      val expectedReply = IntegerReply(1)
      val actualReply = Await.result(client(Persist(baz)))
      assert(actualReply === expectedReply)
    }
  }

  test("RENAME should return an ErrorReply when renaming a key to the original name",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val originalKeyName = string2ChanBuf("rename1")
      val actualSetReply = Await.result(client(Set(originalKeyName, bar)))
      assert(actualSetReply === OKStatusReply)

      val actualReply = Await.result(client(Rename(originalKeyName, originalKeyName)))
      val actualReplyClass = actualReply.getClass.getName.split('.').last
      val expectedReplyClass = ErrorReply.getClass.getName.split('.').last.dropRight(1)
      assert(actualReplyClass === expectedReplyClass)
    }
  }

  test("RENAME should return an ErrorReply when renaming a key that does not exist",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val noSuchKey = string2ChanBuf("noSuchKey")

      val actualReply = Await.result(client(Rename(noSuchKey, string2ChanBuf("DOES NOT MATTER"))))
      val actualReplyClass = actualReply.getClass.getName.split('.').last
      val expectedReplyClass = ErrorReply.getClass.getName.split('.').last.dropRight(1)
      assert(actualReplyClass === expectedReplyClass)
    }
  }

  test("RENAME should return a StatusReply(\"OK\") after correctly renaming a key",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val rename1 = string2ChanBuf("rename1")
      val rename2 = string2ChanBuf("rename2")
      val actualSetReply = Await.result(client(Set(rename1, bar)))
      assert(actualSetReply === OKStatusReply)

      val actualRenameReply = Await.result(client(Rename(rename1, rename2)))
      assert(actualRenameReply === OKStatusReply)
    }
  }

  test("RENAMENX should return an ErrorReply when renaming a key to the original name",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val originalKeyName = string2ChanBuf("rename1")
      val actualSetReply = Await.result(client(Set(originalKeyName, bar)))
      assert(actualSetReply === OKStatusReply)

      val actualReply = Await.result(client(RenameNx(originalKeyName, originalKeyName)))
      val actualReplyClass = actualReply.getClass.getName.split('.').last
      val expectedReplyClass = ErrorReply.getClass.getName.split('.').last.dropRight(1)
      assert(actualReplyClass === expectedReplyClass)
    }
  }

  test("RENAMENX should return an ErrorReply when renaming a key that does not exist",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val noSuchKey = string2ChanBuf("noSuchKey")

      val actualReply = Await.result(client(RenameNx(noSuchKey, string2ChanBuf("DOES NOT MATTER"))))
      val actualReplyClass = actualReply.getClass.getName.split('.').last
      val expectedReplyClass = ErrorReply.getClass.getName.split('.').last.dropRight(1)
      assert(actualReplyClass === expectedReplyClass)
    }
  }

  test("RENAMENX should an IntegerReply of 1 to verify a key was renamed",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val rename1 = string2ChanBuf("rename1")
      val rename2 = string2ChanBuf("rename2")
      val actualSetReply = Await.result(client(Set(rename1, bar)))
      assert(actualSetReply === OKStatusReply)

      val actualRenameReplyNx = Await.result(client(RenameNx(rename1, rename2)))
      val expectedReply = IntegerReply(1)
      assert(actualRenameReplyNx === expectedReply)
    }
  }

  test("RENAMENX should return an IntegerReply of 0 to verify a key rename did not occur when the" +
    " the new key name already exists", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val rename1 = string2ChanBuf("rename1")
      val actualSetReply0 = Await.result(client(Set(rename1, bar)))
      assert(actualSetReply0 === OKStatusReply)

      val rename2 = string2ChanBuf("rename2")
      val actualSetReply1 = Await.result(client(Set(rename2, baz)))
      assert(actualSetReply1 === OKStatusReply)

      val actualRenameNxReply = Await.result(client(RenameNx(rename1, rename2)))
      val expectedReply = IntegerReply(0)
      assert(actualRenameNxReply === expectedReply)
    }
  }

  test("RANDOMKEY should return a BulkReply", ClientServerTest, RedisTest) {
    withRedisClient{ client =>
      val actualReply = Await.result(client(Randomkey()))
      val actualReplyClass = actualReply.getClass.getName.split('.').last.takeRight(9)
      val expectedReplyClass = BulkReply.getClass.getName.split('.').last.dropRight(1).takeRight(9)
      assert(actualReplyClass === expectedReplyClass)
    }
  }

  test("TTL should throw a ClientError when given an empty key", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val emptyKey = string2ChanBuf("")

      intercept[ClientError] {
        Await.result(client(Ttl(emptyKey)))
      }
    }
  }

  test("TTL should return an IntegerReply of -1 when the key exists and has no associated timeout",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val actualSetReply = Await.result(client(Set(foo, bar)))
      assert(actualSetReply === OKStatusReply)

      val actualReply = Await.result(client(Ttl(foo)))
      val expectedReply = IntegerReply(-1)
      assert(actualReply === expectedReply)
    }
  }

  test("TYPE should return a StatusReply(\"string\") for a string type stored at given key",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val actualSetReply = Await.result(client(Set(foo, bar)))
      assert(actualSetReply === OKStatusReply)

      val actualReply = Await.result(client(Type(foo)))
      val expectedReply = StatusReply("string")
      assert(actualReply === expectedReply)
    }
  }

  test("TYPE should return a StatusReply(\"none\") when given a key with no assicated value",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val actualReply = Await.result(client(Type(moo)))
      val expectedReply = StatusReply("none")
      assert(actualReply === expectedReply)
    }
  }
}
