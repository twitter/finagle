package com.twitter.finagle.redis.integration

import com.twitter.conversions.time._
import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.naggati.RedisClientServerIntegrationTest
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.tags.{ClientServerTest, RedisTest}
import com.twitter.finagle.redis.util.{StringToBuf, StringToChannelBuffer}
import com.twitter.io.Buf
import com.twitter.util.{Await, Time}
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@Ignore
@RunWith(classOf[JUnitRunner])
final class KeyClientServerIntegrationSuite extends RedisClientServerIntegrationTest {

  test("DELETE two keys", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(Set(foo, bar))) == OKStatusReply)
      assert(Await.result(client(Set(moo, baz))) == OKStatusReply)
      assert(Await.result(client(Del(List(bufFoo, bufMoo)))) == IntegerReply(2))
    }
  }

  test("DELETE should throw ClientError when given null list", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      intercept[ClientError] {
        Await.result(client(Del(null:List[Buf])))
      }
    }
  }

  test("DELETE should throw ClientError when given an empty List", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      intercept[ClientError] {
        Await.result(client(Del(List[Buf]())))
      }
    }
  }

  test("DUMP", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val k = StringToChannelBuffer("mykey")
      val v = StringToChannelBuffer("10")
      val key = StringToBuf("mykey")
      val value = StringToBuf("10")
      assert(Await.result(client(Set(k, v))) == OKStatusReply)
      assert(Await.result(client(Dump(key))).isInstanceOf[BulkReply])
      assert(Await.result(client(Del(List(key)))) == IntegerReply(1))
      assert(Await.result(client(Dump(key))) == EmptyBulkReply())
    }
  }

  test("EXISTS should return an IntegerReply of 0 for a non-existent key",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(Exists(StringToBuf("nosuchkey")))) == IntegerReply(0))
    }
  }

  test("EXISTS should return and IntegerReply of 1 for an existing key",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(Set(foo, bar))) == OKStatusReply)
      assert(Await.result(client(Exists(bufFoo))) == IntegerReply(1))
    }
  }

  test("EXISTS should throw ClientError when given a null ChannelBuffer",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      intercept[ClientError] {
        Await.result(client(Exists(null:Buf)))
      }
    }
  }

  test("EXPIRE should return an IntegerReply of 0 to verify a INVALID timeout was set",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(Expire(bufBaz, 30))) == IntegerReply(0))
    }
  }

  test("EXPIRE should return an IntegerReply of 1 to verify a VALID timeout was set",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(Set(foo, bar))) == OKStatusReply)
      assert(Await.result(client(Expire(bufFoo, 30))) == IntegerReply(1))
    }
  }

  test("EXPIRE should throw ClientError when given a null key", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      intercept[ClientError] {
        Await.result(client(Expire(null:Buf, 30)))
      }
    }
  }

  test("EXPIREAT should return an IntegerReply of 0 to verify a INVALID timeout was set",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(ExpireAt(bufBoo, Time.now + 3600.seconds))) == IntegerReply(0))
    }
  }

  test("EXPIREAT should return an IntegerReply of 1 to verify a VALID timeout was set",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(Set(foo, bar))) == OKStatusReply)
      assert(Await.result(client(ExpireAt(bufFoo, Time.now + 3600.seconds))) == IntegerReply(1))
    }
  }

  test("EXPIREAT should throw ClientError when given a null key", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      intercept[ClientError] {
        Await.result(client(ExpireAt(null:Buf, Time.now + 3600.seconds)))
      }
    }
  }

  test("KEYS should return a list of all keys in the database that match the provided pattern",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(Set(foo, bar))) == OKStatusReply)
      assertMBulkReply(client(Keys(StringToBuf("*"))), List("foo"), true)
    }
  }

  test("MOVE should throw ClientError when given an empty or null key",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val toDBDoesNotMatter = StringToBuf("71")
      val blankKey = StringToBuf("")
      intercept[ClientError] {
        Await.result(client(Move(blankKey, toDBDoesNotMatter)))
      }
      val nullKey = null: Buf
      intercept[ClientError] {
        Await.result(client(Move(nullKey, toDBDoesNotMatter)))
      }
    }
  }

  test("MOVE should throw ClientError when given an empty or null toDatabase",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val blankToDb = StringToBuf("")
      intercept[ClientError] {
        Await.result(client(Move(bufMoo, blankToDb)))
      }
      val nullToDb = null:Buf
      intercept[ClientError] {
        Await.result(client(Move(bufMoo, nullToDb)))
      }
    }
  }

  test("MOVE should return an Integer Reply of 1 to verify a key was moved",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val fromDb = 14
      val toDb = 15
      assert(Await.result(client(Select(toDb))) == OKStatusReply)
      Await.result(client(Del(List(bufBaz))))
      assert(Await.result(client(Select(fromDb))) == OKStatusReply)
      assert(Await.result(client(Set(baz, bar))) == OKStatusReply)
      assert(Await.result(client(Move(bufBaz, StringToBuf(toDb.toString)))) == IntegerReply(1))
    }
  }

  test("MOVE should return an Integer Reply of 0 to show a MOVE was not completed",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      Await.result(client(Select(1)))
      val toDb = StringToBuf("14")
      assert(Await.result(client(Move(bufMoo, toDb))) == IntegerReply(0))
    }
  }

  test("PERSIST should return an IntegerReply of 0 when no key is found",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(Persist(StringToBuf("nosuchKey")))) == IntegerReply(0))
    }
  }

  test("PERSIST should return an IntegerReply of 0 when a found key has no associated timeout",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(Set(foo, bar))) == OKStatusReply)
      assert(Await.result(client(Persist(bufFoo))) == IntegerReply(0))
    }
  }

  test("PERSIST should return an IntegerReply of 1 when removing an associated timeout",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(Set(baz, bar))) == OKStatusReply)
      assert(Await.result(client(Expire(bufBaz, 30))) == IntegerReply(1),
        "FATAL could not expire existing key")

      assert(Await.result(client(Persist(bufBaz))) == IntegerReply(1))
    }
  }

  test("RENAME should return an ErrorReply when renaming a key to the original name",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val originalKeyName = string2ChanBuf("rename1")
      val keyName = StringToBuf("rename1")
      assert(Await.result(client(Set(originalKeyName, bar))) == OKStatusReply)

      assert(
        Await.result(client(Rename(keyName, keyName))).isInstanceOf[ErrorReply])
    }
  }

  test("RENAME should return an ErrorReply when renaming a key that does not exist",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val noSuchKey = StringToBuf("noSuchKey")
      assert(
        Await.result(
          client(Rename(noSuchKey, StringToBuf("DOES NOT MATTER")))).isInstanceOf[ErrorReply])
    }
  }

  test("RENAME should return a StatusReply(\"OK\") after correctly renaming a key",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val rename1 = string2ChanBuf("rename1")
      val rename = StringToBuf("rename1")
      val rename2 = StringToBuf("rename2")
      assert(Await.result(client(Set(rename1, bar))) == OKStatusReply)
      assert(Await.result(client(Rename(rename, rename2))) == OKStatusReply)
    }
  }

  test("RENAMENX should return an ErrorReply when renaming a key to the original name",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val originalKeyName = string2ChanBuf("rename1")
      assert(Await.result(client(Set(originalKeyName, bar))) == OKStatusReply)
      assert(
        Await.result(client(RenameNx(StringToBuf("rename1"), StringToBuf("rename1")))).isInstanceOf[ErrorReply])
    }
  }

  test("RENAMENX should return an ErrorReply when renaming a key that does not exist",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val noSuchKey = StringToBuf("noSuchKey")

      assert(
        Await.result(
          client(RenameNx(noSuchKey, StringToBuf("DOES NOT MATTER")))).isInstanceOf[ErrorReply])
    }
  }

  test("RENAMENX should an IntegerReply of 1 to verify a key was renamed",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val rename1 = string2ChanBuf("rename1")
      val rename = StringToBuf("rename1")
      val rename2 = StringToBuf("rename2")
      assert(Await.result(client(Set(rename1, bar))) == OKStatusReply)
      assert(Await.result(client(RenameNx(rename, rename2))) == IntegerReply(1))
    }
  }

  test("RENAMENX should return an IntegerReply of 0 to verify a key rename did not occur when the" +
    " the new key name already exists", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val rename = string2ChanBuf("rename1")
      val rename1 = StringToBuf("rename1")
      assert(Await.result(client(Set(rename, bar))) == OKStatusReply)

      val rename2 = StringToBuf("rename2")
      assert(Await.result(client(Set(rename, baz))) == OKStatusReply)

      assert(Await.result(client(RenameNx(rename1, rename2))) == IntegerReply(0))
    }
  }

  test("RANDOMKEY should return an EmptyBulkReply", ClientServerTest, RedisTest) {
    withRedisClient{ client =>
      assert(Await.result(client(Randomkey())).isInstanceOf[EmptyBulkReply])
    }
  }

  test("TTL should throw a ClientError when given an empty key", ClientServerTest, RedisTest) {
    withRedisClient { client =>
      val emptyKey = StringToBuf("")

      intercept[ClientError] {
        Await.result(client(Ttl(emptyKey)))
      }
    }
  }

  test("TTL should return an IntegerReply of -1 when the key exists and has no associated timeout",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(Set(foo, bar))) == OKStatusReply)
      assert(Await.result(client(Ttl(bufFoo))) == IntegerReply(-1))
    }
  }

  test("TYPE should return a StatusReply(\"string\") for a string type stored at given key",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(Set(foo, bar))) == OKStatusReply)
      assert(Await.result(client(Type(bufFoo))) == StatusReply("string"))
    }
  }

  test("TYPE should return a StatusReply(\"none\") when given a key with no assicated value",
    ClientServerTest, RedisTest) {
    withRedisClient { client =>
      assert(Await.result(client(Type(bufMoo))) == StatusReply("none"))
    }
  }
}
