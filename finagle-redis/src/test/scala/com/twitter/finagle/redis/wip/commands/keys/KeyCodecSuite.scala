package com.twitter.finagle.redis.protocol

import com.twitter.conversions.time._
import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.naggati.RedisRequestTest
import com.twitter.finagle.redis.tags.CodecTest
import com.twitter.util.Time
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class KeyCodecSuite extends RedisRequestTest {

  test("Correctly encode DELETE for one key", CodecTest) {
    val actualEncoding = codec(wrap("DEL foo\r\n"))
    val expectedEncoding = List(Del(List(foo)))
    assert(actualEncoding === expectedEncoding)
  }

  test("Correctly encode DELETE for two keys", CodecTest) {
    val actualEncoding = codec(wrap("DEL foo bar\r\n"))
    val expectedEncoding = List(Del(List(foo, bar)))
    assert(actualEncoding === expectedEncoding)
  }

  test("Throw a ClientError if DELETE is called with no key") {
    intercept[ClientError] {
      codec(wrap("DEL\r\n"))
    }
  }

  test("Correctly encode EXISTS for one key", CodecTest) {
    val actualEncoding = codec(wrap("EXISTS foo\r\n"))
    val expectedEncoding = List(Exists(foo))
    assert(actualEncoding === expectedEncoding)
  }

  test("Throw a ClientError if EXISTS is called with no key") {
    intercept[ClientError] {
      codec(wrap("EXISTS\r\n"))
    }
  }

  test("Correctly encode EXPIRE for one key in 100 seconds", CodecTest) {
    val actualEncoding = codec(wrap("EXPIRE moo 100\r\n"))
    val expectedEncoding = List(Expire(moo, 100))
    assert(actualEncoding === expectedEncoding)
  }

  test("Correctly encode one key to never EXPIRE", CodecTest) {
    val actualEncoding = codec(wrap("EXPIRE baz -1\r\n"))
    val expectedEncoding = List(Expire(baz, -1))
    assert(actualEncoding === expectedEncoding)
  }

  test("Correctly encode one key to EXPIREAT at future timestamp", CodecTest) {
    val actualEncoding = codec(wrap("EXPIREAT moo 100\r\n"))
    val expectedEncoding = List(ExpireAt(moo, Time.fromMilliseconds(100*1000)))
    assert(actualEncoding === expectedEncoding)
  }

  test("Correctly encode one key to EXPIREAT a future interpolated timestamp", CodecTest) {
    val time = Time.now + 10.seconds
    val expectedTimestamp = time.inSeconds
    unwrap(codec(wrap("EXPIREAT foo %d\r\n".format(time.inSeconds)))) {
      case ExpireAt(foo, timestamp) => {
        val actualTimestamp = timestamp.inSeconds
        assert(actualTimestamp === expectedTimestamp)
      }
    }
  }

  test("Correctly encode a KEYS pattern", CodecTest) {
    val actualEncoding = codec(wrap("KEYS h?llo\r\n"))
    val expectedEncoding = List(Keys(string2ChanBuf("h?llo")))
    assert(actualEncoding === expectedEncoding)
  }

  test("Correctly encode MOVE for one key to another database", CodecTest) {
    val actualEncoding = codec(wrap("MOVE boo moo \r\n"))
    val expectedEncoding = List(Move(boo, moo))
    assert(actualEncoding === expectedEncoding)
  }

  test("Throw a ClientError if MOVE is called with no key or database", CodecTest) {
    intercept[ClientError] {
      codec(wrap("MOVE\r\n"))
    }
  }

  test("Throw a ClientError if MOVE is called with a key but no database", CodecTest) {
    intercept[ClientError] {
      codec(wrap("MOVE foo\r\n"))
    }
  }

  test("Correctly encode PERSIST for one key", CodecTest) {
    val actualEncoding = codec(wrap("PERSIST foo\r\n"))
    val expectedEncoding =List(Persist(foo))
    assert(actualEncoding === expectedEncoding)
  }

  test("Throw a ClientError if Persist is called without a key", CodecTest) {
    intercept[ClientError] {
      codec(wrap("PERSIST\r\n"))
    }
  }

  test("Correctly encode PEXPIRE for one key in 100 seconds", CodecTest) {
    val actualEncoding = codec(wrap("PEXPIRE foo 100000\r\n"))
    val expectedEncoding = List(PExpire(foo, 100000L))
    assert(actualEncoding === expectedEncoding)
  }

  test("Correctly encode one key to never PEXPIRE", CodecTest) {
    val actualEncoding = codec(wrap("PEXPIRE baz -1\r\n"))
    val expectedEncoding = List(PExpire(baz, -1L))
    assert(actualEncoding === expectedEncoding)
  }

  test("Correctly encode one key to PEXPIREAT at a future timestamp", CodecTest) {
    val actualEncoding = codec(wrap("PEXPIREAT boo 100000\r\n"))
    val expectedEncoding = List(PExpireAt(boo, Time.fromMilliseconds(100000)))
    assert(actualEncoding === expectedEncoding)
  }

  test("Correctly encode one key to PEXPIREAT at a future interpolated timestamp", CodecTest) {
    val time = Time.now + 10.seconds
    val expectedTimestamp = time.inMilliseconds
    unwrap(codec(wrap("PEXPIREAT foo %d\r\n".format(time.inMilliseconds))))  {
      case PExpireAt(foo, timestamp) => {
        val actualTimestamp = timestamp.inMilliseconds
        assert(actualTimestamp === expectedTimestamp)
      }
    }
  }

  test("Correctly encode a PTTL, time to live in milliseconds, for a key", CodecTest) {
    val actualEncoding = codec(wrap("PTTL foo\r\n"))
    val expectedEncoding = List(PTtl(foo))
    assert(actualEncoding === expectedEncoding)
  }

  test("Throw a ClientError if RENAME is called with no arguments", CodecTest) {
    intercept[ClientError] {
      codec(wrap("RENAME\r\n"))
    }
  }

  test("Throw a ClientError if RENAME is called without a second argument", CodecTest) {
    intercept[ClientError] {
      codec(wrap("RENAME foo\r\n"))
    }
  }

  test("Correctly encode RENAME of one key to another", CodecTest) {
    val actualEncoding = codec(wrap("RENAME foo bar\r\n"))
    val expectedEncoding = List(Rename(foo, bar))
    assert(actualEncoding === expectedEncoding)
  }

  test("Throw a ClientError if RENAMEX is called with no arguments", CodecTest) {
    intercept[ClientError] {
      codec(wrap("RENAMENX\r\n"))
    }
  }

  test("Throw a ClientError if RENAMEX is called without a second argument", CodecTest) {
    intercept[ClientError] {
      codec(wrap("RENAMENX foo\r\n"))
    }
  }

  test("Correctly encode RENAMEX of one key to another", CodecTest) {
    val actualEncoding = codec(wrap("RENAMENX foo bar\r\n"))
    val expectedEncoding = List(RenameNx(foo, bar))
    assert(actualEncoding === expectedEncoding)
  }

  test("Correctly encode a request for a RANDOMKEY", CodecTest) {
    val actualEncoding = codec(wrap("RANDOMKEY\r\n"))
    val expectedEncoding = List(Randomkey())
    assert(actualEncoding === expectedEncoding)
  }

  test("Correctly encode a TTL, time to live in seconds, for a key", CodecTest) {
    val actualEncoding = codec(wrap("TTL foo\r\n"))
    val expectedEncoding = List(Ttl(foo))
    assert(actualEncoding === expectedEncoding)
  }

  test("Throw a ClientError if TYPE is called with no key", CodecTest) {
    intercept[ClientError] {
      codec(wrap("TYPE\r\n"))
    }
  }

  test("Correctly encode a TYPE request for a provided key", CodecTest) {
    val actualEncoding = codec(wrap("TYPE foo\r\n"))
    val expectedEncoding = List(Type(foo))
    assert(actualEncoding === expectedEncoding)
  }
}
