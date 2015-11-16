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
    assert(codec(wrap("DEL foo\r\n")) == List(Del(List(foo))))
  }

  test("Correctly encode DELETE for two keys", CodecTest) {
    assert(codec(wrap("DEL foo bar\r\n")) == List(Del(List(foo, bar))))
  }

  test("Throw a ClientError if DELETE is called with no key") {
    intercept[ClientError] {
      codec(wrap("DEL\r\n"))
    }
  }

  test("Throw a ClientError if DUMP is called with no key", CodecTest) {
    intercept[ClientError] {
      codec(wrap("DUMP\r\n"))
    }
  }

  test("Correctly encode DUMP for one key", CodecTest) {
    assert(codec(wrap("DUMP baz\r\n")) == List(Dump(baz)))
  }

  test("Correctly encode EXISTS for one key", CodecTest) {
    assert(codec(wrap("EXISTS foo\r\n")) == List(Exists(foo)))
  }

  test("Throw a ClientError if EXISTS is called with no key") {
    intercept[ClientError] {
      codec(wrap("EXISTS\r\n"))
    }
  }

  test("Correctly encode EXPIRE for one key in 100 seconds", CodecTest) {
    assert(codec(wrap("EXPIRE moo 100\r\n")) == List(Expire(moo, 100)))
  }

  test("Correctly encode one key to never EXPIRE", CodecTest) {
    assert(codec(wrap("EXPIRE baz -1\r\n")) == List(Expire(baz, -1)))
  }

  test("Correctly encode one key to EXPIREAT at future timestamp", CodecTest) {
    assert(codec(wrap("EXPIREAT moo 100\r\n")) ==
      List(ExpireAt(moo, Time.fromMilliseconds(100*1000))))
  }

  test("Correctly encode one key to EXPIREAT a future interpolated timestamp", CodecTest) {
    val time = Time.now + 10.seconds
    unwrap(codec(wrap("EXPIREAT foo %d\r\n".format(time.inSeconds)))) {
      case ExpireAt(foo, timestamp) => {
        assert(timestamp.inSeconds == time.inSeconds)
      }
    }
  }

  test("Correctly encode a KEYS pattern", CodecTest) {
    assert(codec(wrap("KEYS h?llo\r\n")) == List(Keys(string2ChanBuf("h?llo"))))
  }

  test("Correctly encode MOVE for one key to another database", CodecTest) {
    assert(codec(wrap("MOVE boo moo \r\n")) == List(Move(boo, moo)))
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
    assert(codec(wrap("PERSIST foo\r\n")) == List(Persist(foo)))
  }

  test("Throw a ClientError if Persist is called without a key", CodecTest) {
    intercept[ClientError] {
      codec(wrap("PERSIST\r\n"))
    }
  }

  test("Correctly encode PEXPIRE for one key in 100 seconds", CodecTest) {
    assert(codec(wrap("PEXPIRE foo 100000\r\n")) == List(PExpire(foo, 100000L)))
  }

  test("Correctly encode one key to never PEXPIRE", CodecTest) {
    assert(codec(wrap("PEXPIRE baz -1\r\n")) == List(PExpire(baz, -1L)))
  }

  test("Correctly encode one key to PEXPIREAT at a future timestamp", CodecTest) {
    assert(codec(wrap("PEXPIREAT boo 100000\r\n")) ==
      List(PExpireAt(boo, Time.fromMilliseconds(100000))))
  }

  test("Correctly encode one key to PEXPIREAT at a future interpolated timestamp", CodecTest) {
    val time = Time.now + 10.seconds
    unwrap(codec(wrap("PEXPIREAT foo %d\r\n".format(time.inMilliseconds))))  {
      case PExpireAt(foo, timestamp) => {
        assert(timestamp.inMilliseconds == time.inMilliseconds)
      }
    }
  }

  test("Correctly encode a PTTL, time to live in milliseconds, for a key", CodecTest) {
    assert(codec(wrap("PTTL foo\r\n")) == List(PTtl(foo)))
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
    assert(codec(wrap("RENAME foo bar\r\n")) == List(Rename(foo, bar)))
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
    assert(codec(wrap("RENAMENX foo bar\r\n")) == List(RenameNx(foo, bar)))
  }

  test("Correctly encode a request for a RANDOMKEY", CodecTest) {
    assert(codec(wrap("RANDOMKEY\r\n")) == List(Randomkey()))
  }

  test("Correctly encode a TTL, time to live in seconds, for a key", CodecTest) {
    assert(codec(wrap("TTL foo\r\n")) == List(Ttl(foo)))
  }

  test("Throw a ClientError if TYPE is called with no key", CodecTest) {
    intercept[ClientError] {
      codec(wrap("TYPE\r\n"))
    }
  }

  test("Correctly encode a TYPE request for a provided key", CodecTest) {
    assert(codec(wrap("TYPE foo\r\n")) == List(Type(foo)))
  }
}
