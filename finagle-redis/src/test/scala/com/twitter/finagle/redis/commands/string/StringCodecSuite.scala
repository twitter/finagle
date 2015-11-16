package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.naggati.RedisRequestTest
import com.twitter.finagle.redis.tags.CodecTest
import com.twitter.finagle.redis.util.{BytesToString, StringToChannelBuffer}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class StringCodecSuite extends RedisRequestTest {

  test("Throw a ClientError if APPEND is called with no key or value", CodecTest) {
    intercept[ClientError] {
      codec(wrap("APPEND\r\n"))
    }
  }

  test("Throw a ClientError if APPEND is called with no value", CodecTest) {
    intercept[ClientError] {
      codec(wrap("APPEND foo\r\n"))
    }
  }

  test("Correctly encode APPEND") {
    val foo = StringToChannelBuffer("foo")

    unwrap(codec(wrap("APPEND foo bar\r\n"))) {
      case Append(foo, value) =>
        assert(BytesToString(value.array) == "bar")
    }
  }

  test("Correctly encode BITCOUNT") {
    assert(codec(wrap("BITCOUNT foo\r\n")) == List(BitCount(StringToChannelBuffer("foo"))))
  }

  test("Throw a ClientError if BITCOUNT is called with start but no end", CodecTest) {
    intercept[ClientError] {
      codec(wrap("BITCOUNT foo 0\r\n"))
    }
  }

  test("Correctly encode BITCOUNT with start and end") {
    assert(codec(wrap("BITCOUNT foo 0 1\r\n")) ==
      List(BitCount(StringToChannelBuffer("foo"), Some(0), Some(1))))
  }

  test("Throw a ClientError if BITOP is called with no arguments") {
    intercept[ClientError] {
      codec(wrap("BITOP\r\n"))
    }
  }

  test("Correctly encode BITOP AND") {
    assert(codec(wrap("BITOP AND baz foo bar\r\n")) ==
      List(
        BitOp(
          BitOp.And,
          StringToChannelBuffer("baz"),
          Seq(StringToChannelBuffer("foo"), StringToChannelBuffer("bar")))
      ))
  }

  test("Throw a ClientError if BITOP NOT is called with three arguments") {
    intercept[ClientError] {
      codec(wrap("BITOP NOT foo bar baz\r\n"))
    }
  }

  test("Correctly encode BITOP NOT") {
    assert(codec(wrap("BITOP NOT foo bar\r\n")) ==
      List(BitOp(BitOp.Not, StringToChannelBuffer("foo"), Seq(StringToChannelBuffer("bar")))))
  }

  test("Correctly encode DECR with an integer key") {
    assert(codec(wrap("DECR 1\r\n")) == List(Decr(StringToChannelBuffer("1"))))
  }

  test("Correctly encode DECR with a string key") {
    assert(codec(wrap("DECR foo\r\n")) == List(Decr(StringToChannelBuffer("foo"))))
  }

  test("Throw a ClientError if DECR is called with two arguments") {
    intercept[ClientError] {
      codec(wrap("DECR foo 1\r\n"))
    }
  }

  test("Correctly encode DECRBY") {
    assert(codec(wrap("DECRBY foo 1\r\n")) == List(DecrBy(StringToChannelBuffer("foo"), 1)))
    assert(codec(wrap("DECRBY foo 4096\r\n")) == List(DecrBy(StringToChannelBuffer("foo"), 4096)))
  }

  test("Throw a ClientError if DECRBY is called with one argument") {
    intercept[ClientError] {
      codec(wrap("DECRBY foo\r\n"))
    }
  }

  test("Correctly encode GET") {
    assert(codec(wrap("GET foo\r\n")) == List(Get(StringToChannelBuffer("foo"))))
  }

  test("Throw a ClientError if GETBIT is called with no arguments") {
    intercept[ClientError] {
      codec(wrap("GETBIT\r\n"))
    }
  }

  test("Throw a ClientError if GETBIT is called with one argument") {
    intercept[ClientError] {
      codec(wrap("GETBIT foo\r\n"))
    }
  }

  test("Correctly encode GETBIT") {
    assert(codec(wrap("GETBIT foo 0\r\n")) == List(GetBit(StringToChannelBuffer("foo"), 0)))
  }

  test("Throw a ClientError if GETRANGE is called with no arguments") {
    intercept[ClientError] {
      codec(wrap("GETRANGE\r\n"))
    }
  }

  test("Throw a ClientError if GETRANGE is called with one argument") {
    intercept[ClientError] {
      codec(wrap("GETRANGE key\r\n"))
    }
  }

  test("Throw a ClientError if GETRANGE is called with two arguments") {
    intercept[ClientError] {
      codec(wrap("GETRANGE key 0\r\n"))
    }
  }

  test("Correctly encode GETRANGE") {
    assert(codec(wrap("GETRANGE key 0 5\r\n")) ==
      List(GetRange(StringToChannelBuffer("key"), 0, 5)))
  }

  test("Throw a ClientError if GETSET is called with no arguments") {
    intercept[ClientError] {
      codec(wrap("GETSET\r\n"))
    }
  }

  test("Throw a ClientError if GETSET is called with one argument") {
    intercept[ClientError] {
      codec(wrap("GETSET key\r\n"))
    }
  }

  test("Correctly encode GETSET") {
    val key = StringToChannelBuffer("key")

    unwrap(codec(wrap("GETSET key value\r\n"))) {
      case GetSet(key, value) =>
        assert(BytesToString(value.array) == "value")
    }
  }

  test("Correctly encode INCR with an integer argument") {
    assert(codec(wrap("INCR 1\r\n")) ==
      List(Incr(StringToChannelBuffer("1"))))
  }

  test("Correctly encode INCR with a string argument") {
    assert(codec(wrap("INCR foo\r\n")) ==
      List(Incr(StringToChannelBuffer("foo"))))
  }

  test("Throw a ClientError if INCR is called with two arguments") {
    intercept[ClientError] {
      codec(wrap("INCR foo 1\r\n"))
    }
  }

  test("Correctly encode INCRBY") {
    assert(codec(wrap("INCRBY foo 1\r\n")) == List(IncrBy(StringToChannelBuffer("foo"), 1)))
    assert(codec(wrap("INCRBY foo 4096\r\n")) == List(IncrBy(StringToChannelBuffer("foo"), 4096)))
  }

  test("Throw a ClientError if INCRBY is called with one argument") {
    intercept[ClientError] {
      codec(wrap("INCRBY foo\r\n"))
    }
  }

  test("Correctly encode MGET") {
    assert(codec(wrap("MGET foo bar\r\n")) ==
      List(MGet(List(StringToChannelBuffer("foo"), StringToChannelBuffer("bar")))))
  }

  test("Throw a ClientError if MSETNX is called with no arguments") {
    intercept[ClientError] {
      codec(wrap("MSETNX\r\n"))
    }
  }

  test("Throw a ClientError if MSETNX is called with one argument") {
    intercept[ClientError] {
      codec(wrap("MSETNX foo\r\n"))
    }
  }

  test("Correctly encode MSETNX") {
    unwrap(codec(wrap("MSETNX foo bar\r\n"))) {
      case MSetNx(map) =>
        assert(map.contains(foo))
        assert(BytesToString(map(foo).array) == "bar")
    }
  }

  test("Throw a ClientError if PSETEX is called with no arguments") {
    intercept[ClientError] {
      codec(wrap("PSETEX\r\n"))
    }
  }

  test("Correctly encode PSETEX") {
    assert(codec(wrap("PSETEX foo 1000 bar\r\n")) ==
      List(PSetEx(StringToChannelBuffer("foo"), 1000L, StringToChannelBuffer("bar"))))
  }

  test("Correctly encode SET") {
    unwrap(codec(wrap("SET foo bar\r\n"))) {
      case Set(foo, bar, _, _, _) =>
        assert(BytesToString(bar.array) == "bar")
    }
  }

  test("Throw a ClientError if SETBIT is called with no arguments") {
    intercept[ClientError] {
      codec(wrap("SETBIT\r\n"))
    }
  }

  test("Throw a ClientError if SETBIT is called with one argument") {
    intercept[ClientError] {
      codec(wrap("SETBIT foo\r\n"))
    }
  }

  test("Throw a ClientError if SETBIT is called with two arguments") {
    intercept[ClientError] {
      codec(wrap("SETBIT foo 0\r\n"))
    }
  }

  test("Correctly encode SETBIT") {
    assert(codec(wrap("SETBIT foo 7 1\r\n")) ==
      List(SetBit(StringToChannelBuffer("foo"), 7, 1)))
  }

  test("Throw a ClientError if SETEX is called with no arguments") {
    intercept[ClientError] {
      codec(wrap("SETEX\r\n"))
    }
  }

  test("Throw a ClientError if SETEX is called with one argument") {
    intercept[ClientError] {
      codec(wrap("SETEX key\r\n"))
    }
  }

  test("Throw a ClientError if SETEX is called with two arguments") {
    intercept[ClientError] {
      codec(wrap("SETEX key 30\r\n"))
    }
  }

  test("Correctly encode SETEX") {
    unwrap(codec(wrap("SETEX key 30 value\r\n"))) {
      case SetEx(key, 30, value) =>
        assert(BytesToString(value.array) == "value")
    }
  }

  test("Throw a ClientError if SETNX is called with no arguments") {
    intercept[ClientError] {
      codec(wrap("SETNX\r\n"))
    }
  }

  test("Throw a ClientError if SETNX is called with one argument") {
    intercept[ClientError] {
      codec(wrap("SETNX key\r\n"))
    }
  }

  test("Correctly encode SETNX") {
    unwrap(codec(wrap("SETNX key value\r\n"))) {
      case SetNx(key, value) =>
        assert(BytesToString(value.array) == "value")
    }
  }

  test("Throw a ClientError if the new SET syntax is called with two strings and an integer") {
    intercept[ClientError] {
      codec(wrap("SET foo bar 100\r\n"))
    }
  }

  test("Throw a ClientError if the new SET syntax is called with two strings and EX NX") {
    intercept[ClientError] {
      codec(wrap("SET foo bar EX NX\r\n"))
    }
  }

  test("Throw a ClientError if the new SET syntax is called with two strings and PX NX") {
    intercept[ClientError] {
      codec(wrap("SET foo bar PX NX\r\n"))
    }
  }

  test("Correctly encode the new SET syntax with EX") {
    unwrap(codec(wrap("SET foo bar EX 10\r\n"))) {
      case Set(key, value, ttl, false, false) =>
        assert(ttl == Some(InSeconds(10L)))
      }
  }

  test("Correctly encode the new SET syntax with PX") {
    unwrap(codec(wrap("SET foo bar PX 10000\r\n"))) {
      case Set(key, value, ttl, false, false) =>
        assert(ttl == Some(InMilliseconds(10000L)))
      }
  }

  test("Correctly encode the new SET syntax with NX EX") {
    unwrap(codec(wrap("SET foo bar NX EX 10\r\n"))) {
      case Set(key, value, ttl, true, false) =>
        assert(ttl == Some(InSeconds(10)))
      }
  }

  test("Correctly encode the new SET syntax with XX") {
    unwrap(codec(wrap("SET foo bar XX\r\n"))) {
      case Set(key, value, None, false, true) =>
        assert(BytesToString(value.array) == "bar")
      }
  }


  test("Throw a ClientError if SETRANGE is called with no arguments") {
    intercept[ClientError] {
      codec(wrap("SETRANGE\r\n"))
    }
  }

  test("Throw a ClientError if SETRANGE is called with one argument") {
    intercept[ClientError] {
      codec(wrap("SETRANGE key\r\n"))
    }
  }

  test("Throw a ClientError if SETRANGE is called with two arguments") {
    intercept[ClientError] {
      codec(wrap("SETRANGE key 0\r\n"))
    }
  }

  test("Correctly encode SETRANGE") {
    unwrap(codec(wrap("SETRANGE key 0 value\r\n"))) {
      case SetRange(key, 0, value) =>
        assert(BytesToString(value.array) == "value")
    }
  }

  test("Throw a ClientError if STRLEN is called with no arguments") {
    intercept[ClientError] {
      codec(wrap("STRLEN\r\n"))
    }
  }

  test("Correctly encode STRLEN") {
    assert(codec(wrap("STRLEN foo\r\n")) == List(Strlen(StringToChannelBuffer("foo"))))
  }

  // Unified GET request
  test("Correctly encode array size in unified GET requests", CodecTest) {
    assert(codec(wrap("*2\r\n")) == Nil)
  }

  test("Correctly encode command string size in unified GET requests", CodecTest) {
    assert(codec(wrap("$3\r\n")) == Nil)
  }

  test("Correctly encode the command in unified GET requests", CodecTest) {
    assert(codec(wrap("GET\r\n")) == Nil)
  }

  test("Correctly encode string size in unified GET requests", CodecTest) {
    assert(codec(wrap("$3\r\n")) == Nil)
  }

  test("Correctly encode unified GET requests", CodecTest) {
    assert(codec(wrap("bar\r\n")) == List(Get(StringToChannelBuffer("bar"))))
  }

  // Unified MGET request
  test("Correctly encode array size in unified MGET requests", CodecTest) {
    assert(codec(wrap("*3\r\n")) == Nil)
  }

  test("Correctly encode command string size in unified MGET requests", CodecTest) {
    assert(codec(wrap("$4\r\n")) == Nil)
  }

  test("Correctly encode the command in unified MGET requests", CodecTest) {
    assert(codec(wrap("MGET\r\n")) == Nil)
  }

  test("Correctly encode string size in unified MGET requests 1", CodecTest) {
    assert(codec(wrap("$3\r\n")) == Nil)
  }

  test("Correctly encode string in unified MGET requests", CodecTest) {
    assert(codec(wrap("foo\r\n")) == Nil)
  }

  test("Correctly encode string size in unified MGET requests 2", CodecTest) {
    assert(codec(wrap("$3\r\n")) == Nil)
  }

  test("Correctly encode unified MGET requests", CodecTest) {
    assert(codec(wrap("bar\r\n")) ==
      List(MGet(List(StringToChannelBuffer("foo"), StringToChannelBuffer("bar")))))
  }

  // Unified MSET request
  test("Correctly encode array size in unified MSET requests", CodecTest) {
    assert(codec(wrap("*5\r\n")) == Nil)
  }

  test("Correctly encode command string size in unified MSET requests", CodecTest) {
    assert(codec(wrap("$4\r\n")) == Nil)
  }

  test("Correctly encode the command in unified MSET requests", CodecTest) {
    assert(codec(wrap("MSET\r\n")) == Nil)
  }

  test("Correctly encode string size in unified MSET requests 1", CodecTest) {
    assert(codec(wrap("$3\r\n")) == Nil)
  }

  test("Correctly encode string in unified MSET requests 1", CodecTest) {
    assert(codec(wrap("foo\r\n")) == Nil)
  }

  test("Correctly encode string size in unified MSET requests 2", CodecTest) {
    assert(codec(wrap("$7\r\n")) == Nil)
  }

  test("Correctly encode string in unified MSET requests 2", CodecTest) {
    assert(codec(wrap("bar baz\r\n")) == Nil)
  }

  test("Correctly encode string size in unified MSET requests 3", CodecTest) {
    assert(codec(wrap("$3\r\n")) == Nil)
  }

  test("Correctly encode string in unified MSET requests 3", CodecTest) {
    assert(codec(wrap("bar\r\n")) == Nil)
  }

  test("Correctly encode string size in unified MSET requests 4", CodecTest) {
    assert(codec(wrap("$5\r\n")) == Nil)
  }

  test("Correctly encode unified MSET requests", CodecTest) {
    codec(wrap("Hello\r\n")) match {
      case MSet(kv) :: Nil =>
        val nkv = kv.map {
          case(k, v) => (BytesToString(k.array), BytesToString(v.array))
        }
        assert(nkv == Map("foo" -> "bar baz", "bar" -> "Hello"))
      case _ => fail("Expected MSet to be returned")
    }
  }
}
