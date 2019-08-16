package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.RedisRequestTest
import com.twitter.finagle.redis.tags.CodecTest
import com.twitter.io.Buf
import org.scalacheck.Gen

final class StringCodecSuite extends RedisRequestTest {

  test("APPEND", CodecTest) { checkSingleKeySingleVal("APPEND", Append.apply) }
  test("GET", CodecTest) { checkSingleKey("GET", Get.apply) }
  test("GETSET", CodecTest) { checkSingleKeySingleVal("GETSET", GetSet.apply) }
  test("MGET", CodecTest) { checkMultiKey("MGET", MGet.apply) }

  test("BITCOUNT", CodecTest) {
    assert(encodeCommand(BitCount(Buf.Utf8("foo"), None, None)) == Seq("BITCOUNT", "foo"))
    assert(
      encodeCommand(BitCount(Buf.Utf8("foo"), Some(1), Some(2))) == Seq("BITCOUNT", "foo", "1", "2")
    )
  }

  test("BITOP", CodecTest) {
    forAll(Gen.oneOf("AND", "OR", "XOR", "NOT")) { op =>
      assert(
        encodeCommand(BitOp(Buf.Utf8(op), Buf.Utf8("foo"), Seq(Buf.Utf8("bar")))) ==
          Seq("BITOP", op, "foo", "bar")
      )
    }
  }

  test("DECR", CodecTest) { checkSingleKey("DECR", Decr.apply) }

  test("DECRBY", CodecTest) {
    checkSingleKeyArbitraryVal("DECRBY", (k: Buf, v: Long) => DecrBy(k, v))
  }

  test("GETBIT", CodecTest) {
    checkSingleKeyArbitraryVal("GETBIT", (k: Buf, v: Int) => GetBit(k, v))
  }

  test("GETRANGE", CodecTest) {
    assert(encodeCommand(GetRange(Buf.Utf8("foo"), 0, 42)) == Seq("GETRANGE", "foo", "0", "42"))
  }

  test("INCR", CodecTest) { checkSingleKey("INCR", Incr.apply) }
  test("INCRBY", CodecTest) {
    checkSingleKeyArbitraryVal("INCRBY", (k: Buf, v: Int) => IncrBy(k, v))
  }
}
