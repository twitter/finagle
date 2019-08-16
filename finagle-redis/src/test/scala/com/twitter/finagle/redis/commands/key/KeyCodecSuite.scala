package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.RedisRequestTest
import com.twitter.finagle.redis.tags.CodecTest
import com.twitter.io.Buf
import com.twitter.util.{Duration, Time}
import java.net.InetSocketAddress

final class KeyCodecSuite extends RedisRequestTest {

  test("DEL", CodecTest) { checkMultiKey("DEL", Del.apply) }
  test("DUMP", CodecTest) { checkSingleKey("DUMP", Dump.apply) }
  test("EXISTS", CodecTest) { checkSingleKey("EXISTS", Exists.apply) }
  test("KEYS", CodecTest) { checkSingleKey("KEYS", Keys.apply) }
  test("MIGRATE", CodecTest) {
    val a = new InetSocketAddress("127.0.0.1", 9999)
    val k = Seq(Buf.Utf8("foo"))
    val ks = Seq(Buf.Utf8("foo"), Buf.Utf8("bar"))
    val d = Duration.fromMilliseconds(5000)
    assert(
      encodeCommand(Migrate(a, k, d)) == Seq(
        "MIGRATE",
        "127.0.0.1",
        "9999",
        "",
        "0",
        "5000",
        "KEYS",
        "foo"
      )
    )
    assert(
      encodeCommand(Migrate(a, ks, d)) == Seq(
        "MIGRATE",
        "127.0.0.1",
        "9999",
        "",
        "0",
        "5000",
        "KEYS",
        "foo",
        "bar"
      )
    )
  }
  test("MOVE", CodecTest) { checkSingleKeySingleVal("MOVE", Move.apply) }
  test("PERSIST", CodecTest) { checkSingleKey("PERSIST", Persist.apply) }
  test("RENAME", CodecTest) { checkSingleKeySingleVal("RENAME", Rename.apply) }
  test("RENAMENX", CodecTest) { checkSingleKeySingleVal("RENAMENX", RenameNx.apply) }
  test("RANDOMKEY", CodecTest) { assert(encodeCommand(Randomkey) == Seq("RANDOMKEY")) }
  test("TTL", CodecTest) { checkSingleKey("TTL", Ttl.apply) }
  test("PTTL", CodecTest) { checkSingleKey("PTTL", PTtl.apply) }
  test("TYPE", CodecTest) { checkSingleKey("TYPE", Type.apply) }
  test("EXPIRE", CodecTest) { checkSingleKeyArbitraryVal("EXPIRE", Expire.apply) }
  test("EXPIREAT", CodecTest) {
    checkSingleKeyArbitraryVal("EXPIREAT", (k: Buf, v: Int) => ExpireAt(k, Time.fromSeconds(v)))
  }
  test("PEXPIRE", CodecTest) { checkSingleKeyArbitraryVal("PEXPIRE", PExpire.apply) }
  test("PEXPIREAT", CodecTest) {
    checkSingleKeyArbitraryVal(
      "PEXPIREAT",
      (k: Buf, v: Int) => PExpireAt(k, Time.fromMilliseconds(v.toLong))
    )
  }

  test("SCAN", CodecTest) {
    assert(encodeCommand(Scan(42, None, None)) == Seq("SCAN", "42"))
    assert(encodeCommand(Scan(42, Some(10L), None)) == Seq("SCAN", "42", "COUNT", "10"))
    assert(
      encodeCommand(Scan(42, None, Some(Buf.Utf8("foo")))) == Seq("SCAN", "42", "MATCH", "foo")
    )
    assert(
      encodeCommand(Scan(42, Some(10L), Some(Buf.Utf8("foo")))) ==
        Seq("SCAN", "42", "COUNT", "10", "MATCH", "foo")
    )
  }
}
