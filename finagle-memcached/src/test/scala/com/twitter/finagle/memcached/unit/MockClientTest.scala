package com.twitter.finagle.memcached.unit

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.memcached.MockClient.Cached
import com.twitter.finagle.memcached.protocol.ClientError
import com.twitter.finagle.memcached.{GetResult, MockClient}
import com.twitter.io.Buf
import com.twitter.util.{Await, Awaitable, Return, Time}
import org.scalatest.funsuite.AnyFunSuite

class MockClientTest extends AnyFunSuite {
  import MockClient.asValue

  val TimeOut = 15.seconds

  // http://t.co/momentofzen.jpg
  private[this] val zen: Time = Time.epoch + 1288834974657L.milliseconds // Twepoch

  private def run[T](awaitable: Awaitable[T]): T = Await.result(awaitable, TimeOut)

  test("correctly perform the GET command") {
    val memcache = MockClient.fromStrings("key" -> "value").withStrings

    assert(run(memcache.get("key")) == Some("value"))
    assert(run(memcache.get("unknown")).isEmpty)
  }

  test("getResult with expiry and flags") {
    Time.withTimeAt(zen) { tc =>
      val memcache = MockClient(
        "key" -> Cached(Buf.Utf8("value"), zen + 5.seconds, 42),
        "expired" -> Cached(Buf.Utf8("wat"), zen + 1.second, 42)
      ).withStrings

      val expected = GetResult(
        hits = Map(
          "key" -> asValue("key", "value", 42),
          "expired" -> asValue("expired", "wat", 42)
        )
      )

      assert(run(memcache.getResult(Seq("key", "expired"))) == expected)

      tc.advance(3.seconds)

      assert(
        run(memcache.getResult(Seq("key", "expired"))) == expected.copy(
          hits = expected.hits - "expired",
          misses = Set("expired")
        )
      )
    }
  }

  test("correctly perform the SET command") {
    val memcache = MockClient.fromStrings("key" -> "value").withStrings

    assert(run(memcache.set("key", "new value").liftToTry) == Return.Unit)
    assert(run(memcache.get("key")) == Some("new value"))

    assert(run(memcache.set("key2", "value2").liftToTry) == Return.Unit)
    assert(run(memcache.get("key2")) == Some("value2"))

    assert(run(memcache.set("key2", "value3").liftToTry) == Return.Unit)
    assert(run(memcache.get("key2")) == Some("value3"))

    Time.withTimeAt(zen) { tc =>
      assert(run(memcache.set("a", 0, zen + 1.second, "b").liftToTry) == Return.Unit)
      assert(run(memcache.get("a")) == Some("b"))
      tc.advance(2.seconds)
      assert(run(memcache.get("a")).isEmpty)
    }
  }

  test("correctly perform the ADD command") {
    val memcache = MockClient.fromStrings("key" -> "value").withStrings

    assert(!run(memcache.add("key", "new value")))
    assert(run(memcache.get("key")) == Some("value"))

    assert(run(memcache.add("key2", "value2")).booleanValue)
    assert(run(memcache.get("key2")) == Some("value2"))

    assert(!run(memcache.add("key2", "value3")))
    assert(run(memcache.get("key2")) == Some("value2"))

    Time.withTimeAt(zen) { tc =>
      val bool = run(memcache.add("foo", 0, zen + 1.second, "bar"))
      assert(bool)

      assert(!run(memcache.add("foo", 0, zen + 1.second, "baz")))

      tc.advance(2.seconds)

      // already expired entry should return True
      assert(run(memcache.add("foo", 0, zen + 1.second, "spam")).booleanValue())

      // we get nothing on retrieval though
      assert(run(memcache.get("foo")).isEmpty)

      tc.advance(1.second)

      assert(run(memcache.add("foo", 0, zen + 10.seconds, "spam")).booleanValue())
      assert(run(memcache.get("foo")).contains("spam"))
    }
  }

  test("correctly perform the APPEND command") {
    val memcache = MockClient(
      "key" -> Cached(Buf.Utf8("value")),
      "expired" -> Cached(Buf.Utf8("WRONG!"), zen),
      "k2" -> Cached(Buf.Utf8("v2"), zen + 20.seconds)
    ).withStrings

    assert(run(memcache.append("key", "More")).booleanValue)
    assert(run(memcache.get("key")) == Some("valueMore"))

    assert(!run(memcache.append("unknown", "value")))
    assert(run(memcache.get("unknown")).isEmpty)

    Time.withTimeAt(zen) { tc =>
      tc.advance(1.second)
      assert(!run(memcache.append("expired", 0, zen, "fuu")))

      tc.advance(2.seconds)
      assert(run(memcache.append("k2", 0, zen + 20.seconds, "AA")).booleanValue())

      assert(run(memcache.get("k2")).contains("v2AA"))
    }
  }

  test("correctly perform the PREPEND command") {
    val memcache = MockClient(
      "key" -> Cached(Buf.Utf8("value")),
      "expired" -> Cached(Buf.Utf8("WRONG!"), zen),
      "k2" -> Cached(Buf.Utf8("v2"), zen + 20.seconds)
    ).withStrings

    assert(run(memcache.prepend("key", "More")).booleanValue)
    assert(run(memcache.get("key")) == Some("Morevalue"))

    assert(!run(memcache.prepend("unknown", "value")))
    assert(run(memcache.get("unknown")).isEmpty)

    Time.withTimeAt(zen) { tc =>
      tc.advance(1.second)
      assert(!run(memcache.prepend("expired", 0, zen, "fuu")))

      tc.advance(2.seconds)
      assert(run(memcache.prepend("k2", 0, zen + 20.seconds, "AA")).booleanValue())

      assert(run(memcache.get("k2")).contains("AAv2"))
    }
  }

  test("correctly perform the REPLACE command") {
    val memcache = MockClient(
      "key" -> Cached(Buf.Utf8("value")),
      "expired" -> Cached(Buf.Utf8("WRONG!"), zen),
      "k2" -> Cached(Buf.Utf8("v2"), zen + 20.seconds)
    ).withStrings

    assert(run(memcache.replace("key", "new value")).booleanValue)
    assert(run(memcache.get("key")) == Some("new value"))

    assert(!run(memcache.replace("unknown", "value")))
    assert(run(memcache.get("unknown")).isEmpty)

    Time.withTimeAt(zen) { tc =>
      tc.advance(1.second)

      assert(!run(memcache.replace("expired", "NOPE")).booleanValue)

      assert(run(memcache.replace("k2", "2")).booleanValue)
      assert(run(memcache.get("k2")).contains("2"))
    }
  }

  test("correctly perform the DELETE command") {
    val memcache = MockClient(
      "key" -> Cached(Buf.Utf8("value")),
      "expired" -> Cached(Buf.Utf8("WRONG!"), zen),
      "k2" -> Cached(Buf.Utf8("v2"), zen + 20.seconds)
    ).withStrings

    assert(run(memcache.delete("key")).booleanValue)
    assert(run(memcache.get("key")).isEmpty)

    assert(!run(memcache.delete("unknown")))
    assert(run(memcache.get("unknown")).isEmpty)
    Time.withTimeAt(zen) { tc =>
      tc.advance(1.second)

      assert(!run(memcache.delete("expired")).booleanValue)
      assert(run(memcache.delete("k2")).booleanValue)
    }
  }

  test("correctly perform the INCR command") {
    val memcache = MockClient(
      "key" -> Cached(Buf.Utf8("value")),
      "count" -> Cached(Buf.Utf8("1")),
      "expired" -> Cached(Buf.Utf8("1"), zen),
      "count2" -> Cached(Buf.Utf8("1"), zen + 20.seconds)
    ).withStrings

    intercept[ClientError] { run(memcache.incr("key")) }

    assert(run(memcache.get("key")) == Some("value"))

    assert(run(memcache.incr("count")) == Some(2))
    assert(run(memcache.get("count")) == Some("2"))

    assert(run(memcache.incr("unknown")).isEmpty)
    assert(run(memcache.get("unknown")).isEmpty)

    Time.withTimeAt(zen) { tc =>
      tc.advance(1.second)

      assert(run(memcache.incr("expired")).isEmpty)
      assert(run(memcache.incr("count2")).contains(2))
    }
  }

  test("correctly perform the DECR command") {
    val memcache = MockClient(
      "key" -> Cached(Buf.Utf8("value")),
      "count" -> Cached(Buf.Utf8("1")),
      "expired" -> Cached(Buf.Utf8("1"), zen),
      "count2" -> Cached(Buf.Utf8("1"), zen + 20.seconds)
    ).withStrings

    intercept[ClientError] { run(memcache.decr("key")) }

    assert(run(memcache.get("key")) == Some("value"))

    assert(run(memcache.decr("count")) == Some(0))
    assert(run(memcache.get("count")) == Some("0"))
    assert(run(memcache.decr("count")) == Some(0))
    assert(run(memcache.get("count")) == Some("0"))

    assert(run(memcache.decr("unknown")).isEmpty)
    assert(run(memcache.get("unknown")).isEmpty)

    Time.withTimeAt(zen) { tc =>
      tc.advance(1.second)

      assert(run(memcache.incr("expired")).isEmpty)
      assert(run(memcache.decr("count2")).contains(0))
    }
  }

  test("`getResults` command populates the `casUnique` value") {
    val memcache = MockClient.fromStrings("key" -> "value", "count" -> "1").withStrings

    val result = run(memcache.getResult(Seq("key")))

    assert(result.hits("key").casUnique.isDefined)
  }

  test("`contents` produces immutable copies") {
    val memcache = MockClient()
    val emptyContents = memcache.contents

    memcache.withStrings.set("key", "value")
    val oneKey = memcache.contents

    // ensure that contents of emptyContents has not changed: check it after set
    assert(emptyContents.isEmpty)
    assert(oneKey == Map("key" -> Buf.Utf8("value")))
  }
}
