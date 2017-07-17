package com.twitter.finagle.memcached.unit

import com.twitter.conversions.time._
import com.twitter.finagle.memcached.MockClient
import com.twitter.finagle.memcached.protocol.ClientError
import com.twitter.io.Buf
import com.twitter.util.{Await, Awaitable, Return}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MockClientTest extends FunSuite {

  val TimeOut = 15.seconds

  private def awaitResult[T](awaitable: Awaitable[T]): T = Await.result(awaitable, TimeOut)

  test("correctly perform the GET command") {
    val memcache = new MockClient(Map("key" -> "value")).withStrings

    assert(awaitResult(memcache.get("key")) == Some("value"))
    assert(awaitResult(memcache.get("unknown")) == None)
  }

  test("correctly perform the SET command") {
    val memcache = new MockClient(Map("key" -> "value")).withStrings

    assert(awaitResult(memcache.set("key", "new value").liftToTry) == Return.Unit)
    assert(awaitResult(memcache.get("key")) == Some("new value"))

    assert(awaitResult(memcache.set("key2", "value2").liftToTry) == Return.Unit)
    assert(awaitResult(memcache.get("key2")) == Some("value2"))

    assert(awaitResult(memcache.set("key2", "value3").liftToTry) == Return.Unit)
    assert(awaitResult(memcache.get("key2")) == Some("value3"))
  }

  test("correctly perform the ADD command") {
    val memcache = new MockClient(Map("key" -> "value")).withStrings

    assert(!awaitResult(memcache.add("key", "new value")))
    assert(awaitResult(memcache.get("key")) == Some("value"))

    assert(awaitResult(memcache.add("key2", "value2")).booleanValue)
    assert(awaitResult(memcache.get("key2")) == Some("value2"))

    assert(!awaitResult(memcache.add("key2", "value3")))
    assert(awaitResult(memcache.get("key2")) == Some("value2"))
  }

  test("correctly perform the APPEND command") {
    val memcache = new MockClient(Map("key" -> "value")).withStrings

    assert(awaitResult(memcache.append("key", "More")).booleanValue)
    assert(awaitResult(memcache.get("key")) == Some("valueMore"))

    assert(!awaitResult(memcache.append("unknown", "value")))
    assert(awaitResult(memcache.get("unknown")) == None)
  }

  test("correctly perform the PREPEND command") {
    val memcache = new MockClient(Map("key" -> "value")).withStrings

    assert(awaitResult(memcache.prepend("key", "More")).booleanValue)
    assert(awaitResult(memcache.get("key")) == Some("Morevalue"))

    assert(!awaitResult(memcache.prepend("unknown", "value")))
    assert(awaitResult(memcache.get("unknown")) == None)
  }

  test("correctly perform the REPLACE command") {
    val memcache = new MockClient(Map("key" -> "value")).withStrings

    assert(awaitResult(memcache.replace("key", "new value")).booleanValue)
    assert(awaitResult(memcache.get("key")) == Some("new value"))

    assert(!awaitResult(memcache.replace("unknown", "value")))
    assert(awaitResult(memcache.get("unknown")) == None)
  }

  test("correctly perform the DELETE command") {
    val memcache = new MockClient(Map("key" -> "value")).withStrings

    assert(awaitResult(memcache.delete("key")).booleanValue)
    assert(awaitResult(memcache.get("key")) == None)

    assert(!awaitResult(memcache.delete("unknown")))
    assert(awaitResult(memcache.get("unknown")) == None)
  }

  test("correctly perform the INCR command") {
    val memcache = new MockClient(Map("key" -> "value", "count" -> "1")).withStrings

    intercept[ClientError] { awaitResult(memcache.incr("key")) }

    assert(awaitResult(memcache.get("key")) == Some("value"))

    assert(awaitResult(memcache.incr("count")) == Some(2))
    assert(awaitResult(memcache.get("count")) == Some("2"))

    assert(awaitResult(memcache.incr("unknown")) == None)
    assert(awaitResult(memcache.get("unknown")) == None)
  }

  test("correctly perform the DECR command") {
    val memcache = new MockClient(Map("key" -> "value", "count" -> "1")).withStrings

    intercept[ClientError] { awaitResult(memcache.decr("key")) }

    assert(awaitResult(memcache.get("key")) == Some("value"))

    assert(awaitResult(memcache.decr("count")) == Some(0))
    assert(awaitResult(memcache.get("count")) == Some("0"))
    assert(awaitResult(memcache.decr("count")) == Some(0))
    assert(awaitResult(memcache.get("count")) == Some("0"))

    assert(awaitResult(memcache.decr("unknown")) == None)
    assert(awaitResult(memcache.get("unknown")) == None)
  }

  test("`getResults` command populates the `casUnique` value") {
    val memcache = new MockClient(Map("key" -> "value", "count" -> "1")).withStrings

    val result = awaitResult(memcache.getResult(Seq("key")))

    assert(result.hits("key").casUnique.isDefined)
  }

  test("`contents` produces immutable copies") {
    val memcache = new MockClient()
    val emptyContents = memcache.contents

    memcache.withStrings.set("key", "value")
    val oneKey = memcache.contents

    // ensure that contents of emptyContents has not changed: check it after set
    assert(emptyContents == Map())
    assert(oneKey == Map("key" -> Buf.Utf8("value")))
  }
}
