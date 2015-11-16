package com.twitter.finagle.memcached.unit

import com.twitter.finagle.memcached.MockClient
import com.twitter.finagle.memcached.protocol.ClientError
import com.twitter.util.Await
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MockClientTest extends FunSuite {

  test("correctly perform the GET command") {
    val memcache = new MockClient(Map("key" -> "value")).withStrings

    assert(Await.result(memcache.get("key")) == Some("value"))
    assert(Await.result(memcache.get("unknown")) == None)
  }

  test("correctly perform the SET command") {
    val memcache = new MockClient(Map("key" -> "value")).withStrings

    assert(Await.result(memcache.set("key", "new value")) == (()))
    assert(Await.result(memcache.get("key")) == Some("new value"))

    assert(Await.result(memcache.set("key2", "value2")) == (()))
    assert(Await.result(memcache.get("key2")) == Some("value2"))

    assert(Await.result(memcache.set("key2", "value3")) == (()))
    assert(Await.result(memcache.get("key2")) == Some("value3"))
  }

  test("correctly perform the ADD command") {
    val memcache = new MockClient(Map("key" -> "value")).withStrings

    assert(!Await.result(memcache.add("key", "new value")))
    assert(Await.result(memcache.get("key")) == Some("value"))

    assert(Await.result(memcache.add("key2", "value2")))
    assert(Await.result(memcache.get("key2")) == Some("value2"))

    assert(!Await.result(memcache.add("key2", "value3")))
    assert(Await.result(memcache.get("key2")) == Some("value2"))
  }

  test("correctly perform the APPEND command") {
    val memcache = new MockClient(Map("key" -> "value")).withStrings

    assert(Await.result(memcache.append("key", "More")))
    assert(Await.result(memcache.get("key")) == Some("valueMore"))

    assert(!Await.result(memcache.append("unknown", "value")))
    assert(Await.result(memcache.get("unknown")) == None)
  }

  test("correctly perform the PREPEND command") {
    val memcache = new MockClient(Map("key" -> "value")).withStrings

    assert(Await.result(memcache.prepend("key", "More")))
    assert(Await.result(memcache.get("key")) == Some("Morevalue"))

    assert(!Await.result(memcache.prepend("unknown", "value")))
    assert(Await.result(memcache.get("unknown")) == None)
  }

  test("correctly perform the REPLACE command") {
    val memcache = new MockClient(Map("key" -> "value")).withStrings

    assert(Await.result(memcache.replace("key", "new value")))
    assert(Await.result(memcache.get("key")) == Some("new value"))

    assert(!Await.result(memcache.replace("unknown", "value")))
    assert(Await.result(memcache.get("unknown")) == None)
  }

  test("correctly perform the DELETE command") {
    val memcache = new MockClient(Map("key" -> "value")).withStrings

    assert(Await.result(memcache.delete("key")))
    assert(Await.result(memcache.get("key")) == None)

    assert(!Await.result(memcache.delete("unknown")))
    assert(Await.result(memcache.get("unknown")) == None)
  }

  test("correctly perform the INCR command") {
    val memcache = new MockClient(Map("key" -> "value", "count" -> "1")).withStrings

    intercept[ClientError] { Await.result(memcache.incr("key")) }

    assert(Await.result(memcache.get("key")) == Some("value"))

    assert(Await.result(memcache.incr("count")) == Some(2))
    assert(Await.result(memcache.get("count")) == Some("2"))

    assert(Await.result(memcache.incr("unknown")) == None)
    assert(Await.result(memcache.get("unknown")) == None)
  }

  test("correctly perform the DECR command") {
    val memcache = new MockClient(Map("key" -> "value", "count" -> "1")).withStrings

    intercept[ClientError] { Await.result(memcache.decr("key")) }

    assert(Await.result(memcache.get("key")) == Some("value"))

    assert(Await.result(memcache.decr("count")) == Some(0))
    assert(Await.result(memcache.get("count")) == Some("0"))
    assert(Await.result(memcache.decr("count")) == Some(0))
    assert(Await.result(memcache.get("count")) == Some("0"))

    assert(Await.result(memcache.decr("unknown")) == None)
    assert(Await.result(memcache.get("unknown")) == None)
  }

  test("`getResults` command populates the `casUnique` value") {
    val memcache = new MockClient(Map("key" -> "value", "count" -> "1")).withStrings

    val result = Await.result(memcache.getResult(Seq("key")))

    assert(result.hits("key").casUnique.isDefined)
  }
}
