package com.twitter.finagle.memcached.unit

import com.twitter.finagle.memcached.MockClient
import com.twitter.finagle.memcached.protocol.ClientError
import com.twitter.util.Await
import org.specs.SpecificationWithJUnit

class MockClientSpec extends SpecificationWithJUnit {
  "MockClient" should {
    "get" in {
      val memcache = new MockClient(Map("key" -> "value")).withStrings
      Await.result(memcache.get("key"))                  must beSome("value")
      Await.result(memcache.get("unknown"))              must be_==(None)
    }

    "set" in {
      val memcache = new MockClient(Map("key" -> "value")).withStrings

      Await.result(memcache.set("key", "new value"))     must be_==(())
      Await.result(memcache.get("key"))                  must beSome("new value")

      Await.result(memcache.set("key2", "value2"))       must be_==(())
      Await.result(memcache.get("key2"))                 must beSome("value2")
      Await.result(memcache.set("key2", "value3"))       must be_==(())
      Await.result(memcache.get("key2"))                 must beSome("value3")
    }

    "add" in {
      val memcache = new MockClient(Map("key" -> "value")).withStrings

      Await.result(memcache.add("key", "new value"))     must beFalse
      Await.result(memcache.get("key"))                  must beSome("value")

      Await.result(memcache.add("key2", "value2"))       must beTrue
      Await.result(memcache.get("key2"))                 must beSome("value2")
      Await.result(memcache.add("key2", "value3"))       must beFalse
      Await.result(memcache.get("key2"))                 must beSome("value2")
    }

    "append" in {
      val memcache = new MockClient(Map("key" -> "value")).withStrings

      Await.result(memcache.append("key", "More"))       must beTrue
      Await.result(memcache.get("key"))                  must beSome("valueMore")

      Await.result(memcache.append("unknown", "value"))  must beFalse
      Await.result(memcache.get("unknown"))              must beNone
    }

    "prepend" in {
      val memcache = new MockClient(Map("key" -> "value")).withStrings

      Await.result(memcache.prepend("key", "More"))      must beTrue
      Await.result(memcache.get("key"))                  must beSome("Morevalue")

      Await.result(memcache.prepend("unknown", "value")) must beFalse
      Await.result(memcache.get("unknown"))              must beNone
    }

    "replace" in {
      val memcache = new MockClient(Map("key" -> "value")).withStrings

      Await.result(memcache.replace("key", "new value")) must beTrue
      Await.result(memcache.get("key"))                  must beSome("new value")

      Await.result(memcache.replace("unknown", "value")) must beFalse
      Await.result(memcache.get("unknown"))              must beNone
    }

    "delete" in {
      val memcache = new MockClient(Map("key" -> "value")).withStrings

      Await.result(memcache.delete("key"))               must beTrue
      Await.result(memcache.get("key"))                  must beNone

      Await.result(memcache.delete("unknown"))           must beFalse
      Await.result(memcache.get("unknown"))              must beNone
    }

    "incr" in {
      val memcache = new MockClient(Map("key" -> "value", "count" -> "1")).withStrings

      Await.result(memcache.incr("key"))                 must throwA[ClientError]
      Await.result(memcache.get("key"))                  must beSome("value")

      Await.result(memcache.incr("count"))               must beSome(2)
      Await.result(memcache.get("count"))                must beSome("2")

      Await.result(memcache.incr("unknown"))             must beNone
      Await.result(memcache.get("unknown"))              must beNone
    }

    "decr" in {
      val memcache = new MockClient(Map("key" -> "value", "count" -> "1")).withStrings

      Await.result(memcache.decr("key"))                 must throwA[ClientError]
      Await.result(memcache.get("key"))                  must beSome("value")

      Await.result(memcache.decr("count"))               must beSome(0)
      Await.result(memcache.get("count"))                must beSome("0")
      Await.result(memcache.decr("count"))               must beSome(0)
      Await.result(memcache.get("count"))                must beSome("0")

      Await.result(memcache.decr("unknown"))             must beNone
      Await.result(memcache.get("unknown"))              must beNone
    }
  }
}


