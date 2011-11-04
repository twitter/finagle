package com.twitter.finagle.memcached.unit

import com.twitter.finagle.memcached.MockClient
import org.specs.Specification
import scala.collection.mutable

object MockClientSpec extends Specification {
  "MockClient" should {
    "get" in {
      val memcache = new MockClient(Map("key" -> "value")).withStrings
      memcache.get("key")()                  must beSome("value")
      memcache.get("unknown")()              must be_==(None)
    }

    "set" in {
      val memcache = new MockClient(Map("key" -> "value")).withStrings

      memcache.set("key", "new value")()     must be_==(())
      memcache.get("key")()                  must beSome("new value")

      memcache.set("key2", "value2")()       must be_==(())
      memcache.get("key2")()                 must beSome("value2")
      memcache.set("key2", "value3")()       must be_==(())
      memcache.get("key2")()                 must beSome("value3")
    }

    "add" in {
      val memcache = new MockClient(Map("key" -> "value")).withStrings

      memcache.add("key", "new value")()     must beFalse
      memcache.get("key")()                  must beSome("value")

      memcache.add("key2", "value2")()       must beTrue
      memcache.get("key2")()                 must beSome("value2")
      memcache.add("key2", "value3")()       must beFalse
      memcache.get("key2")()                 must beSome("value2")
    }

    "append" in {
      val memcache = new MockClient(Map("key" -> "value")).withStrings

      memcache.append("key", "More")()       must beTrue
      memcache.get("key")()                  must beSome("valueMore")

      memcache.append("unknown", "value")()  must beFalse
      memcache.get("unknown")()              must beNone
    }

    "prepend" in {
      val memcache = new MockClient(Map("key" -> "value")).withStrings

      memcache.prepend("key", "More")()      must beTrue
      memcache.get("key")()                  must beSome("Morevalue")

      memcache.prepend("unknown", "value")() must beFalse
      memcache.get("unknown")()              must beNone
    }

    "replace" in {
      val memcache = new MockClient(Map("key" -> "value")).withStrings

      memcache.replace("key", "new value")() must beTrue
      memcache.get("key")()                  must beSome("new value")

      memcache.replace("unknown", "value")() must beFalse
      memcache.get("unknown")()              must beNone
    }

    "delete" in {
      val memcache = new MockClient(Map("key" -> "value")).withStrings

      memcache.delete("key")()               must beTrue
      memcache.get("key")()                  must beNone

      memcache.delete("unknown")()           must beFalse
      memcache.get("unknown")()              must beNone
    }

    "incr" in {
      val memcache = new MockClient(Map("key" -> "value", "count" -> "1")).withStrings

      memcache.incr("key")()                 must beNone
      memcache.get("key")()                  must beSome("value")

      memcache.incr("count")()               must beSome(2)
      memcache.get("count")()                must beSome("2")

      memcache.incr("unknown")()             must beNone
      memcache.get("unknown")()              must beNone
    }

    "decr" in {
      val memcache = new MockClient(Map("key" -> "value", "count" -> "1")).withStrings

      memcache.decr("key")()                 must beNone
      memcache.get("key")()                  must beSome("value")

      memcache.decr("count")()               must beSome(0)
      memcache.get("count")()                must beSome("0")
      memcache.decr("count")()               must beSome(0)
      memcache.get("count")()                must beSome("0")

      memcache.decr("unknown")()             must beNone
      memcache.get("unknown")()              must beNone
    }
  }
}


