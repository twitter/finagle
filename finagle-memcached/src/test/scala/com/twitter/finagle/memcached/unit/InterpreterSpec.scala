package com.twitter.finagle.memcached.unit

import com.twitter.conversions.time._
import com.twitter.finagle.memcached.{Entry, Interpreter}
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.util.AtomicMap
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.util.Time
import org.jboss.netty.buffer.ChannelBuffer
import org.specs.SpecificationWithJUnit
import scala.collection.mutable

class InterpreterSpec extends SpecificationWithJUnit {
  "Interpreter" should {
    val map = mutable.Map[ChannelBuffer, Entry]()
    val atomicMap = new AtomicMap(Seq(map))
    val interpreter = new Interpreter(atomicMap)

    "set & get" in {
      val key   = "foo"
      val value = "bar"
      interpreter(Delete(key))
      interpreter(Set(key, 0, Time.epoch, value))
      interpreter(Get(Seq(key))) mustEqual Values(Seq(Value(key, value)))
    }

    "quit" in {
      interpreter(Quit()) mustEqual NoOp()
    }

    "expiry" in {
      val key = "key1"
      val noExpiry = "key2"
      val value = "value1"
      val now = Time.now
      Time.withTimeAt(now) { control =>
        interpreter(Set(key, 0, now + 10.seconds, value)) // set with an expiry...
        interpreter(Set(noExpiry, 0, Time.epoch, value)) // set without an expiry...
        atomicMap.lock(key) { data =>
          data.contains(key) must beTrue
        }

        // verify we can retrieve it up until the expiry
        control.advance(9.seconds)
        interpreter(Get(Seq(key))) mustEqual Values(Seq(Value(key, value)))
        interpreter(Get(Seq(noExpiry))) mustEqual Values(Seq(Value(noExpiry, value)))

        // verify it's not accessible after the expiry
        control.advance(1.second)
        interpreter(Get(Seq(key))) mustEqual Values(Seq())

        // and verify that the entry is cleaned up from the underlying map
        atomicMap.lock(key) { data =>
          data.contains(key) must beFalse
        }

        // but the value without an expiry should still be accessible (even minutes later)
        control.advance(1.hour)
        interpreter(Get(Seq(noExpiry))) mustEqual Values(Seq(Value(noExpiry, value)))
      }
    }

  }
}
