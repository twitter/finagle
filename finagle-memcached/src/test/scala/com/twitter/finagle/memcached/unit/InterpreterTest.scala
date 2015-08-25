package com.twitter.finagle.memcachedx.unit

import scala.collection.mutable

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import com.twitter.conversions.time._
import com.twitter.finagle.memcachedx.{Entry, Interpreter}
import com.twitter.finagle.memcachedx.protocol._
import com.twitter.finagle.memcachedx.util.AtomicMap
import com.twitter.io.Buf
import com.twitter.util.Time

@RunWith(classOf[JUnitRunner])
class InterpreterTest extends FunSuite {

  val map = mutable.Map[Buf, Entry]()
  val atomicMap = new AtomicMap(Seq(map))
  val interpreter = new Interpreter(atomicMap)
        
  test("correctly perform the GET & SET commands") {
    val key   = Buf.Utf8("foo")
    val value = Buf.Utf8("bar")
    interpreter(Delete(key))
    interpreter(Set(key, 0, Time.epoch, value))

    assert(interpreter(Get(Seq(key))) === Values(Seq(Value(key, value))))
  }

  test("correctly perform the QUIT command") {
    assert(interpreter(Quit()) === NoOp())
  }

  test("correctly perform the EXPIRY command") {
    val key = Buf.Utf8("key1")
    val noExpiry = Buf.Utf8("key2")
    val value = Buf.Utf8("value1")
    val now = Time.now

    Time.withTimeAt(now) { control =>
      interpreter(Set(key, 0, now + 10.seconds, value)) // set with an expiry...
        interpreter(Set(noExpiry, 0, Time.epoch, value)) // set without an expiry...
        atomicMap.lock(key) { data =>
          assert(data.contains(key) === true)
        }

      info("verify we can retrieve it up until the expiry")
      control.advance(9.seconds)
      assert(interpreter(Get(Seq(key))) === Values(Seq(Value(key, value))))
      assert(interpreter(Get(Seq(noExpiry))) === Values(Seq(Value(noExpiry, value))))

      info("verify it's not accessible after the expiry")
      control.advance(1.second)
      assert(interpreter(Get(Seq(key))) === Values(Seq()))

      info("and verify that the entry is cleaned up from the underlying map")
      atomicMap.lock(key) { data =>
        assert(data.contains(key) === false)
      }

      info("but the value without an expiry should still be accessible (even minutes later)")
      control.advance(1.hour)
      assert(interpreter(Get(Seq(noExpiry))) === Values(Seq(Value(noExpiry, value))))
    }
  }

}
