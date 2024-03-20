package com.twitter.finagle.memcached.unit

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.util.AtomicMap
import com.twitter.finagle.memcached.Entry
import com.twitter.finagle.memcached.Interpreter
import com.twitter.io.Buf
import com.twitter.util.Time
import scala.collection.mutable
import org.scalatest.funsuite.AnyFunSuite

class InterpreterTest extends AnyFunSuite {

  val map = mutable.Map[Buf, Entry]()
  val atomicMap = new AtomicMap(Seq(map))
  val interpreter = new Interpreter(atomicMap)
  val emptyFlags = Some(Buf.Utf8("0"))

  test("correctly perform the GET & SET commands") {
    val key = Buf.Utf8("foo")
    val value = Buf.Utf8("bar")
    interpreter(Delete(key))
    interpreter(Set(key, 0, Time.epoch, value))

    assert(interpreter(Get(Seq(key))) == Values(Seq(Value(key, value, None, emptyFlags))))
  }

  test("correctly perform the GETS & CAS commands") {
    val key = Buf.Utf8("key")
    val value1 = Buf.Utf8("value1")
    val value2 = Buf.Utf8("value2")
    val value3 = Buf.Utf8("value3")
    interpreter(Set(key, 0, Time.epoch, value1))
    assert(interpreter(Get(Seq(key))) == Values(Seq(Value(key, value1, None, emptyFlags))))
    val hashValue1 = interpreter(Gets(Seq(key)))
      .asInstanceOf[Values]
      .values
      .last
      .casUnique
    assert(interpreter(Gets(Seq(key))) == Values(Seq(Value(key, value1, hashValue1, emptyFlags))))

    assert(interpreter(Cas(key, 0, Time.epoch, value2, hashValue1.get)) == Stored)
    assert(interpreter(Cas(key, 0, Time.epoch, value3, hashValue1.get)) == NotStored)
  }

  test("correctly perform the QUIT command") {
    assert(interpreter(Quit()) == NoOp)
  }

  test("correctly perform the EXPIRY command") {
    val key = Buf.Utf8("key1")
    val noExpiry = Buf.Utf8("key2")
    val value = Buf.Utf8("value1")
    val now = Time.now

    Time.withTimeAt(now) { control =>
      interpreter(Set(key, 0, now + 10.seconds, value)) // set with an expiry...
      interpreter(Set(noExpiry, 0, Time.epoch, value)) // set without an expiry...
      atomicMap.lock(key) { data => assert(data.contains(key) == true) }

      info("verify we can retrieve it up until the expiry")
      control.advance(9.seconds)
      assert(interpreter(Get(Seq(key))) == Values(Seq(Value(key, value, None, emptyFlags))))
      assert(
        interpreter(Get(Seq(noExpiry))) == Values(Seq(Value(noExpiry, value, None, emptyFlags))))

      info("verify it's not accessible after the expiry")
      control.advance(1.second)
      assert(interpreter(Get(Seq(key))) == Values(Seq()))

      info("and verify that the entry is cleaned up from the underlying map")
      atomicMap.lock(key) { data => assert(data.contains(key) == false) }

      info("but the value without an expiry should still be accessible (even minutes later)")
      control.advance(1.hour)
      assert(
        interpreter(Get(Seq(noExpiry))) == Values(Seq(Value(noExpiry, value, None, emptyFlags))))
    }
  }
}
