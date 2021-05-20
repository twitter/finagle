package com.twitter.finagle.util

import org.scalatest.funsuite.AnyFunSuite

class CachedHashCodeTest extends AnyFunSuite {

  private class HashTo(hc: Int) extends CachedHashCode.ForClass {
    override protected def computeHashCode: Int = hc

    // note: we leave equals as is, so that it is based on reference equality.
  }

  private case class CachedCaseClass(i: Int) extends CachedHashCode.ForCaseClass

  test("hashCode") {
    val h1 = new HashTo(1)
    assert(1 == h1.hashCode)

    val h2 = new HashTo(2)
    assert(2 == h2.hashCode)
  }

  test("hashCode to the sentinel value") {
    val h0 = new HashTo(CachedHashCode.NotComputed)
    assert(CachedHashCode.ComputedCollision == h0.hashCode)
  }

  test("used as keys in a Map") {
    val h0a = new HashTo(0)
    val h0b = new HashTo(0)
    val h1c = new HashTo(1)
    val h1d = new HashTo(1)
    val map = Map(h0a -> "a", h0b -> "b", h1c -> "c", h1d -> "d")

    assert("a" == map(h0a))
    assert("b" == map(h0b))
    assert("c" == map(h1c))
    assert("d" == map(h1d))

    assert(map.get(new HashTo(1)).isEmpty)
  }

  test("mixed into case classes") {
    val a = CachedCaseClass(0)
    val b = CachedCaseClass(0)
    assert(a == b)
    assert(a.hashCode == b.hashCode)
  }

  test("same hashCode mixed in or not") {
    //case class productPrefix (class name) gets mixed in
    //into hash in 2.13+, so we have to override
    //it if we want to check whether two separate
    //case classes would have hashed the same if
    //the only difference had been the mixin.
    case class NotMixedIn(s: String) {
      override def productPrefix = "prefix"
    }
    case class MixedIn(s: String) extends CachedHashCode.ForCaseClass {
      override def productPrefix = "prefix"
    }

    val nmi = NotMixedIn("hello")
    val mi = MixedIn("hello")

    assert(nmi.hashCode == mi.hashCode)
  }

}
