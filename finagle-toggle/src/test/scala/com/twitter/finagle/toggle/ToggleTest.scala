package com.twitter.finagle.toggle

import org.scalacheck.Gen
import org.scalatest.FunSuite
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

object ToggleGenerator {

  val Id: Gen[String] = for {
    prefix <- Gen.identifier
    suffix <- Gen.identifier
  } yield {
    prefix + "." + suffix
  }

}

class ToggleTest extends FunSuite with ScalaCheckDrivenPropertyChecks {

  private val IntGen =
    Gen.chooseNum(Int.MinValue, Int.MaxValue)

  private val on = Toggle.on[Int]("com.twitter.on")
  private val off = Toggle.off[Int]("com.twitter.off")

  def newToggle[T](
    id: String,
    pf: PartialFunction[T, Boolean],
    fraction: Double
  ): Toggle.Fractional[T] = new Toggle.Fractional[T](id) {
    override def toString: String = s"Toggle($id)"
    def isDefinedAt(x: T): Boolean = pf.isDefinedAt(x)
    def apply(v1: T): Boolean = pf(v1)
    def currentFraction: Double = fraction
  }

  test("True") {
    forAll(IntGen) { i =>
      assert(on.isDefinedAt(i))
      assert(on(i))
    }
  }

  test("False") {
    forAll(IntGen) { i =>
      assert(off.isDefinedAt(i))
      assert(!off(i))
    }
  }

  test("Undefined") {
    val toggle = Toggle.Undefined
    forAll(IntGen) { i =>
      assert(!toggle.isDefinedAt(i))
      intercept[UnsupportedOperationException] {
        assert(toggle(i))
      }
    }
  }

  test("orElse(Toggle) basics") {
    val trueOrFalse = on.orElse(off)
    val trueOrTrue = on.orElse(on)
    Seq(trueOrFalse, trueOrTrue).foreach { tog =>
      forAll(IntGen) { i =>
        assert(tog.isDefinedAt(i))
        assert(tog(i))
      }
    }

    val falseOrFalse = off.orElse(off)
    val falseOrTrue = off.orElse(on)
    Seq(falseOrFalse, falseOrTrue).foreach { tog =>
      forAll(IntGen) { i =>
        assert(falseOrFalse.isDefinedAt(i))
        assert(!falseOrFalse(i))
      }
    }
  }

  test("orElse(Toggle) uses the first Toggle.isDefinedAt") {
    val undef1 = Toggle.Undefined
    val undef2 = Toggle.Undefined
    val undef12 = undef1.orElse(undef2)
    forAll(IntGen) { i =>
      assert(!undef12.isDefinedAt(i))
    }

    val t = undef12.orElse(on)
    forAll(IntGen) { i =>
      assert(t.isDefinedAt(i))
      assert(t(i))
    }

    val f = undef12.orElse(off)
    forAll(IntGen) { i =>
      assert(f.isDefinedAt(i))
      assert(!f(i))
    }
  }

  test("Toggle.Metadata constructor checks id") {
    def assertNotAllowed(id: String): Unit = {
      intercept[IllegalArgumentException] {
        Toggle.Metadata(id, 0.0, None, "test")
      }
    }

    // invalid chars
    assertNotAllowed("!")
    assertNotAllowed(" ")
    assertNotAllowed(" com.toggle.a")

    // check ids are package like
    assertNotAllowed("")
    assertNotAllowed("t")
    assertNotAllowed("2c")
    assertNotAllowed("comtoggleA")
    assertNotAllowed(".com.toggle.A")

    // test boundaries are ok
    Toggle.Metadata("com.toggle.A", 0.0, None, "test")
    Toggle.Metadata("com.toggle.Z", 0.0, None, "test")
    Toggle.Metadata("com.toggle.a", 0.0, None, "test")
    Toggle.Metadata("com.toggle.z", 0.0, None, "test")
    Toggle.Metadata("com.toggle.0", 0.0, None, "test")
    Toggle.Metadata("com.toggle.9", 0.0, None, "test")
    Toggle.Metadata("com.toggle._", 0.0, None, "test")
    Toggle.Metadata("com.toggle..", 0.0, None, "test")
    Toggle.Metadata("com.toggle.-", 0.0, None, "test")

    forAll(ToggleGenerator.Id) { id =>
      Toggle.validateId(id)
    }
  }

  test("Toggle.Metadata constructor checks fraction") {
    intercept[IllegalArgumentException] {
      Toggle.Metadata("com.toggle.Test", -0.1, None, "test")
    }
    intercept[IllegalArgumentException] {
      Toggle.Metadata("com.toggle.Test", 1.1, None, "test")
    }

    // test boundaries are ok
    Toggle.Metadata("com.toggle.Test", 0.0, None, "test")
    Toggle.Metadata("com.toggle.Test", 1.0, None, "test")
  }

  test("Toggle.Metadata constructor checks description") {
    intercept[IllegalArgumentException] {
      Toggle.Metadata("com.toggle.Test", 0.0, Some(""), "test")
    }
    intercept[IllegalArgumentException] {
      Toggle.Metadata("com.toggle.Test", 0.0, Some("  "), "test")
    }

    Toggle.Metadata("com.toggle.Test", 0.0, Some("a description"), "test")
  }

  test("Toggle.Fractional.min throws exception if ids don't match") {
    intercept[IllegalArgumentException] {
      Toggle.Fractional.min(on, off)
    }
  }

  test("Toggle.Fractional.min isDefinedAt is result of isDefinedAt for toggle with lower fraction") {
    val definedAtZero = Toggle.on[Int]("com.foo")
    val undefinedAtZero = newToggle[Int]("com.foo", { case x if x > 0 => true }, 1.0)

    val toggle1 = Toggle.Fractional.min(definedAtZero, definedAtZero)
    assert(toggle1.isDefinedAt(0))

    val toggle2 = Toggle.Fractional.min(undefinedAtZero, definedAtZero)
    assert(!toggle2.isDefinedAt(0))

    val toggle3 = Toggle.Fractional.min(definedAtZero, undefinedAtZero)
    assert(toggle3.isDefinedAt(0))
  }

  test("Toggle.Fractional.min currentFraction is min of both toggles' currentFractions") {
    val a = newToggle[Int]("com.foo", { case _ => true }, 0.5)
    val b = newToggle[Int]("com.foo", { case _ => true }, 0.8)

    val toggle1 = Toggle.Fractional.min(a, b)
    assert(toggle1.currentFraction == 0.5)

    val toggle2 = Toggle.Fractional.min(b, a)
    assert(toggle2.currentFraction == 0.5)
  }

  test("Toggle.Fractional.min apply uses currentFraction") {
    val a = newToggle[Int]("com.foo", { case _ => false }, 0.0)
    val b = newToggle[Int]("com.foo", { case _ => true }, 1.0)

    val toggle1 = Toggle.Fractional.min(a, b)
    assert(toggle1(5) == false)

    val toggle2 = Toggle.Fractional.min(b, b)
    assert(toggle2(5) == true)
  }
}
