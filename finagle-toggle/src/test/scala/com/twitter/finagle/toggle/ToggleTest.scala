package com.twitter.finagle.toggle

import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.funsuite.AnyFunSuite

object ToggleGenerator {

  val Id: Gen[String] = for {
    prefix <- Gen.identifier
    suffix <- Gen.identifier
  } yield {
    prefix + "." + suffix
  }

}

class ToggleTest extends AnyFunSuite with ScalaCheckDrivenPropertyChecks {

  private val IntGen =
    Gen.chooseNum(Int.MinValue, Int.MaxValue)

  private val on = Toggle.on("com.twitter.on")
  private val off = Toggle.off("com.twitter.off")

  def newToggle[T](
    id: String,
    pf: PartialFunction[Int, Boolean],
    fraction: Double,
    defined: Boolean
  ): Toggle.Fractional = new Toggle.Fractional(id) {
    override def toString: String = s"Toggle($id)"
    def isDefined: Boolean = defined
    def apply(v1: Int): Boolean = pf(v1)
    def currentFraction: Double = fraction
  }

  test("True") {
    forAll(IntGen) { i =>
      assert(on.isDefined)
      assert(on(i))
    }
  }

  test("False") {
    forAll(IntGen) { i =>
      assert(off.isDefined)
      assert(!off(i))
    }
  }

  test("Undefined") {
    val toggle = Toggle.Undefined
    forAll(IntGen) { i =>
      assert(toggle.isUndefined)
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
        assert(tog.isDefined)
        assert(tog(i))
      }
    }

    val falseOrFalse = off.orElse(off)
    val falseOrTrue = off.orElse(on)
    Seq(falseOrFalse, falseOrTrue).foreach { tog =>
      forAll(IntGen) { i =>
        assert(falseOrFalse.isDefined)
        assert(!falseOrFalse(i))
      }
    }
  }

  test("orElse(Toggle) uses the first Toggle.isDefinedAt") {
    val undef1 = Toggle.Undefined
    val undef2 = Toggle.Undefined
    val undef12 = undef1.orElse(undef2)
    assert(undef12.isUndefined)

    val t = undef12.orElse(on)
    forAll(IntGen) { i =>
      assert(t.isDefined)
      assert(t(i))
    }

    val f = undef12.orElse(off)
    forAll(IntGen) { i =>
      assert(f.isDefined)
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

    forAll(ToggleGenerator.Id) { id => Toggle.validateId(id) }
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

  test(
    "Toggle.Fractional.min isDefinedAt is result of isDefinedAt for toggle with lower fraction") {
    val definedAtZero = Toggle.on("com.foo")
    val undefinedAtZero = newToggle("com.foo", { case x if x > 0 => true }, 1.0, false)

    val toggle1 = Toggle.Fractional.min(definedAtZero, definedAtZero)
    assert(toggle1.isDefined)

    val toggle2 = Toggle.Fractional.min(undefinedAtZero, definedAtZero)
    assert(toggle2.isUndefined)

    val toggle3 = Toggle.Fractional.min(definedAtZero, undefinedAtZero)
    assert(toggle3.isDefined)
  }

  test("Toggle.Fractional.min currentFraction is min of both toggles' currentFractions") {
    val a = newToggle("com.foo", { case _ => true }, 0.5, defined = true)
    val b = newToggle("com.foo", { case _ => true }, 0.8, defined = true)

    val toggle1 = Toggle.Fractional.min(a, b)
    assert(toggle1.currentFraction == 0.5)

    val toggle2 = Toggle.Fractional.min(b, a)
    assert(toggle2.currentFraction == 0.5)
  }

  test("Toggle.Fractional.min apply uses currentFraction") {
    val a = newToggle("com.foo", { case _ => false }, 0.0, defined = true)
    val b = newToggle("com.foo", { case _ => true }, 1.0, defined = true)

    val toggle1 = Toggle.Fractional.min(a, b)
    assert(toggle1(5) == false)

    val toggle2 = Toggle.Fractional.min(b, b)
    assert(toggle2(5) == true)
  }
}
