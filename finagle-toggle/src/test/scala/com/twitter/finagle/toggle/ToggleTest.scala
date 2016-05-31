package com.twitter.finagle.toggle

import org.junit.runner.RunWith
import org.scalacheck.Gen
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.GeneratorDrivenPropertyChecks

object ToggleGenerator {

  val Id: Gen[String] = for {
    prefix <- Gen.identifier
    suffix <- Gen.identifier
  } yield {
    prefix + "." + suffix
  }

}

@RunWith(classOf[JUnitRunner])
class ToggleTest extends FunSuite
  with GeneratorDrivenPropertyChecks {

  private val IntGen =
    Gen.chooseNum(Int.MinValue, Int.MaxValue)

  test("True") {
    forAll(IntGen) { i =>
      assert(Toggle.True.isDefinedAt(i))
      assert(Toggle.True(i))
    }
  }

  test("False") {
    forAll(IntGen) { i =>
      assert(Toggle.False.isDefinedAt(i))
      assert(!Toggle.False(i))
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
    val trueOrFalse = Toggle.True.orElse(Toggle.False)
    val trueOrTrue = Toggle.True.orElse(Toggle.True)
    Seq(trueOrFalse, trueOrTrue).foreach { tog =>
      forAll(IntGen) { i =>
        assert(tog.isDefinedAt(i))
        assert(tog(i))
      }
    }

    val falseOrFalse = Toggle.False.orElse(Toggle.False)
    val falseOrTrue = Toggle.False.orElse(Toggle.True)
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

    val t = undef12.orElse(Toggle.True)
    forAll(IntGen) { i =>
      assert(t.isDefinedAt(i))
      assert(t(i))
    }

    val f = undef12.orElse(Toggle.False)
    forAll(IntGen) { i =>
      assert(f.isDefinedAt(i))
      assert(!f(i))
    }
  }

  test("Toggle.Metadata constructor checks id") {
    def assertNotAllowed(id: String): Unit = {
      intercept[IllegalArgumentException] {
        Toggle.Metadata(id, 0.0, None)
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
    Toggle.Metadata("com.toggle.A", 0.0, None)
    Toggle.Metadata("com.toggle.Z", 0.0, None)
    Toggle.Metadata("com.toggle.a", 0.0, None)
    Toggle.Metadata("com.toggle.z", 0.0, None)
    Toggle.Metadata("com.toggle.0", 0.0, None)
    Toggle.Metadata("com.toggle.9", 0.0, None)
    Toggle.Metadata("com.toggle._", 0.0, None)
    Toggle.Metadata("com.toggle..", 0.0, None)
    Toggle.Metadata("com.toggle.-", 0.0, None)

    forAll(ToggleGenerator.Id) { id =>
      Toggle.validateId(id)
    }
  }

  test("Toggle.Metadata constructor checks fraction") {
    intercept[IllegalArgumentException] {
      Toggle.Metadata("com.toggle.Test", -0.1, None)
    }
    intercept[IllegalArgumentException] {
      Toggle.Metadata("com.toggle.Test", 1.1, None)
    }

    // test boundaries are ok
    Toggle.Metadata("com.toggle.Test", 0.0, None)
    Toggle.Metadata("com.toggle.Test", 1.0, None)
  }

}
