package com.twitter.finagle.toggle

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StandardToggleMapTest extends FunSuite {

  test("apply with a known libraryName") {
    flag.overrides.let(Map.empty) {
      // should load `ServiceLoadedToggleTestA`
      val tm = StandardToggleMap("A")
      val togs = tm.iterator.toSeq
      assert(togs.size == 1)
      assert(togs.head.id == "com.toggle.a")
    }
  }

  test("apply with an unknown libraryName") {
    flag.overrides.let(Map.empty) {
      val tm = StandardToggleMap("ZZZ")
      assert(tm.iterator.isEmpty)
      val toggle = tm("com.toggle.XYZ")
      assert(!toggle.isDefinedAt(245))
      intercept[UnsupportedOperationException] {
        toggle(245)
      }
    }
  }

  test("apply with a duplicate libraryName") {
    intercept[IllegalStateException] {
      StandardToggleMap("B")
    }
  }

  test("Toggles use correct ordering") {
    // we want to see what the fractions are when the toggle
    // exists in multiple places.
    // we'll use "a", which is service loaded to use 1.0
    // we can test what happens by modifying that flag and in-memory toggle

    def assertFraction(togMap: ToggleMap, fraction: Double): Unit = {
      val togs = togMap.iterator.toSeq
      assert(togs.size == 1)
      assert(togs.head.id == "com.toggle.a")
      assert(togs.head.fraction == fraction)
    }

    val inMem = ToggleMap.newMutable()
    // should load `ServiceLoadedToggleTestA`
    val togMap = StandardToggleMap("A", inMem)
    flag.overrides.letClear("com.toggle.a") {
      // start without the flag or in-memory, and only the service loaded
      assertFraction(togMap, 1.0)

      // now set the flag, and verify we pick that up
      flag.overrides.let("com.toggle.a", 0.5) {
        assertFraction(togMap, 0.5)

        // now set the in-memory version, verify we use that
        inMem.put("com.toggle.a", 0.3)
        assertFraction(togMap, 0.3)

        // remove the in-memory value and stop using it
        inMem.remove("com.toggle.a")
        assertFraction(togMap, 0.5)
      }

      // now we are back outside of the flag being set,
      // verify its still using the service loaded setting
      assertFraction(togMap, 1.0)

      // change in-memory and make sure that its used
      inMem.put("com.toggle.a", 0.8)
      assertFraction(togMap, 0.8)
    }
  }

}
