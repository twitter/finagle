package com.twitter.finagle.toggle

import com.twitter.finagle.server.{ServerInfo, environment}
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StandardToggleMapTest extends FunSuite {

  test("apply with a known libraryName") {
    flag.overrides.let(Map.empty) {
      // should load `ServiceLoadedToggleTestA`
      val tm = StandardToggleMap("A", NullStatsReceiver)
      val togs = tm.iterator.toSeq
      assert(togs.size == 1)
      assert(togs.head.id == "com.toggle.a")
    }
  }

  test("apply with an unknown libraryName") {
    flag.overrides.let(Map.empty) {
      val tm = StandardToggleMap("ZZZ", NullStatsReceiver)
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
      StandardToggleMap("B", NullStatsReceiver)
    }
  }

  test("apply with resource-based configs") {
    val togMap = StandardToggleMap(
      // this will have corresponding file(s) in test/resources/com/twitter/toggles/configs/
      "com.twitter.finagle.toggle.tests.StandardToggleMapTest",
      NullStatsReceiver,
      NullToggleMap,
      ServerInfo.Empty)

    val togs = togMap.iterator.toSeq

    def assertFraction(id: String, fraction: Double): Unit = {
      togs.find(_.id == id) match {
        case None => fail(s"$id not found in $togs")
        case Some(md) => assert(md.fraction == fraction)
      }
    }

    assertFraction("com.twitter.service-overrides-on", 1.0)
    assertFraction("com.twitter.service-overrides-off", 0.0)
    assertFraction("com.twitter.not-in-service-overrides", 0.0)
  }

  test("apply with resource-based configs and overrides") {
    environment.let("staging") {
      val togMap = StandardToggleMap(
        // this will have corresponding file(s) in test/resources/com/twitter/toggles/configs/
        "com.twitter.finagle.toggle.tests.EnvOverlays",
        NullStatsReceiver,
        NullToggleMap,
        ServerInfo.Flag)

      val togs = togMap.iterator.toSeq

      def assertFraction(id: String, fraction: Double): Unit = {
        togs.find(_.id == id) match {
          case None => fail(s"$id not found in $togs")
          case Some(md) => assert(md.fraction == fraction)
        }
      }

      assertFraction("com.twitter.base-is-off", 1.0)
      assertFraction("com.twitter.only-in-base", 0.0)
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
    val togMap = StandardToggleMap("A", NullStatsReceiver, inMem, ServerInfo.Empty)
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

  test("Toggles are observed") {
    val toggleName = "com.toggle.Test"
    val libraryName = "observedTest"
    val stats = new InMemoryStatsReceiver()
    val inMem = ToggleMap.newMutable()
    // start with the toggle turned on.
    inMem.put(toggleName, 1.0)

    val togMap = StandardToggleMap(libraryName, stats, inMem, ServerInfo.Empty)
    val gauge = stats.gauges(Seq("toggles", libraryName, "checksum"))
    val initial = gauge()

    // turn the toggle off and make sure the checksum changes
    inMem.put(toggleName, 0.0)
    assert(initial != gauge())
  }

}
