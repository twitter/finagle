package com.twitter.finagle.toggle

import com.twitter.finagle.server.ServerInfo
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.stats.NullStatsReceiver
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import scala.collection.JavaConverters._
import org.scalatest.funsuite.AnyFunSuite

class StandardToggleMapTest extends AnyFunSuite {

  private def newRegistry(): ConcurrentMap[String, ToggleMap.Mutable] =
    new ConcurrentHashMap[String, ToggleMap.Mutable]()

  test("registeredLibraries") {
    val uniqueLibName = s"com.twitter.${System.nanoTime}"
    assert(!StandardToggleMap.registeredLibraries.contains(uniqueLibName))

    val tm = StandardToggleMap(uniqueLibName, NullStatsReceiver)
    assert(StandardToggleMap.registeredLibraries.contains(uniqueLibName))
    assert(tm == StandardToggleMap.registeredLibraries(uniqueLibName))
  }

  // note the underlying utility (Toggle.validateId) is heavily tested in ToggleTest
  test("apply validates libraryName") {
    def assertNotAllowed(libraryName: String): Unit = {
      intercept[IllegalArgumentException] {
        StandardToggleMap(libraryName, NullStatsReceiver)
      }
    }

    assertNotAllowed("")
    assertNotAllowed("A")
    assertNotAllowed("finagle")
    assertNotAllowed("com.toggle!")
  }

  test("apply returns the same instance for a given libraryName") {
    val name = "com.twitter.Test"
    val registry = newRegistry()
    val duplicateHandling = JsonToggleMap.FailParsingOnDuplicateId
    val tm0 =
      StandardToggleMap(
        name,
        NullStatsReceiver,
        ToggleMap.newMutable(),
        ServerInfo(),
        registry,
        duplicateHandling)
    val tm1 =
      StandardToggleMap(
        name,
        NullStatsReceiver,
        ToggleMap.newMutable(),
        ServerInfo(),
        registry,
        duplicateHandling)
    assert(tm0 eq tm1)
  }

  test("apply with a known libraryName") {
    flag.overrides.let(Map.empty) {
      // should load `ServiceLoadedToggleTestA`
      val tm = StandardToggleMap("com.twitter.finagle.toggle.test.A", NullStatsReceiver)
      val togs = tm.iterator.toSeq
      assert(togs.size == 1)
      assert(togs.head.id == "com.toggle.a")
    }
  }

  test("apply with an unknown libraryName") {
    flag.overrides.let(Map.empty) {
      val tm = StandardToggleMap("com.twitter.finagle.toggle.test.ZZZ", NullStatsReceiver)
      assert(tm.iterator.isEmpty)
      val toggle = tm("com.toggle.XYZ")
      assert(toggle.isUndefined)
      intercept[UnsupportedOperationException] {
        toggle(245)
      }
    }
  }

  test("apply with a duplicate libraryName") {
    intercept[IllegalStateException] {
      StandardToggleMap("com.twitter.finagle.toggle.test.B", NullStatsReceiver)
    }
  }

  test("apply with resource-based configs") {
    val togMap = StandardToggleMap(
      // this will have corresponding file(s) in test/resources/com/twitter/toggles/configs/
      "com.twitter.finagle.toggle.tests.StandardToggleMapTest",
      NullStatsReceiver,
      ToggleMap.newMutable(),
      ServerInfo.Empty,
      newRegistry(),
      JsonToggleMap.FailParsingOnDuplicateId
    )

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
    val serverInfo: ServerInfo = new ServerInfo {
      def environment: Option[String] = Some("staging")
      def id: String = "testing"
      def instanceId: Option[Long] = None
      def clusterId: String = id
      def zone: Option[String] = None
    }
    val togMap = StandardToggleMap(
      // this will have corresponding file(s) in test/resources/com/twitter/toggles/configs/
      "com.twitter.finagle.toggle.tests.EnvOverlays",
      NullStatsReceiver,
      ToggleMap.newMutable(),
      serverInfo,
      newRegistry(),
      JsonToggleMap.FailParsingOnDuplicateId
    )

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

  test("selectResource ignores duplicate inputs") {
    // this will have a corresponding file in test/resources/com/twitter/toggles/configs/
    val rsc = getClass.getClassLoader
      .getResources(
        "com/twitter/toggles/configs/com.twitter.finagle.toggle.tests.StandardToggleMapTest.json"
      )
      .asScala
      .toSeq
      .head

    val selected = StandardToggleMap.selectResource("configName", Seq(rsc, rsc))
    assert(selected == rsc)
  }

  test("selectResource fails with multiple unique inputs") {
    // these will have a corresponding file in test/resources/com/twitter/toggles/configs/
    val rsc1 = getClass.getClassLoader
      .getResources(
        "com/twitter/toggles/configs/com.twitter.finagle.toggle.tests.StandardToggleMapTest.json"
      )
      .asScala
      .toSeq
      .head
    val rsc2 = getClass.getClassLoader
      .getResources("com/twitter/toggles/configs/com.twitter.finagle.toggle.tests.Valid.json")
      .asScala
      .toSeq
      .head

    intercept[IllegalArgumentException] {
      StandardToggleMap.selectResource("configName", Seq(rsc1, rsc2))
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
    val togMap = StandardToggleMap(
      "com.twitter.finagle.toggle.test.A",
      NullStatsReceiver,
      inMem,
      ServerInfo.Empty,
      newRegistry(),
      JsonToggleMap.FailParsingOnDuplicateId
    )
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
    val libraryName = "com.twitter.finagle.toggle.test.Observed"
    val stats = new InMemoryStatsReceiver()
    val inMem = ToggleMap.newMutable()
    // start with the toggle turned on.
    inMem.put(toggleName, 1.0)

    val togMap = StandardToggleMap(
      libraryName,
      stats,
      inMem,
      ServerInfo.Empty,
      newRegistry(),
      JsonToggleMap.FailParsingOnDuplicateId
    )
    val gauge = stats.gauges(Seq("toggles", libraryName, "checksum"))
    val initial = gauge()

    // turn the toggle off and make sure the checksum changes
    inMem.put(toggleName, 0.0)
    assert(initial != gauge())
  }

  test("components") {
    val inMem = ToggleMap.newMutable()
    val togMap = StandardToggleMap(
      "com.twitter.components",
      NullStatsReceiver,
      inMem,
      ServerInfo.Empty,
      newRegistry(),
      JsonToggleMap.FailParsingOnDuplicateId
    )

    val components = ToggleMap.components(togMap)
    assert(5 == components.size, components.mkString(", "))
    assert(components.exists(_ eq inMem))
  }

  test("mutating a togglemap directly") {
    val inMem = ToggleMap.newMutable()
    val togMap: ToggleMap.Mutable = StandardToggleMap(
      "com.twitter.components",
      NullStatsReceiver,
      inMem,
      ServerInfo.Empty,
      newRegistry(),
      JsonToggleMap.FailParsingOnDuplicateId
    )

    assert(togMap("com.twitter.foo").isUndefined)
    togMap.put("com.twitter.foo", 1.0)
    assert(togMap("com.twitter.foo")(1))
  }
}
