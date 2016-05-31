package com.twitter.finagle.toggle

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

//
// These are used via the `LoadService` mechanism.
// See finagle-toggle/src/test/resources/META-INF/services/com.twitter.finagle.toggle.ServiceLoadedToggleMap
//
class ServiceLoadedToggleTestA extends ServiceLoadedToggleMap with ToggleMap.Proxy {
  private val tm = ToggleMap.newMutable()
  tm.put("com.toggle.a", 1.0)

  protected def underlying: ToggleMap = tm
  def libraryName: String = "A"
}

class ServiceLoadedToggleTestB extends ServiceLoadedToggleMap with ToggleMap.Proxy {
  protected def underlying: ToggleMap = NullToggleMap
  def libraryName: String = "B"
}

class ServiceLoadedToggleTestBToo extends ServiceLoadedToggleMap with ToggleMap.Proxy {
  protected def underlying: ToggleMap = NullToggleMap
  def libraryName: String = "B"
}

@RunWith(classOf[JUnitRunner])
class ServiceLoadedToggleMapTest extends FunSuite {

  test("one libraryName match") {
    val tm = ServiceLoadedToggleMap("A")

    // make sure we got ToggleMap we expected
    assert(tm("com.toggle.a")(500))
    val toggles = tm.iterator.toSeq
    assert(toggles.size == 1)
    assert(toggles.head.id == "com.toggle.a")
  }

  test("no libraryName matches") {
    val tm = ServiceLoadedToggleMap("ZZZ")
    assert(tm.iterator.isEmpty)
  }

  test("two libraryName matches") {
    intercept[IllegalStateException] {
      ServiceLoadedToggleMap("B")
    }
  }

}
