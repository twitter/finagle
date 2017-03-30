package com.twitter.finagle.tunable

import com.twitter.util.tunable.{ServiceLoadedTunableMap, TunableMap}

import org.scalatest.FunSuite

class StandardTunableMapTestClient extends ServiceLoadedTunableMap with TunableMap.Proxy {
  val serviceLoaded = TunableMap.newMutable()
  serviceLoaded.put("com.twitter.util.tunable.ServiceLoaded", "service loaded")

  protected def underlying: TunableMap = serviceLoaded
  def id: String = "IdForStandardTunableMapTest"
}

class StandardTunableMapTest extends FunSuite {

  test("composes in-memory, service-loaded, and file-based tunable maps") {
    val inMemory = TunableMap.newMutable()
    inMemory.put("com.twitter.util.tunable.InMemory", "in memory")

    val standardTunableMap = StandardTunableMap("IdForStandardTunableMapTest", inMemory)
    val components = TunableMap.components(standardTunableMap)

    assert(components(0)(TunableMap.Key[String]("com.twitter.util.tunable.InMemory"))() ==
      Some("in memory"))
    assert(components(1)(TunableMap.Key[String]("com.twitter.util.tunable.ServiceLoaded"))() ==
      Some("service loaded"))
    assert(components(2)(TunableMap.Key[String]("com.twitter.util.tunable.FileBased"))() ==
      Some("file based"))
  }
}
