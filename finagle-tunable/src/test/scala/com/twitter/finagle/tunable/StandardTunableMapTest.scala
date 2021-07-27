package com.twitter.finagle.tunable

import com.twitter.finagle.server.ServerInfo
import com.twitter.util.tunable.{NullTunableMap, ServiceLoadedTunableMap, TunableMap}

import org.scalatest.funsuite.AnyFunSuite

class StandardTunableMapTestClient extends ServiceLoadedTunableMap with TunableMap.Proxy {
  val serviceLoaded = TunableMap.newMutable()
  serviceLoaded.put("com.twitter.util.tunable.ServiceLoaded", "service loaded")

  protected def underlying: TunableMap = serviceLoaded
  def id: String = "IdForStandardTunableMapTest"
}

class StandardTunableMapTest extends AnyFunSuite {

  test("Application returns the same map for the same id") {
    val map1 = StandardTunableMap("foo")
    val map2 = StandardTunableMap("foo")

    assert(map1 eq map2)
  }

  test("composes in-memory, service-loaded, and file-based tunable maps") {
    val inMemory = TunableMap.newMutable()
    inMemory.put("com.twitter.util.tunable.InMemory", "in memory")

    val standardTunableMap =
      StandardTunableMap("IdForStandardTunableMapTest", ServerInfo.Empty, inMemory)
    val components = TunableMap.components(standardTunableMap)

    assert(
      components(0)(TunableMap.Key[String]("com.twitter.util.tunable.InMemory"))() ==
        Some("in memory")
    )
    assert(
      components(1)(TunableMap.Key[String]("com.twitter.util.tunable.ServiceLoaded"))() ==
        Some("service loaded")
    )
    assert(
      components(2)(TunableMap.Key[String]("com.twitter.util.tunable.FileBased"))() ==
        Some("file based")
    )
  }

  // For loadJsonConfig, there are 4 different files that we look for, in order of priority:
  // 1. /env/instance-id.json
  // 2. /env/instances.json
  // 3. instance-id.json
  // 4. instances.json
  // There are 2^4 = 16 different permutations here, but we'll cover a selection.
  test("loadJsonConfig: all possible files exist") {
    val serverInfo: ServerInfo = new ServerInfo {
      def environment: Option[String] = Some("staging")
      def instanceId: Option[Long] = Some(0)
      def id: String = "id"
      def clusterId: String = id
      def zone: Option[String] = None
    }

    val map = StandardTunableMap.loadJsonConfig("IdForStandardTunableMapTest", serverInfo)
    val components = TunableMap.components(map)

    assert(components.size == 4)

    assert(
      components(0)(
        TunableMap.Key[String]("com.twitter.util.tunable.FileBasedPerEnvPerInstance"))() ==
        Some("file based per env per instance")
    )
    assert(
      components(1)(TunableMap.Key[String]("com.twitter.util.tunable.FileBasedPerEnv"))() ==
        Some("file based per env")
    )
    assert(
      components(2)(TunableMap.Key[String]("com.twitter.util.tunable.FileBasedPerInstance"))() ==
        Some("file based per instance")
    )
    assert(
      components(3)(TunableMap.Key[String]("com.twitter.util.tunable.FileBased"))() ==
        Some("file based")
    )
  }

  test("loadJsonConfig: per-instance-id and all-instances files exists") {
    val serverInfo: ServerInfo = new ServerInfo {
      def environment: Option[String] = None
      def instanceId: Option[Long] = Some(0)
      def id: String = "id"
      def clusterId: String = id
      def zone: Option[String] = None
    }

    val map = StandardTunableMap.loadJsonConfig("IdForStandardTunableMapTest", serverInfo)
    val components = TunableMap.components(map)

    assert(components.size == 2)

    assert(
      components(0)(TunableMap.Key[String]("com.twitter.util.tunable.FileBasedPerInstance"))() ==
        Some("file based per instance")
    )
    assert(
      components(1)(TunableMap.Key[String]("com.twitter.util.tunable.FileBased"))() ==
        Some("file based")
    )
  }

  test("loadJsonConfig: per-environment all-instances and all-instances files exist") {
    val serverInfo: ServerInfo = new ServerInfo {
      def environment: Option[String] = Some("staging")
      def instanceId: Option[Long] = None
      def id: String = "id"
      def clusterId: String = id
      def zone: Option[String] = None
    }

    val map = StandardTunableMap.loadJsonConfig("IdForStandardTunableMapTest", serverInfo)
    val components = TunableMap.components(map)

    assert(components.size == 2)

    assert(
      components(0)(TunableMap.Key[String]("com.twitter.util.tunable.FileBasedPerEnv"))() ==
        Some("file based per env")
    )
    assert(
      components(1)(TunableMap.Key[String]("com.twitter.util.tunable.FileBased"))() ==
        Some("file based")
    )
  }

  test("loadJsonConfig: all-instances file exists") {
    val serverInfo: ServerInfo = new ServerInfo {
      def environment: Option[String] = None
      def instanceId: Option[Long] = None
      def id: String = "id"
      def clusterId: String = id
      def zone: Option[String] = None
    }

    val map = StandardTunableMap.loadJsonConfig("IdForStandardTunableMapTest", serverInfo)
    val components = TunableMap.components(map)

    assert(components.size == 1)

    assert(
      components(0)(TunableMap.Key[String]("com.twitter.util.tunable.FileBased"))() ==
        Some("file based")
    )
  }

  test("loadJsonConfig: no files exist") {
    val serverInfo: ServerInfo = new ServerInfo {
      def environment: Option[String] = None
      def instanceId: Option[Long] = None
      def id: String = "id"
      def clusterId: String = id
      def zone: Option[String] = None
    }

    val map = StandardTunableMap.loadJsonConfig("IdWithNoFiles", serverInfo)
    val components = TunableMap.components(map)

    assert(components.size == 1)

    assert(components(0) == NullTunableMap)
  }
}
