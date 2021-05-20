package com.twitter.finagle.client

import com.twitter.finagle.{Addr, Dtab, Address}
import com.twitter.finagle.Namer.AddrWeightKey
import com.twitter.util.Var
import org.scalatest.funsuite.AnyFunSuite

final class EndpointRegistryTest extends AnyFunSuite {

  val name = "fooClient"
  val path = "/foo"
  val path2 = "/bar"

  val addrs: Set[Address] = Set(Address(8080))

  val bound = Addr.Bound(addrs.toSet, Addr.Metadata(AddrWeightKey -> 2.0))
  val bound2 = Addr.Bound(addrs.toSet, Addr.Metadata(AddrWeightKey -> 3.0))

  val endpoints = Var[Addr](bound)
  val endpoints2 = Var[Addr](bound2)

  val dtab = Dtab.read("/foo => /bar")
  val dtab2 = Dtab.read("/foo => /baz")

  def getEndpoints(
    registry: EndpointRegistry,
    name: String,
    dtab: Dtab,
    path: String
  ): Option[Addr] = {
    registry.endpoints(name).get(dtab).flatMap(_.get(path))
  }

  test("adding endpoints for a path for a dtab for a new client adds it to the registry") {
    val registry = new EndpointRegistry()

    assert(registry.endpoints(name).isEmpty)

    registry.addObservation(name, dtab, path, endpoints)

    assert(registry.endpoints(name).size == 1)

    assert(getEndpoints(registry, name, dtab, path) == Some(bound))
  }

  test("adding endpoints for a path for a dtab for an existing client adds them to the registry") {
    val registry = new EndpointRegistry()

    registry.addObservation(name, dtab, path, endpoints)
    registry.addObservation(name, dtab2, path2, endpoints2)

    assert(getEndpoints(registry, name, dtab, path) == Some(bound))
    assert(getEndpoints(registry, name, dtab2, path2) == Some(bound2))
  }

  test("adding endpoints for a path for an existing dtab adds them to the registry") {
    val registry = new EndpointRegistry()

    registry.addObservation(name, dtab, path, endpoints)
    registry.addObservation(name, dtab, path2, endpoints2)

    assert(getEndpoints(registry, name, dtab, path) == Some(bound))
    assert(getEndpoints(registry, name, dtab, path2) == Some(bound2))
  }

  test(
    "removing a path for a dtab for a client with only one dtab removes the client from the registry"
  ) {
    val registry = new EndpointRegistry()

    registry.addObservation(name, dtab, path, endpoints)

    assert(getEndpoints(registry, name, dtab, path) == Some(bound))

    registry.removeObservation(name, dtab, path)

    assert(registry.endpoints(name).size == 0)
  }

  test("removing a path for a dtab with only one path removes the dtab") {
    val registry = new EndpointRegistry()

    registry.addObservation(name, dtab, path, endpoints)
    registry.addObservation(name, dtab2, path2, endpoints2)

    registry.removeObservation(name, dtab, path)

    assert(getEndpoints(registry, name, dtab, path) == None)
    assert(getEndpoints(registry, name, dtab2, path2) == Some(bound2))

    assert(registry.endpoints(name).size == 1)

  }
}
