package com.twitter.finagle.client

import com.twitter.finagle._
import com.twitter.finagle.Namer.AddrWeightKey
import com.twitter.util._
import org.scalatest.funsuite.AnyFunSuite

final class EndpointRecorderTest extends AnyFunSuite {

  val name = "fooClient"
  val path = "/foo"

  val addrs: Set[Address] = Set(Address(8080))

  val bound = Addr.Bound(addrs.toSet, Addr.Metadata(AddrWeightKey -> 2.0))

  val endpoints = Var[Addr](bound)

  val dtab = Dtab.read("/foo => /bar")

  val neverFactory = ServiceFactory.const(new Service[Int, Int] {
    def apply(req: Int) = Future.never
  })

  def getEndpoints(
    registry: EndpointRegistry,
    name: String,
    dtab: Dtab,
    path: String
  ): Option[Addr] = {
    registry.endpoints(name).get(dtab).flatMap(_.get(path))
  }

  test("EndpointRecorder is disabled if BindingFactory.Dest is not bound") {
    val stk: StackBuilder[ServiceFactory[Int, Int]] = new StackBuilder(
      Stack.leaf(Stack.Role("never"), neverFactory)
    )

    stk.push(EndpointRecorder.module[Int, Int])

    val factory = stk.make(Stack.Params.empty)

    assert(factory == neverFactory)
  }

  test("EndpointRecorder registers in EndpointRegistry") {
    val registry = new EndpointRegistry()
    val factory = new EndpointRecorder(neverFactory, registry, name, dtab, path, endpoints)

    assert(getEndpoints(registry, name, dtab, path) == Some(bound))
  }

  test("EndpointRecorder deregisters on close()") {
    val registry = new EndpointRegistry()
    val factory = new EndpointRecorder(neverFactory, registry, name, dtab, path, endpoints)

    assert(getEndpoints(registry, name, dtab, path) == Some(bound))
    factory.close()
    assert(getEndpoints(registry, name, dtab, path) == None)
  }
}
