package com.twitter.finagle.client

import com.twitter.finagle._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Var, Return}

import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.concurrent.Eventually
import org.scalatest.junit.JUnitRunner

import java.net.SocketAddress

@RunWith(classOf[JUnitRunner])
class ClientRegistryTest extends FunSuite with StringClient with Eventually with BeforeAndAfter {
  trait Ctx {
    val sr = new InMemoryStatsReceiver
    val stackClient = stringClient
      .configured(param.Stats(sr))
  }

  before {
    ClientRegistry.clear()
  }

  test("ClientRegistry.expAllRegisteredClientsResolved zero clients")(new Ctx {
    val allResolved0 = ClientRegistry.expAllRegisteredClientsResolved()
    assert(allResolved0.poll === Some(Return(())))
  })

  test("ClientRegistry.expAllRegisteredClientsResolved handles Addr.Bound")(new Ctx {
    val va = Var[Addr](Addr.Pending)

    val c = stackClient.newClient(Name.Bound(va, new Object()), "zero")
    val allResolved = ClientRegistry.expAllRegisteredClientsResolved()
    assert(allResolved.poll === None)

    va() = Addr.Bound(Set.empty[SocketAddress])
    eventually { assert(allResolved.isDefined) }
  })

  test("ClientRegistry.expAllRegisteredClientsResolved handles Addr.Failed")(new Ctx {
    val va = Var[Addr](Addr.Pending)

    val c = stackClient.newClient(Name.Bound(va, new Object()), "zero")
    val allResolved = ClientRegistry.expAllRegisteredClientsResolved()
    assert(allResolved.poll === None)

    va() = Addr.Failed(new Exception("foo"))
    eventually { assert(allResolved.isDefined) }
  })

  test("ClientRegistry.expAllRegisteredClientsResolved handles Addr.Neg")(new Ctx {
    val va = Var[Addr](Addr.Pending)

    val c = stackClient.newClient(Name.Bound(va, new Object()), "zero")
    val allResolved = ClientRegistry.expAllRegisteredClientsResolved()
    assert(allResolved.poll === None)

    va() = Addr.Neg
    eventually { assert(allResolved.isDefined) }
  })

  test("ClientRegistry.expAllRegisteredClientsResolved more than one client")(new Ctx {
    val va0 = Var[Addr](Addr.Pending)
    val va1 = Var[Addr](Addr.Pending)

    val c0 = stackClient.newClient(Name.Bound(va0, new Object()), "zero")
    val allResolved0 = ClientRegistry.expAllRegisteredClientsResolved()
    assert(allResolved0.poll === None)
    va0() = Addr.Bound(Set.empty[SocketAddress])
    eventually { assert(allResolved0.isDefined) }

    val c1 = stackClient.newClient(Name.Bound(va1, new Object()), "one")
    val allResolved1 = ClientRegistry.expAllRegisteredClientsResolved()
    assert(allResolved1.poll === None)
    va1() = Addr.Bound(Set.empty[SocketAddress])
    eventually { assert(allResolved1.isDefined) }
  })
}
