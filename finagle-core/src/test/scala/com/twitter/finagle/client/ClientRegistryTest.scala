package com.twitter.finagle.client

import com.twitter.finagle._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Var, Return, Activity}

import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.junit.JUnitRunner

import java.net.SocketAddress

object crtnamer {
  val va = Var[Addr](Addr.Pending)
}

class crtnamer extends Namer {
  import crtnamer._

  def enum(prefix: Path): Activity[Dtab] = Activity.pending

  def lookup(path: Path): Activity[NameTree[Name]] = {
    Activity(Var.value(Activity.Ok(NameTree.Leaf(Name.Bound(va, new Object())))))
  }
}

@RunWith(classOf[JUnitRunner])
class ClientRegistryTest extends FunSuite
  with StringClient
  with Eventually
  with IntegrationPatience
  with BeforeAndAfter {

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
    assert(allResolved0.poll === Some(Return(Set())))
  })

  test("ClientRegistry.expAllRegisteredClientsResolved handles Addr.Bound")(new Ctx {
    val va = Var[Addr](Addr.Pending)

    val c = stackClient.newClient(Name.Bound(va, new Object()), "foo")
    val allResolved = ClientRegistry.expAllRegisteredClientsResolved()
    assert(allResolved.poll === None)

    va() = Addr.Bound(Set.empty[SocketAddress])
    eventually {
      assert(allResolved.poll === Some(Return(Set("foo"))))
    }
  })

  test("ClientRegistry.expAllRegisteredClientsResolved handles Addr.Failed")(new Ctx {
    val va = Var[Addr](Addr.Pending)

    val c = stackClient.newClient(Name.Bound(va, new Object()), "foo")
    val allResolved = ClientRegistry.expAllRegisteredClientsResolved()
    assert(allResolved.poll === None)

    va() = Addr.Failed(new Exception("foo"))
    eventually {
      assert(allResolved.poll === Some(Return(Set("foo"))))
    }
  })

  test("ClientRegistry.expAllRegisteredClientsResolved handles Addr.Neg")(new Ctx {
    val va = Var[Addr](Addr.Pending)

    val c = stackClient.newClient(Name.Bound(va, new Object()), "foo")
    val allResolved = ClientRegistry.expAllRegisteredClientsResolved()
    assert(allResolved.poll === None)

    va() = Addr.Neg
    eventually {
      assert(allResolved.poll === Some(Return(Set("foo"))))
    }
  })

  test("ClientRegistry.expAllRegisteredClientsResolved more than one client")(new Ctx {
    val va0 = Var[Addr](Addr.Pending)
    val va1 = Var[Addr](Addr.Pending)

    val c0 = stackClient.newClient(Name.Bound(va0, new Object()), "foo")
    val allResolved0 = ClientRegistry.expAllRegisteredClientsResolved()
    assert(allResolved0.poll === None)
    va0() = Addr.Bound(Set.empty[SocketAddress])
    eventually {
      assert(allResolved0.poll === Some(Return(Set("foo"))))
    }

    val c1 = stackClient.newClient(Name.Bound(va1, new Object()), "bar")
    val allResolved1 = ClientRegistry.expAllRegisteredClientsResolved()
    assert(allResolved1.poll === None)
    va1() = Addr.Bound(Set.empty[SocketAddress])

    eventually {
      assert(allResolved1.poll === Some(Return(Set("foo", "bar"))))
    }
  })

  test("ClientRegistry.expAllRegisteredClientsResolved handles Name.Path")(new Ctx {
    val path = Path.read("/$/com.twitter.finagle.client.crtnamer/foo")
    val c = stackClient.newClient(Name.Path(path), "foo")
    val allResolved = ClientRegistry.expAllRegisteredClientsResolved()
    assert(allResolved.poll === None)
    crtnamer.va() = Addr.Bound(Set.empty[SocketAddress])
    eventually {
      assert(allResolved.poll === Some(Return(Set("foo"))))
    }
  })
}
