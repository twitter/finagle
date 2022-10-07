package com.twitter.finagle.client

import com.twitter.finagle._
import com.twitter.finagle.client.utils.StringClient
import com.twitter.finagle.stack.nilStack
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.util.StackRegistry
import com.twitter.finagle.util.TestParam
import com.twitter.finagle.util.TestParam2
import com.twitter.util._
import com.twitter.util.registry.Entry
import com.twitter.util.registry.GlobalRegistry
import com.twitter.util.registry.SimpleRegistry
import org.mockito.ArgumentMatchers.anyObject
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.IntegrationPatience
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

object crtnamer {
  @volatile var observationsOpened = 0
  @volatile var observationsClosed = 0
  val e = Event[Addr]
  val va = Var.async[Addr](Addr.Pending) { u =>
    observationsOpened += 1
    val obs = e.register(Witness(u))
    Closable.make { deadline =>
      observationsClosed += 1
      obs.close(deadline)
    }
  }
}

class crtnamer extends Namer {
  import crtnamer._

  def lookup(path: Path): Activity[NameTree[Name]] = {
    Activity(Var.value(Activity.Ok(NameTree.Leaf(Name.Bound(va, new Object())))))
  }
}

class ClientRegistryTest
    extends AnyFunSuite
    with Eventually
    with IntegrationPatience
    with BeforeAndAfter
    with MockitoSugar {

  trait Ctx {
    val sr = new InMemoryStatsReceiver
    val stackClient = StringClient.client
      .configured(param.Stats(sr))
      .configured(param.ProtocolLibrary("fancy"))
  }

  before {
    ClientRegistry.clear()
  }

  test("ClientRegistry.expAllRegisteredClientsResolved handles Addr.Bound")(new Ctx {
    val va = Var[Addr](Addr.Pending)

    val c = stackClient.newClient(Name.Bound(va, new Object()), "foo")
    val allResolved = ClientRegistry.expAllRegisteredClientsResolved()

    va() = Addr.Bound(Set.empty[Address])
    eventually {
      assert(allResolved.poll.get.get().contains("foo"))
    }
  })

  test("ClientRegistry.expAllRegisteredClientsResolved handles Addr.Failed")(new Ctx {
    val va = Var[Addr](Addr.Pending)

    val c = stackClient.newClient(Name.Bound(va, new Object()), "foo")
    val allResolved = ClientRegistry.expAllRegisteredClientsResolved()

    va() = Addr.Failed(new Exception("foo"))
    eventually {
      assert(allResolved.poll.get.get().contains("foo"))
    }
  })

  test("ClientRegistry.expAllRegisteredClientsResolved handles Addr.Neg")(new Ctx {
    val va = Var[Addr](Addr.Pending)

    val c = stackClient.newClient(Name.Bound(va, new Object()), "foo")
    val allResolved = ClientRegistry.expAllRegisteredClientsResolved()

    va() = Addr.Neg
    eventually {
      assert(allResolved.poll.get.get().contains("foo"))
    }
  })

  test("ClientRegistry.expAllRegisteredClientsResolved more than one client")(new Ctx {
    val va0 = Var[Addr](Addr.Pending)
    val va1 = Var[Addr](Addr.Pending)

    val c0 = stackClient.newClient(Name.Bound(va0, new Object()), "foo")
    val allResolved0 = ClientRegistry.expAllRegisteredClientsResolved()
    va0() = Addr.Bound(Set.empty[Address])
    eventually {
      assert(allResolved0.poll.get.get.contains("foo"))
    }

    val c1 = stackClient.newClient(Name.Bound(va1, new Object()), "bar")
    val allResolved1 = ClientRegistry.expAllRegisteredClientsResolved()
    va1() = Addr.Bound(Set.empty[Address])

    eventually {
      val res = allResolved1.poll.get.get
      assert(res.contains("foo") && res.contains("bar"))
    }
  })

  test("ClientRegistry.expAllRegisteredClientsResolved handles Name.Path")(new Ctx {
    val path = Path.read("/$/com.twitter.finagle.client.crtnamer/foo")
    val c = stackClient.newClient(Name.Path(path), "foo")
    val allResolved = ClientRegistry.expAllRegisteredClientsResolved()
    crtnamer.e.notify(Addr.Bound(Set.empty[Address]))
    eventually {
      assert(allResolved.poll.get.get().contains("foo"))
    }
    c.close()
  })

  test("ClientRegistry does not release Var observations")(new Ctx {
    // reset observation counters in crtnamer, since other tests may alter them
    crtnamer.observationsOpened = 0
    crtnamer.observationsClosed = 0
    val path = Path.read("/$/com.twitter.finagle.client.crtnamer/foo")
    assert(crtnamer.observationsOpened == 0)
    val c = stackClient.newClient(Name.Path(path), "foo")
    assert(crtnamer.observationsOpened == 1) // check that we kicked off resolution
    assert(crtnamer.observationsClosed == 0)
    c.close()
    assert(crtnamer.observationsOpened == 1)
    assert(crtnamer.observationsClosed == 1)
  })

  test("ClientRegistry registers clients in registry")(new Ctx {
    val path = Path.read("/$/com.twitter.finagle.client.crtnamer/foo")
    val simple = new SimpleRegistry
    GlobalRegistry.withRegistry(simple) {
      val c = stackClient.newClient(Name.Path(path), "foo")
      val prefix =
        Seq("client", "fancy", "foo", "/$/com.twitter.finagle.client.crtnamer/foo", "Pool")
      val filtered = GlobalRegistry.get.toSet.filter { e => e.key.startsWith(prefix) }
      val expected = Seq(
        "high" -> "2147483647",
        "low" -> "0",
        "idleTime" -> "Duration.Top",
        "maxWaiters" -> "2147483647"
      ).map { case (key, value) => Entry(prefix :+ key, value) }

      expected.foreach { entry => assert(filtered.contains(entry)) }
    }
  })

  // copied from StackRegistryTest
  val headRole = Stack.Role("head")
  val nameRole = Stack.Role("name")

  val param1 = TestParam(999)

  def newStack(): Stack[ServiceFactory[Int, Int]] = {
    val mockSvc = mock[Service[Int, Int]]
    when(mockSvc.apply(anyObject[Int])).thenReturn(Future.value(10))
    when(mockSvc.close(anyObject[Time])).thenReturn(Future.Done)

    val factory = ServiceFactory.const(mockSvc)

    val stack = new StackBuilder(
      Stack.leaf(
        new Stack.Head {
          def role: Stack.Role = headRole
          def description: String = "the head!!"
          def parameters: Seq[Stack.Param[_]] = Seq(TestParam2.param)
        },
        factory))
    val stackable: Stackable[ServiceFactory[Int, Int]] =
      new Stack.Module1[TestParam, ServiceFactory[Int, Int]] {
        def make(p: TestParam, l: ServiceFactory[Int, Int]): ServiceFactory[Int, Int] = l.map {
          _.map { _ + p.p1 }
        }

        val description: String = "description"
        val role: Stack.Role = nameRole
      }
    stack.push(stackable)

    stack.result
  }

  test("RegistryEntryLifecycle module registers a Stack and then deregisters it") {
    val stk = newStack()
    val params = Stack.Params.empty + param1 + param.Label("foo") + param.ProtocolLibrary("fancy")
    val simple = new SimpleRegistry()
    GlobalRegistry.withRegistry(simple) {
      val factory = (RegistryEntryLifecycle.module[Int, Int] +: stk).make(params)
      val expected = {
        Set(
          Entry(Seq("client", "fancy", "foo", "/$/fail", "name", "p1"), "999"),
          Entry(Seq("client", "fancy", "foo", "/$/fail", "head", "p2"), "1")
        )
      }
      assert(GlobalRegistry.get.toSet == expected)
      Await.result(factory.close())

      assert(GlobalRegistry.get.isEmpty)
    }
  }

  test("RegistryEntryLifecycle module cleans up duplicates after service closes") {
    val stk = newStack()
    val params = Stack.Params.empty + param.Label("foo")

    ClientRegistry.register("first", stk, params)
    ClientRegistry.register("second", stk, params)
    val factory = (RegistryEntryLifecycle.module[Int, Int] +: stk).make(params)

    assert(ClientRegistry.registeredDuplicates.size == 2)
    assert(ClientRegistry.registeredDuplicates(0).name == "foo")
    assert(ClientRegistry.registeredDuplicates(0).addr == "second")
    assert(ClientRegistry.registeredDuplicates(1).name == "foo")
    assert(ClientRegistry.registeredDuplicates(1).addr == "/$/fail")

    factory.close()

    assert(ClientRegistry.registeredDuplicates.size == 1)
    assert(ClientRegistry.registeredDuplicates(0).name == "foo")
    assert(ClientRegistry.registeredDuplicates(0).addr == "/$/fail")
  }

  test("ClientRegistry does not register nilStack") {
    val stk = newStack()
    ClientRegistry.register("bar", stk, Stack.Params.empty)
    val registrans: Set[StackRegistry.Entry] = ClientRegistry.registrants.toSet
    val modules: Set[StackRegistry.Module] = registrans.flatMap(_.modules)
    assert(!modules.exists(_.name == nilStack.head.role.name))
  }
}
