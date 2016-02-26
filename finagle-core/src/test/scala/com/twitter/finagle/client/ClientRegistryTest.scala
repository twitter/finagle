package com.twitter.finagle.client

import com.twitter.finagle._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.util.{StackRegistry, TestParam, TestParam2}
import com.twitter.util.{Var, Return, Activity, Future, Await}
import com.twitter.util.registry.{GlobalRegistry, SimpleRegistry, Entry}
import com.twitter.conversions.time.intToTimeableNumber

import org.junit.runner.RunWith
import org.mockito.Matchers.anyObject
import org.mockito.Mockito
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

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
  with BeforeAndAfter
  with MockitoSugar {

  trait Ctx {
    val sr = new InMemoryStatsReceiver
    val stackClient = stringClient
      .configured(param.Stats(sr))
      .configured(param.ProtocolLibrary("fancy"))
  }

  before {
    ClientRegistry.clear()
  }

  test("ClientRegistry.expAllRegisteredClientsResolved zero clients")(new Ctx {
    val allResolved0 = ClientRegistry.expAllRegisteredClientsResolved()
    assert(allResolved0.poll == Some(Return(Set())))
  })

  test("ClientRegistry.expAllRegisteredClientsResolved handles Addr.Bound")(new Ctx {
    val va = Var[Addr](Addr.Pending)

    val c = stackClient.newClient(Name.Bound(va, new Object()), "foo")
    val allResolved = ClientRegistry.expAllRegisteredClientsResolved()
    assert(allResolved.poll == None)

    va() = Addr.Bound(Set.empty[SocketAddress])
    eventually {
      assert(allResolved.poll == Some(Return(Set("foo"))))
    }
  })

  test("ClientRegistry.expAllRegisteredClientsResolved handles Addr.Failed")(new Ctx {
    val va = Var[Addr](Addr.Pending)

    val c = stackClient.newClient(Name.Bound(va, new Object()), "foo")
    val allResolved = ClientRegistry.expAllRegisteredClientsResolved()
    assert(allResolved.poll == None)

    va() = Addr.Failed(new Exception("foo"))
    eventually {
      assert(allResolved.poll == Some(Return(Set("foo"))))
    }
  })

  test("ClientRegistry.expAllRegisteredClientsResolved handles Addr.Neg")(new Ctx {
    val va = Var[Addr](Addr.Pending)

    val c = stackClient.newClient(Name.Bound(va, new Object()), "foo")
    val allResolved = ClientRegistry.expAllRegisteredClientsResolved()
    assert(allResolved.poll == None)

    va() = Addr.Neg
    eventually {
      assert(allResolved.poll == Some(Return(Set("foo"))))
    }
  })

  test("ClientRegistry.expAllRegisteredClientsResolved more than one client")(new Ctx {
    val va0 = Var[Addr](Addr.Pending)
    val va1 = Var[Addr](Addr.Pending)

    val c0 = stackClient.newClient(Name.Bound(va0, new Object()), "foo")
    val allResolved0 = ClientRegistry.expAllRegisteredClientsResolved()
    assert(allResolved0.poll == None)
    va0() = Addr.Bound(Set.empty[SocketAddress])
    eventually {
      assert(allResolved0.poll == Some(Return(Set("foo"))))
    }

    val c1 = stackClient.newClient(Name.Bound(va1, new Object()), "bar")
    val allResolved1 = ClientRegistry.expAllRegisteredClientsResolved()
    assert(allResolved1.poll == None)
    va1() = Addr.Bound(Set.empty[SocketAddress])

    eventually {
      assert(allResolved1.poll == Some(Return(Set("foo", "bar"))))
    }
  })

  test("ClientRegistry.expAllRegisteredClientsResolved handles Name.Path")(new Ctx {
    val path = Path.read("/$/com.twitter.finagle.client.crtnamer/foo")
    val c = stackClient.newClient(Name.Path(path), "foo")
    val allResolved = ClientRegistry.expAllRegisteredClientsResolved()
    assert(allResolved.poll == None)
    crtnamer.va() = Addr.Bound(Set.empty[SocketAddress])
    eventually {
      assert(allResolved.poll == Some(Return(Set("foo"))))
    }
  })

  test("ClientRegistry registers clients in registry")(new Ctx {
    val path = Path.read("/$/com.twitter.finagle.client.crtnamer/foo")
    val simple = new SimpleRegistry
    GlobalRegistry.withRegistry(simple) {
      val c = stackClient.newClient(Name.Path(path), "foo")
      val prefix = Seq("client", "fancy", "foo", "/$/com.twitter.finagle.client.crtnamer/foo", "Pool")
      val filtered = GlobalRegistry.get.toSet.filter { e =>
        e.key.startsWith(prefix)
      }
      val expected = Seq(
        "high" -> "2147483647",
        "low" -> "0",
        "idleTime" -> "Duration.Top",
        "maxWaiters" -> "2147483647"
      ).map { case (key, value) => Entry(prefix :+ key, value) }

      expected.foreach { entry =>
        assert(filtered.contains(entry))
      }
    }
  })

  // copied from StackRegistryTest
  val headRole = Stack.Role("head")
  val nameRole = Stack.Role("name")

  val param1 = TestParam(999)


  def newStack(): Stack[ServiceFactory[Int, Int]] = {
    val mockSvc = mock[Service[Int, Int]]
    when(mockSvc.apply(anyObject[Int])).thenReturn(Future.value(10))

    val factory = ServiceFactory.const(mockSvc)

    val stack = new StackBuilder(Stack.Leaf(new Stack.Head {
      def role: Stack.Role = headRole
      def description: String = "the head!!"
      def parameters: Seq[Stack.Param[_]] = Seq(TestParam2.param)
    }, factory))
    val stackable: Stackable[ServiceFactory[Int, Int]] = new Stack.Module1[TestParam, ServiceFactory[Int, Int]] {
      def make(p: TestParam, l: ServiceFactory[Int, Int]): ServiceFactory[Int, Int] = l.map { _.map { _ + p.p1 }}

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
}
