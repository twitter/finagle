package com.twitter.finagle.util

import com.twitter.finagle.{Stack, StackBuilder, Stackable, param, stack}
import com.twitter.util.Var
import com.twitter.util.registry.{Entry, GlobalRegistry, SimpleRegistry}
import org.scalatest.funsuite.AnyFunSuite

class StackRegistryTest extends AnyFunSuite {

  val headRole = Stack.Role("head")
  val nameRole = Stack.Role("name")

  val param1 = TestParam(999)

  def newStack() = {
    val stack = new StackBuilder(
      Stack.leaf(
        new Stack.Head {
          def role: Stack.Role = headRole
          def description: String = "the head!!"
          def parameters: Seq[Stack.Param[_]] = Seq(TestParam2.param)
        },
        List(1, 2, 3, 4)))
    val stackable: Stackable[List[Int]] = new Stack.Module1[TestParam, List[Int]] {
      def make(p: TestParam, l: List[Int]): List[Int] = p.p1 :: l

      val description: String = "description"
      val role: Stack.Role = nameRole
    }
    stack.push(stackable)

    stack.result
  }

  test("StackRegistry registryPrefix includes expected keys") {
    new StackRegistry {
      def registryName: String = "reg_name"

      // we run the test inside a subclass to have access to the protected
      // `registryPrefix` method.
      private val params = Stack.Params.empty +
        param.Label("a_label") +
        param.ProtocolLibrary("a_protocol_lib")
      private val entry = StackRegistry.Entry("an_addr", stack.nilStack, params)
      private val prefix = registryPrefix(entry)
      assert(
        prefix ==
          Seq("reg_name", "a_protocol_lib", "a_label", "an_addr")
      )
    }
  }

  test("StackRegistry should register stacks and params properly") {
    val reg = new StackRegistry { def registryName: String = "test" }
    val stk = newStack()
    val params = Stack.Params.empty + param1 + param.Label("foo") + param.ProtocolLibrary("qux")
    val simple = new SimpleRegistry()
    GlobalRegistry.withRegistry(simple) {
      reg.register("bar", stk, params)
      val expected = {
        Set(
          Entry(Seq("test", "qux", "foo", "bar", "name", "p1"), "999"),
          Entry(Seq("test", "qux", "foo", "bar", "head", "p2"), "1")
        )
      }
      assert(GlobalRegistry.get.toSet == expected)
    }
  }

  test("StackRegistry should record params properly that override the Stack.Param.show method") {
    val reg = new StackRegistry {
      def registryName: String = "test"
    }
    val stack = new StackBuilder(
      Stack.leaf(
        new Stack.Head {
          def role: Stack.Role = headRole
          def description: String = "the head!!"
          def parameters: Seq[Stack.Param[_]] = Seq(NotCaseClassParam.param)
        },
        List(1, 2, 3, 4)
      )
    ).result
    val params = (Stack.Params.empty
      + new NotCaseClassParam(Var(50))
      + param.Label("foo")
      + param.ProtocolLibrary("qux"))
    val simple = new SimpleRegistry()
    GlobalRegistry.withRegistry(simple) {
      reg.register("bar", stack, params)
      val expected = Set(Entry(List("test", "qux", "foo", "bar", "head", "ncc"), "50"))
      assert(GlobalRegistry.get.toSet == expected)
    }
  }

  test("StackRegistry should reflect updates to mutable param values") {
    val reg = new StackRegistry {
      def registryName: String = "test"
    }
    val stack = new StackBuilder(
      Stack.leaf(
        new Stack.Head {
          def role: Stack.Role = headRole
          def description: String = "the head!!"
          def parameters: Seq[Stack.Param[_]] = Seq(NotCaseClassParam.param)
        },
        List(1, 2, 3, 4)
      )
    ).result

    val mutableParam = Var(50)

    val params = Stack.Params.empty + new NotCaseClassParam(mutableParam)
    reg.register("bar", stack, params)

    val entry1: StackRegistry.Entry = reg.registrants.toSet.head
    assert(entry1.modules == Seq(StackRegistry.Module("head", "the head!!", List(("ncc", "50")))))

    mutableParam.update(60)
    val entry2: StackRegistry.Entry = reg.registrants.toSet.head
    assert(entry2.modules == Seq(StackRegistry.Module("head", "the head!!", List(("ncc", "60")))))
  }

  test("StackRegistry should unregister stacks and params properly") {
    val reg = new StackRegistry { def registryName: String = "test" }
    val stk = newStack()
    val params = Stack.Params.empty + param1 + param.Label("foo") + param.ProtocolLibrary("qux")
    val simple = new SimpleRegistry()
    GlobalRegistry.withRegistry(simple) {
      reg.register("bar", stk, params)
      val expected = {
        Set(
          Entry(Seq("test", "qux", "foo", "bar", "name", "p1"), "999"),
          Entry(Seq("test", "qux", "foo", "bar", "head", "p2"), "1")
        )
      }
      assert(GlobalRegistry.get.toSet == expected)

      reg.unregister("bar", stk, params)
      assert(GlobalRegistry.get.toSet.isEmpty)
    }
  }

  test("StackRegistry keeps track of the number of GlobalRegistry entries it enters") {
    val reg = new StackRegistry { def registryName: String = "test" }
    val stk = newStack()
    val params = Stack.Params.empty + param1 + param.Label("foo") + param.ProtocolLibrary("qux")
    val simple = new SimpleRegistry()
    GlobalRegistry.withRegistry(simple) {
      reg.register("bar", stk, params)
      assert(GlobalRegistry.get.size == reg.size)

      reg.unregister("bar", stk, params)
      assert(GlobalRegistry.get.size == reg.size)
    }
  }

  test("Duplicates are tracked") {
    val reg = new StackRegistry { def registryName: String = "test" }
    val stk = newStack()

    val name = "aname"
    reg.register("addr1", stk, Stack.Params.empty + param.Label(name))
    assert(reg.registeredDuplicates.isEmpty)

    reg.register("addr2", stk, Stack.Params.empty + param.Label(name))
    assert(reg.registeredDuplicates.size == 1)

    reg.register("addr3", stk, Stack.Params.empty + param.Label("somethingelse"))
    assert(reg.registeredDuplicates.size == 1)
  }

}
