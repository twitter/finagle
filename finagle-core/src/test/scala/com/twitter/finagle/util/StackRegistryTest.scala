package com.twitter.finagle.util

import com.twitter.finagle.{Stack, StackBuilder, Stackable}
import com.twitter.util.registry.{SimpleRegistry, GlobalRegistry, Entry}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

case class TestParam(p1: Int) {
  def mk() = (this, TestParam.param)
}
object TestParam {
  implicit val param = Stack.Param(TestParam(1))
}

case class TestParam2(p2: Int) {
  def mk() = (this, TestParam2.param)
}

object TestParam2 {
  implicit val param = Stack.Param(TestParam2(1))
}

@RunWith(classOf[JUnitRunner])
class StackRegistryTest extends FunSuite {

  val headRole = Stack.Role("head")
  val nameRole = Stack.Role("name")

  val param1 = TestParam(999)

  def newStack() = {
    val stack = new StackBuilder(Stack.Leaf(new Stack.Head {
      def role: Stack.Role = headRole
      def description: String = "the head!!"
      def parameters: Seq[Stack.Param[_]] = Seq(TestParam2.param)
    }, List(1, 2, 3, 4)))
    val stackable: Stackable[List[Int]] = new Stack.Module1[TestParam, List[Int]] {
      def make(p: TestParam, l: List[Int]): List[Int] = p.p1 :: l

      val description: String = "description"
      val role: Stack.Role = nameRole
    }
    stack.push(stackable)

    stack.result
  }

  test("StackRegistry should register stacks and params properly") {
    val reg = new StackRegistry { def registryName: String = "test" }
    val stk = newStack()
    val params = Stack.Params.empty + param1
    val simple = new SimpleRegistry()
    GlobalRegistry.withRegistry(simple) {
      reg.register("foo", "bar", stk, params)
      val expected = {
        Set(
          Entry(Seq("test", "foo", "bar", "name", "p1"), "999"),
          Entry(Seq("test", "foo", "bar", "head", "p2"), "1")
        )
      }
      assert(GlobalRegistry.get.toSet == expected)
    }
  }
}
