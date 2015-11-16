package com.twitter.finagle

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class StackTest extends FunSuite {
  val testRole1 = Stack.Role("TestRole1")
  val testRole2 = Stack.Role("TestRole2")
  val testRole3 = Stack.Role("TestRole3")
  val testRole4 = Stack.Role("TestRole4")

  val testHead4 = new Stack.Head {
    val role = testRole4
    val description = testRole4.toString
    val parameters = Nil
  }

  val empty = Stack.Params.empty

  def newStack() = {
    val stack = new StackBuilder(testRole1, List(1, 2, 3, 4))
    stack.push(testRole2, (l: List[Int]) => 10 :: l)
    stack.push(testRole3, (l: List[Int]) => 20 :: l)
    stack.result
  }

  test("Stack.make") {
    assert(newStack().make(empty) == Seq(20, 10, 1, 2, 3, 4))
  }

  test("Stack.transform") {
    val stack = newStack() transform {
      case Stack.Node(head, mk, next) =>
        if (head.role == testRole3) Stack.Node(testHead4, (l: List[Int]) => 30::l, next)
        else if (head.role == testRole2) next
        else Stack.Node(head, mk, next)
      case other => other
    }

    assert(stack.make(empty) == Seq(30, 1, 2, 3, 4))
  }

  test("Stack.insertBefore") {
    val stack = newStack()
    val module = new Stack.Module0[List[Int]] {
      val role = testRole4
      val description = testRole4.toString
      def make(next: List[Int]): List[Int] = 100 :: next
    }

    assert(
      stack.insertBefore(testRole4, module).make(empty) ==
        Seq(20, 10, 1, 2, 3, 4))

    assert(
      stack.insertBefore(testRole2, module).make(empty) ==
        Seq(20, 100, 10, 1, 2, 3, 4))

    assert(
      (stack ++ stack).insertBefore(testRole2, module).make(empty) ==
        Seq(20, 100, 10, 20, 100, 10, 1, 2, 3, 4))
  }

  test("Stack.insertAfter") {
    val stack = newStack()
    val module = new Stack.Module0[List[Int]] {
      val role = testRole4
      val description = testRole4.toString
      def make(next: List[Int]): List[Int] = 100 :: next
    }

    assert(
      stack.insertAfter(testRole4, module).make(empty) ==
        Seq(20, 10, 1, 2, 3, 4))

    assert(
      stack.insertAfter(testRole2, module).make(empty) ==
        Seq(20, 10, 100, 1, 2, 3, 4))

    assert(
      (stack ++ stack).insertAfter(testRole2, module).make(empty) ==
        Seq(20, 10, 100, 20, 10, 100, 1, 2, 3, 4))
  }

  test("Stack.remove") {
    val stack = newStack()
    assert(stack.remove(testRole4).make(empty) == Seq(20, 10, 1, 2, 3, 4))
    assert(stack.remove(testRole2).make(empty) == Seq(20, 1, 2, 3, 4))
    assert(stack.remove(testRole3).make(empty) == Seq(10, 1, 2, 3, 4))

    assert(
      (stack ++ stack).remove(testRole2).make(empty) ==
        Seq(20, 20, 1, 2, 3, 4))
  }

  test("Stack.replace") {
    val stack = newStack()
    val module = new Stack.Module0[List[Int]] {
      val role = testRole2
      val description = testRole2.toString
      def make(next: List[Int]): List[Int] = 100 :: next
    }

    assert(stack.replace(testRole4, module).make(empty) == Seq(20, 10, 1, 2, 3, 4))
    assert(stack.replace(testRole2, module).make(empty) == Seq(20, 100, 1, 2, 3, 4))

    assert(
      (stack ++ stack).replace(testRole2, module).make(empty) ==
        Seq(20, 100, 20, 100, 1, 2, 3, 4))
  }

  test("Stack.++") {
    val stack = newStack() ++ newStack()
    assert(stack.make(empty) == Seq(20, 10, 20, 10, 1, 2, 3, 4))
  }

  test("Stack.+:") {
    val stk0 = newStack()
    assert(stk0.make(empty) == Seq(20, 10, 1, 2, 3, 4))

    val m1 = new Stack.Module0[List[Int]] {
      val role = testRole1
      val description = testRole1.toString
      def make(next: List[Int]): List[Int] = 30 :: next
    }

    val stk1 = m1 +: stk0
    assert(stk1.make(empty) == Seq(30, 20, 10, 1, 2, 3, 4))

    val m2 = new Stack.Module0[List[Int]] {
      val role = testRole1
      val description = testRole1.toString
      def make(next: List[Int]): List[Int] = 40 :: next
    }

    val stk2 = m2 +: stk1
    assert(stk2.make(empty) == Seq(40, 30, 20, 10, 1, 2, 3, 4))
  }

  case class TestParam(p1: Int) {
    def mk() = (this, TestParam.param)
  }
  object TestParam {
    implicit val param = Stack.Param(TestParam(1))
  }

  case class TestParamInnerVar(p1: Int) {
    val p2: String = "foo"
    def mk() = (this, TestParamInnerVar.param)
  }
  object TestParamInnerVar {
    implicit val param = Stack.Param(TestParamInnerVar(1))
  }

  test("Params") {
    val params = empty
    val params2 = params + TestParam(999)
    val params3 = params2 + TestParam(100)

    assert(!params.contains[TestParam])
    assert(params2.contains[TestParam])
    assert(params3.contains[TestParam])

    assert(params[TestParam] == TestParam(1))
    assert(params2[TestParam] == TestParam(999))
    assert(params3[TestParam] == TestParam(100))

    assert(!(params ++ params).contains[TestParam])
    assert((params ++ params2)[TestParam] == TestParam(999))
    assert((params2 ++ params)[TestParam] == TestParam(999))
    assert((params2 ++ params3)[TestParam] == TestParam(100))
    assert((params3 ++ params2)[TestParam] == TestParam(999))

    val params4 = params + TestParamInnerVar(0)
    assert((params2 ++ params4)[TestParam] == TestParam(999))
    assert((params2 ++ params4)[TestParamInnerVar] == TestParamInnerVar(0))
  }

  test("Role.toString: should return lowercase object name") {
    assert(testRole1.toString == "testrole1")
  }
}
