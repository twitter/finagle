package com.twitter.finagle

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StackTest extends FunSuite {
  object TestRole1 extends Stack.Role
  object TestRole2 extends Stack.Role
  object TestRole3 extends Stack.Role
  object TestRole4 extends Stack.Role

  def newStack() = {
    val stack = new StackBuilder(TestRole1, List(1,2,3,4))
    stack.push(TestRole2, (l: List[Int]) => 10 :: l)
    stack.push(TestRole3, (l: List[Int]) => 20 :: l)
    stack.result
  }

  test("Stack.make") {
    assert(newStack().make(Stack.Params.empty) === Seq(20,10,1,2,3,4))
  }

  test("Stack.transform") {
    val stack = newStack() transform {
      case Stack.Node(TestRole3, _, next) =>
        Stack.Node(TestRole4, (l: List[Int]) => 30::l, next)
      case Stack.Node(TestRole2, _, next) =>
        next
      case other => other
    }

    assert(stack.make(Stack.Params.empty) === Seq(30,1,2,3,4))
  }

  test("Stack.remove") {
    val stack = newStack()
    val prms = Stack.Params.empty
    assert(stack.remove(TestRole2).make(prms) === Seq(20,1,2,3,4))
    assert(stack.remove(TestRole3).make(prms) === Seq(10,1,2,3,4))
  }

  test("Stack.replace") {
    val stack = newStack().replace(TestRole2, new Stack.Simple[List[Int]](TestRole2) {
      def make(params: Stack.Params, next: List[Int]): List[Int] = 100 :: next
    })

    assert(stack.make(Stack.Params.empty) === Seq(20,100,1,2,3,4))
  }

  test("Stack.++") {
    val stack = newStack() ++ newStack()
    assert(stack.make(Stack.Params.empty) === Seq(20,10,20,10,1,2,3,4))
  }

  case class TestParam(i: Int)
  implicit object TestParam extends Stack.Param[TestParam] {
    val default = TestParam(1)
  }

  test("Params") {
    val params = Stack.Params.empty
    val params2 = params + TestParam(999)
    val params3 = params2 + TestParam(100)

    assert(!params.contains[TestParam])
    assert(params2.contains[TestParam])
    assert(params3.contains[TestParam])

    assert(params[TestParam] === TestParam(1))
    assert(params2[TestParam] === TestParam(999))
    assert(params3[TestParam] === TestParam(100))
  }

  test("Role.toString: should return lowercase object name") {
    assert(TestRole1.toString === "testrole1")
  }
}
