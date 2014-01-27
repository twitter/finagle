package com.twitter.finagle

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StackTest extends FunSuite {
  def newStack() = {
    val stack = new StackBuilder("term", List(1,2,3,4))
    stack.push("conc.10", (l: List[Int]) => 10 :: l)
    stack.push("conc.20", (l: List[Int]) => 20 :: l)
    stack.result
  }
  
  test("Stack.make") {
    assert(newStack().make(Stack.Params.empty) === Seq(20,10,1,2,3,4))
  }

  test("Stack.transform") {
    val stack = newStack() transform {
      case Stack.Node("conc.20", _, next) =>
        Stack.Node("conc.30", (l: List[Int]) => 30::l, next)
      case Stack.Node("conc.10", _, next) =>
        next
    }
    
    assert(stack.make(Stack.Params.empty) === Seq(30,1,2,3,4))
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

    assert(params[TestParam] === TestParam(1))
    assert(params2[TestParam] === TestParam(999))
    assert(params3[TestParam] === TestParam(100))
  }
}
