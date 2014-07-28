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
    val params = Map.empty[String, String]
  }

  def newStack() = {
    val stack = new StackBuilder(testRole1, List(1,2,3,4))
    stack.push(testRole2, (l: List[Int]) => 10 :: l)
    stack.push(testRole3, (l: List[Int]) => 20 :: l)
    stack.result
  }

  test("Stack.make") {
    assert(newStack().make(Stack.Params.empty) === Seq(20,10,1,2,3,4))
  }

  test("Stack.transform") {
    val stack = newStack() transform {
      case Stack.Node(head, mk, next) =>
        if(head.role == testRole3) Stack.Node(testHead4, (l: List[Int]) => 30::l, next)
        else if(head.role == testRole2) next
        else Stack.Node(head, mk, next)
      case other => other
    }

    assert(stack.make(Stack.Params.empty) === Seq(30,1,2,3,4))
  }

  test("Stack.remove") {
    val stack = newStack()
    val prms = Stack.Params.empty
    assert(stack.remove(testRole2).make(prms) === Seq(20,1,2,3,4))
    assert(stack.remove(testRole3).make(prms) === Seq(10,1,2,3,4))
  }

  test("Stack.replace") {
    val stack = newStack().replace(testRole2, new Stack.Simple[List[Int]] {
      val role = testRole2
      val description = testRole2.toString
      def make(next: List[Int])(implicit params: Stack.Params): List[Int] = 100 :: next
    })

    assert(stack.make(Stack.Params.empty) === Seq(20,100,1,2,3,4))
  }

  test("Stack.++") {
    val stack = newStack() ++ newStack()
    assert(stack.make(Stack.Params.empty) === Seq(20,10,20,10,1,2,3,4))
  }

  case class TestParam(p1: Int)
  implicit object TestParam extends Stack.Param[TestParam] {
    val default = TestParam(1)
  }

  case class TestParamInnerVar(p1: Int) {
    val p2: String = "foo"
  }
  implicit object TestParamInnerVar extends Stack.Param[TestParamInnerVar] {
    val default = TestParamInnerVar(1)
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

  test("Head params get param in map") {
    val params = Stack.Params.empty + TestParam(5)
    val stack = newStack().replace(testRole3, 
      new Stack.Simple[List[Int]] {
        val role = testRole3
        val description = testRole3.toString
        def make(next: List[Int])(implicit params: Stack.Params): List[Int] = {
          val TestParam(num) = get[TestParam]
          num :: next
        }
      })

    assert(stack.head.params.isEmpty)
    stack.make(params)
    stack.head.params.get("p1") match {
      case None => fail("Parameter p1 not added to the map")
      case Some(x) => assert(x.contains("5"))
    }
  }

  test("Head params get param not in map") {
    val params = Stack.Params.empty + TestParam(5)
    val stack = newStack().replace(testRole3, 
      new Stack.Simple[List[Int]] {
        val role = testRole3
        val description = testRole3.toString
        def make(next: List[Int])(implicit params: Stack.Params): List[Int] = {
          val TestParam(num) = get[TestParam]
          num :: next
        }
      })

    stack.make(params)
    stack.head.params.get("foo") match {
      case None =>
      case _ => fail("Getting non-existant param from params did not return None")
    }
  }
  
  test("Head params when fields/values of params class unequal lengths") {
    val params = Stack.Params.empty + TestParamInnerVar(5)
    val stack = newStack().replace(testRole3, 
      new Stack.Simple[List[Int]] {
        val role = testRole3
        val description = testRole3.toString
        def make(next: List[Int])(implicit params: Stack.Params): List[Int] = {
          val TestParamInnerVar(num) = get[TestParamInnerVar]
          num :: next
        }
      })

    assert(stack.head.params.isEmpty)
    stack.make(params)

    stack.head.params.get("p1") match {
      case None => fail("Parameter p1 not added to the map")
      case Some(x) => assert(x.contains("5"))
    }

    stack.head.params.get("p2") match {
      case None => fail("Parameter p2 should be added to map but have unknown value")
      case Some(x) => assert(x.contains("unknown"))
    }
  }

  test("Role.toString: should return lowercase object name") {
    assert(testRole1.toString === "testrole1")
  }
}