package com.twitter.finagle.naming

import com.twitter.finagle._
import com.twitter.util.Activity
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class NameInterpreterTest extends FunSuite with BeforeAndAfter {

  val dtab = Dtab.read("/test=>/$/inet/some-host/1234")
  val name = Path.read("/test")

  after {
    NameInterpreter.global = DefaultInterpreter
  }


  // CSL-2175
  ignore("NameInterpreter uses dtab when interpreter is not set") {
    assert(NameInterpreter.bind(dtab, name).sample() ==
      NameTree.Leaf(Path.read("/$/inet/some-host/1234")))
  }

  test("NameInterpreter uses it when interpreter is set") {
    NameInterpreter.global = new NameInterpreter {
      override def bind(dtab: Dtab, path: Path): Activity[NameTree[Name.Bound]] =
        Activity.value(NameTree.Leaf(Name.empty))
    }

    assert(NameInterpreter.bind(dtab, name).sample() == NameTree.Leaf(Name.empty))
  }
}
