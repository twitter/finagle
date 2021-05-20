package com.twitter.finagle.naming

import com.twitter.finagle._
import com.twitter.util.Activity
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class NameInterpreterTest extends AnyFunSuite with BeforeAndAfter {

  val dtab = Dtab.read("/test=>/$/inet/localhost/1234")
  val name = Path.read("/test")

  after {
    NameInterpreter.global = DefaultInterpreter
  }

  test("NameInterpreter uses dtab when interpreter is not set") {
    assert(
      NameInterpreter.bind(dtab, name).sample() ==
        NameTree.Leaf(Path.read("/$/inet/localhost/1234"))
    )
  }

  test("NameInterpreter uses it when interpreter is set") {
    NameInterpreter.global = new NameInterpreter {
      override def bind(dtab: Dtab, path: Path): Activity[NameTree[Name.Bound]] =
        Activity.value(NameTree.Leaf(Name.empty))
    }

    assert(NameInterpreter.bind(dtab, name).sample() == NameTree.Leaf(Name.empty))
  }
}
