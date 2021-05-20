package com.twitter.finagle.naming

import com.twitter.finagle.{Addr, Dtab, Path, Name, NameTree}
import com.twitter.util.{Var, Activity}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class LoadedNameInterpreterTest extends AnyFunSuite with BeforeAndAfter {
  class TestInterpreter(id: String) extends NameInterpreter {
    val va = Var.value(Addr.Bound())
    override def bind(dtab: Dtab, path: Path) =
      Activity.value(NameTree.Leaf(Name.Bound(va, id)))
  }

  test("falls back to DefaultInterpreter") {
    val dtab = Dtab.read("""
      /a => /b;
      /b => $;
    """)
    val interpreter = new LoadedNameInterpreter(() => Seq.empty)
    val act = interpreter.bind(dtab, Path.read("/a/x"))
    assert(act.sample() == NameTree.Empty)
  }

  test("multiple interpreters not allowed") {
    val one = new TestInterpreter("one")
    val two = new TestInterpreter("two")
    val exn = intercept[MultipleNameInterpretersException] {
      new LoadedNameInterpreter(() => Seq(one, two))
    }
    assert(exn.interpreters == Seq(one, two))
  }

  test("eagerly loads") {
    var created = false
    def load(): Seq[NameInterpreter] = {
      val interpreter = new TestInterpreter("a") {
        created = true
      }
      Seq(interpreter)
    }
    val interpreter = new LoadedNameInterpreter(load _)
    assert(created)
    val act = interpreter.bind(Dtab.empty, Path.read("/a"))
    val NameTree.Leaf(name) = act.sample()
    assert(name.id == "a")
  }
}
