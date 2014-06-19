package com.twitter.finagle

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import java.net.InetSocketAddress
import com.twitter.util.{Return, Throw, Activity, Witness, Try}

@RunWith(classOf[JUnitRunner])
class NamerTest extends FunSuite {
  trait Ctx {
    def ia(i: Int) = new InetSocketAddress(i)

    val exc = new Exception {}

    val namer = new Namer {
      var acts: Map[Path, (Activity[NameTree[Path]], Witness[Try[NameTree[Path]]])] =
        Map.empty

      def contains(path: String) = acts contains Path.read(path)

      def apply(path: String): Witness[Try[NameTree[Path]]] = {
        val (_, wit) = acts(Path.read(path))
        wit
      }

      val pathNamer = new Namer {
        def lookup(path: Path): Activity[NameTree[Name]] = path match {
          // Don't capture system paths.
          case Path.Utf8("$", _*) => Activity.value(NameTree.Neg)
          case Path.Utf8(elems@_*) =>
            val p = Path.Utf8(elems: _*)
            acts.get(p) match {
              case Some((a, _)) => a map { tree => tree.map(Name(_)) }
              case None =>
                val tup@(act, _) = Activity[NameTree[Path]]()
                acts += p -> tup
                act map { tree => tree.map(Name(_)) }
            }
          case _ => Activity.value(NameTree.Neg)
        }
      }

      val namer = pathNamer orElse Namer.global
      def lookup(path: Path) = namer.lookup(path)
    }
  }

  def assertEval(res: Activity[NameTree[Name.Bound]], ias: InetSocketAddress*) {
    assert(res.sample().eval === Some((ias map { ia => Name.bound(ia) }).toSet))
  }

  test("NameTree.bind: union")(new Ctx {
    val res = namer.bind(NameTree.read("/test/0 & /test/1"))
    assert(res.run.sample() === Activity.Pending)

    namer("/test/0").notify(Return(NameTree.read("/test/2")))
    assert(res.run.sample() === Activity.Pending)

    namer("/test/1").notify(Return(NameTree.read("/$/inet/0/1")))
    assert(res.run.sample() === Activity.Pending)

    namer("/test/2").notify(Return(NameTree.read("/$/inet/0/2")))

    assertEval(res, ia(1), ia(2))

    namer("/test/2").notify(Return(NameTree.Neg))
    assertEval(res, ia(1))

    namer("/test/1").notify(Return(NameTree.Neg))
    assert(res.sample().eval === None)

    namer("/test/1").notify(Return(NameTree.Empty))

    assert(res.sample().eval === Some(Set.empty))

    namer("/test/2").notify(Throw(exc))
    assert(res.run.sample() === Activity.Failed(exc))
  })

  test("NameTree.bind: failover")(new Ctx {
    val res = namer.bind(NameTree.read("/test/0 | /test/1 & /test/2"))
    assert(res.run.sample() === Activity.Pending)

    namer("/test/0").notify(Return(NameTree.Empty))
    namer("/test/1").notify(Return(NameTree.Neg))
    namer("/test/2").notify(Return(NameTree.Neg))

    assert(res.sample().eval === Some(Set.empty))
    
    namer("/test/0").notify(Return(NameTree.read("/$/inet/0/1")))
    assertEval(res, ia(1))

    namer("/test/0").notify(Return(NameTree.Neg))
    assert(res.sample().eval === None)

    namer("/test/2").notify(Return(NameTree.read("/$/inet/0/2")))
    assertEval(res, ia(2))

    namer("/test/0").notify(Return(NameTree.read("/$/inet/0/3")))
    assertEval(res, ia(3)) 
  })

  test("Namer.global: /$/nil") {
    assert(Namer.global.lookup(Path.read("/$/nil")).sample() === NameTree.Empty)
    assert(Namer.global.lookup(Path.read("/$/nil/foo/bar")).sample()
      === NameTree.Empty)
  }
}


