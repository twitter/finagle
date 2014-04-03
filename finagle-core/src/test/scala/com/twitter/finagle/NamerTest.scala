package com.twitter.finagle

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import java.net.{SocketAddress, InetSocketAddress}
import com.twitter.util.{Return, Throw, Var, Activity, Witness, Try}

@RunWith(classOf[JUnitRunner])
class NamerTest extends FunSuite {
  trait Ctx {
    def ia(i: Int) = new InetSocketAddress(i)

    val exc = new Exception{}

    val namer = new Namer {
      var acts: Map[Path, (Activity[NameTree[Path]], Witness[Try[NameTree[Path]]])] = 
        Map.empty

      def contains(path: String) = acts contains Path.read(path)

      def apply(path: String): Witness[Try[NameTree[Path]]] = {
        val (_, wit) = acts(Path.read(path))
        wit
      }

      def lookup(path: Path): Activity[NameTree[Path]] = path match {
        case Path.Utf8(elems@_*) =>
          val p = Path.Utf8(elems:_*)
          acts.get(p) match {
            case Some((a, _)) => a
            case None =>
              val tup@(act, _) = Activity[NameTree[Path]]()
              acts += p -> tup
              act
          }
        case _ =>  Activity.value(NameTree.Neg)
      }
    }
  }

  test("NameTree.bindAndEval: union") (new Ctx {
    val res = namer.bindAndEval(NameTree.read("/test/0 & /test/1"))
    assert(res.sample() === Addr.Pending)

    namer("/test/0").notify(Return(NameTree.read("/test/2")))
    assert(res.sample() === Addr.Pending)

    namer("/test/1").notify(Return(NameTree.read("/$/inet//1")))
    assert(res.sample() === Addr.Pending)

    namer("/test/2").notify(Return(NameTree.read("/$/inet//2")))
    assert(res.sample() === Addr.Bound(ia(1), ia(2)))

    namer("/test/2").notify(Return(NameTree.Neg))
    assert(res.sample() === Addr.Bound(ia(1)))

    namer("/test/1").notify(Return(NameTree.Neg))
    assert(res.sample() === Addr.Neg)

    namer("/test/1").notify(Return(NameTree.Empty))
    assert(res.sample() === Addr.Bound())

    namer("/test/2").notify(Throw(exc))
    assert(res.sample() === Addr.Failed(exc))
  })

  test("NameTree.bindAndEval: failover") (new Ctx {
    val res = namer.bindAndEval(NameTree.read("/test/0 | /test/1 & /test/2"))
    assert(res.sample() === Addr.Pending)
    
    namer("/test/0").notify(Return(NameTree.Empty))
    assert(res.sample() === Addr.Bound())
    
    // (Test laziness.)
    assert(!(namer contains "/test/1"))

    namer("/test/0").notify(Return(NameTree.read("/$/inet//1")))
    assert(res.sample() === Addr.Bound(ia(1)))

    namer("/test/0").notify(Return(NameTree.Neg))
    assert(res.sample() === Addr.Pending)

    namer("/test/1").notify(Return(NameTree.Neg))
    assert(res.sample() === Addr.Pending)
    
    namer("/test/2").notify(Return(NameTree.read("/$/inet//2")))
    assert(res.sample() === Addr.Bound(ia(2)))
    
    namer("/test/0").notify(Throw(exc))
    assert(res.sample() === Addr.Failed(exc))

    namer("/test/0").notify(Return(NameTree.read("/$/inet//3")))
    assert(res.sample() === Addr.Bound(ia(3)))
    namer("/test/1").notify(Throw(exc))
    assert(res.sample() === Addr.Bound(ia(3)))
  })

  test("Namer.global: /$/nil") {
    assert(Namer.global.lookup(Path.read("/$/nil")).sample() === NameTree.Empty)
    assert(Namer.global.lookup(Path.read("/$/nil/foo/bar")).sample() 
      === NameTree.Empty)
  }
}


