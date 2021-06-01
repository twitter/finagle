package com.twitter.finagle

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Namer.AddrWeightKey
import com.twitter.finagle.naming.namerMaxDepth
import com.twitter.util._
import org.scalatestplus.junit.AssertionsForJUnit
import scala.language.reflectiveCalls
import org.scalatest.funsuite.AnyFunSuite

class NamerTest extends AnyFunSuite with AssertionsForJUnit {
  trait Ctx {
    case class OrElse(fst: Namer, snd: Namer) extends Namer {
      def lookup(path: Path): Activity[NameTree[Name]] =
        (fst.lookup(path) join snd.lookup(path)) map {
          case (left, right) => NameTree.Alt(left, right)
        }
    }

    def boundWithWeight(weight: Double, addrs: Address*): Name.Bound =
      Name.Bound(
        Var.value(Addr.Bound(addrs.toSet, Addr.Metadata(AddrWeightKey -> weight))),
        addrs.toSet
      )

    class TestException extends Exception {}
    val exc = new TestException {}

    val namer = new Namer {
      var acts: Map[Path, (Activity[NameTree[Path]], Witness[Try[NameTree[Path]]])] =
        Map.empty

      def contains(path: String) = acts contains Path.read(path)

      def apply(path: String): Witness[Try[NameTree[Path]]] = {
        val p = Path.read(path)
        val (_, wit) = acts.getOrElse(p, addPath(p))
        wit
      }

      private def addPath(p: Path) = {
        val tup = Activity[NameTree[Path]]
        acts += p -> tup
        tup
      }

      val pathNamer = new Namer {
        def lookup(path: Path): Activity[NameTree[Name]] = path match {
          // Don't capture system paths.
          case Path.Utf8("$", _*) => Activity.value(NameTree.Neg)
          case p @ Path.Utf8(elems @ _*) =>
            acts.get(p) match {
              case Some((a, _)) =>
                a map { tree => tree.map(Name(_)) }
              case None =>
                val (act, _) = addPath(p)
                act map { tree => tree.map(Name(_)) }
            }
          case _ => Activity.value(NameTree.Neg)
        }
      }

      val namer = OrElse(pathNamer, Namer.global)
      def lookup(path: Path) = namer.lookup(path)
    }
  }

  def assertEval(res: Activity[NameTree[Name.Bound]], expected: Name.Bound*) =
    res.sample().eval match {
      case Some(actual) =>
        assert(actual.map(_.addr.sample) == expected.map(_.addr.sample).toSet)
      case _ => assert(false)
    }

  test("NameTree.bind: union")(new Ctx {
    val res = namer.bind(NameTree.read("/test/0 & /test/1"))

    // Pending & Pending
    assert(res.run.sample() == Activity.Pending)

    // Bind /test/0 to another NameTree
    namer("/test/0").notify(Return(NameTree.read("/test/2")))
    assert(res.run.sample() == Activity.Pending)

    // Ok(Bound) & Pending
    namer("/test/1").notify(Return(NameTree.read("/$/inet/1")))
    assertEval(res, boundWithWeight(1.0, Address(1)))

    // Failed(exc) & Pending
    namer("/test/1").notify(Throw(exc))
    intercept[TestException] { res.sample() }

    // Ok(Bound) & Ok(Bound)
    namer("/test/1").notify(Return(NameTree.read("/$/inet/1")))
    namer("/test/2").notify(Return(NameTree.read("/$/inet/2")))
    assertEval(res, boundWithWeight(1.0, Address(1)), boundWithWeight(1.0, Address(2)))

    // Ok(Bound) & Ok(Neg)
    namer("/test/2").notify(Return(NameTree.Neg))
    assertEval(res, boundWithWeight(1.0, Address(1)))

    // Ok(Bound) & Failed(exc)
    namer("/test/2").notify(Throw(exc))
    assertEval(res, boundWithWeight(1.0, Address(1)))

    // Failed(exc) & Failed(exc)
    namer("/test/1").notify(Throw(exc))
    intercept[TestException] { res.sample() }

    // Ok(Neg) & Ok(Neg)
    namer("/test/1").notify(Return(NameTree.Neg))
    namer("/test/2").notify(Return(NameTree.Neg))
    assert(res.sample().eval == None)

    // Ok(Empty) & Ok(Neg)
    namer("/test/1").notify(Return(NameTree.Empty))
    assert(res.sample().eval == Some(Set.empty))
  })

  test("NameTree.bind: failover")(new Ctx {
    val res = namer.bind(NameTree.read("/test/0 | /test/1 & /test/2"))
    assert(res.run.sample() == Activity.Pending)

    namer("/test/0").notify(Return(NameTree.Empty))
    namer("/test/1").notify(Return(NameTree.Neg))
    namer("/test/2").notify(Return(NameTree.Neg))

    assert(res.sample().eval == Some(Set.empty))

    namer("/test/0").notify(Return(NameTree.read("/$/inet/1")))
    assertEval(res, Name.bound(Address(1)))

    namer("/test/0").notify(Return(NameTree.Neg))
    assert(res.sample().eval == None)

    namer("/test/2").notify(Return(NameTree.read("/$/inet/2")))
    assertEval(res, boundWithWeight(1.0, Address(2)))

    namer("/test/0").notify(Return(NameTree.read("/$/inet/3")))
    assertEval(res, Name.bound(Address(3)))
  })

  test("NameTree.bind: Alt with Fail/Empty")(new Ctx {
    assert(namer.bind(NameTree.read("(! | /test/1 | /test/2)")).sample() == NameTree.Fail)
    assert(namer.bind(NameTree.read("(~ | /$/fail | /test/1)")).sample() == NameTree.Fail)
    assert(namer.bind(NameTree.read("(/$/nil | /$/fail | /test/1)")).sample() == NameTree.Empty)
  })

  test("Namer.global: /$/inet") {
    Namer.global.lookup(Path.read("/$/inet/1234")).sample() match {
      case NameTree.Leaf(Name.Bound(addr)) =>
        assert(addr.sample() == Addr.Bound(Set(Address(1234))))
      case _ => fail()
    }

    Await.result(
      Namer.global.lookup(Path.read("/$/inet/127.0.0.1/1234")).values.toFuture(),
      1.second
    )() match {
      case NameTree.Leaf(Name.Bound(addr)) =>
        assert(
          Await.result(addr.changes.filter(_ != Addr.Pending).toFuture(), 1.second)
            == Addr.Bound(Set(Address("127.0.0.1", 1234)))
        )
      case _ => fail()
    }

    intercept[ClassNotFoundException] {
      Namer.global.lookup(Path.read("/$/inet")).sample()
    }

    Namer.global.lookup(Path.read("/$/inet/1234/foobar")).sample() match {
      case NameTree.Leaf(bound: Name.Bound) =>
        assert(bound.addr.sample() == Addr.Bound(Address(1234)))
        assert(bound.id == Path.Utf8("$", "inet", "1234"))
        assert(bound.path == Path.Utf8("foobar"))

      case _ => fail()
    }

    Namer.global.lookup(Path.read("/$/inet/1234/foobar")).sample() match {
      case NameTree.Leaf(bound: Name.Bound) =>
        assert(bound.addr.sample() == Addr.Bound(Address(1234)))
        assert(bound.id == Path.Utf8("$", "inet", "1234"))
        assert(bound.path == Path.Utf8("foobar"))

      case _ => fail()
    }
  }

  test("Namer.global: /$/fixedinet") {
    Namer.global.lookup(Path.read("/$/fixedinet/1234")).sample() match {
      case NameTree.Leaf(Name.Bound(addr)) =>
        assert(addr.sample() == Addr.Bound(Set(Address(1234))))
      case _ => fail()
    }

    Await.result(
      Namer.global.lookup(Path.read("/$/fixedinet/127.0.0.1/1234")).values.toFuture(),
      1.second
    )() match {
      case NameTree.Leaf(Name.Bound(addr)) =>
        assert(
          Await.result(addr.changes.filter(_ != Addr.Pending).toFuture(), 1.second)
            == Addr.Bound(Set(Address("127.0.0.1", 1234)))
        )
      case _ => fail()
    }

    intercept[ClassNotFoundException] {
      Namer.global.lookup(Path.read("/$/fixedinet")).sample()
    }

    Namer.global.lookup(Path.read("/$/fixedinet/1234/foobar")).sample() match {
      case NameTree.Leaf(bound: Name.Bound) =>
        assert(bound.addr.sample() == Addr.Bound(Address(1234)))
        assert(bound.id == Path.Utf8("$", "fixedinet", "1234"))
        assert(bound.path == Path.Utf8("foobar"))

      case _ => fail()
    }
  }

  test("Namer.global: /$/fail") {
    assert(
      Namer.global.lookup(Path.read("/$/fail")).sample()
        == NameTree.Fail
    )
    assert(
      Namer.global.lookup(Path.read("/$/fail/foo/bar")).sample()
        == NameTree.Fail
    )
  }

  test("Namer.global: /$/nil") {
    assert(
      Namer.global.lookup(Path.read("/$/nil")).sample()
        == NameTree.Empty
    )
    assert(
      Namer.global.lookup(Path.read("/$/nil/foo/bar")).sample()
        == NameTree.Empty
    )
  }

  test("Namer.global: /$/{className}") {
    assert(
      Namer.global.lookup(Path.read("/$/com.twitter.finagle.TestNamer/foo")).sample()
        == NameTree.Leaf(Name.Path(Path.Utf8("bar")))
    )
  }

  test("Namer.global: /$/{className} ServiceNamer") {
    val dst = Path.read("/$/com.twitter.finagle.PathServiceNamer/foo")
    Namer.global.lookup(dst).sample() match {
      case NameTree.Leaf(bound: Name.Bound) =>
        assert(bound.path == Path.Utf8("foo"))
        bound.addr.sample() match {
          case bound: Addr.Bound =>
            assert(bound.addrs.size == 1)
            bound.addrs.head match {
              case Address.ServiceFactory(sf, _) =>
                val svc = Await.result(sf.asInstanceOf[ServiceFactory[Path, Path]](), 5.seconds)
                val rsp = Await.result(svc(Path.Utf8("yodles")), 5.seconds)
                assert(rsp == Path.Utf8("foo", "yodles"))

              case addr =>
                fail(s"$addr not a Address.ServiceFactory")
            }
          case x => throw new MatchError(x)
        }
      case nt =>
        fail(s"$nt is not NameTree.Leaf")
    }
  }

  test("Namer.global: /$/{className} ServiceNamer of incompatible type raises ClassCastException") {
    val dst = Path.read("/$/com.twitter.finagle.PathServiceNamer/foo")
    Namer.global.lookup(dst).sample() match {
      case NameTree.Leaf(bound: Name.Bound) =>
        assert(bound.path == Path.Utf8("foo"))
        bound.addr.sample() match {
          case bound: Addr.Bound =>
            assert(bound.addrs.size == 1)
            bound.addrs.head match {
              case Address.ServiceFactory(sf, _) =>
                val svc = Await.result(sf.asInstanceOf[ServiceFactory[Int, Int]](), 5.seconds)
                intercept[ClassCastException] {
                  val rsp = Await.result(svc(3), 5.seconds)
                }

              case addr =>
                fail(s"$addr not a Address.ServiceFactory")
            }
          case x => throw new MatchError(x)
        }
      case nt =>
        fail(s"$nt is not NameTree.Leaf")
    }
  }

  test("Namer.global: negative resolution") {
    assert(
      Namer.global.lookup(Path.read("/foo/bar/bah/blah")).sample()
        == NameTree.Neg
    )
    assert(
      Namer.global.lookup(Path.read("/foo/bar")).sample()
        == NameTree.Neg
    )
  }

  test("Namer.resolve") {
    assert(Namer.resolve("invalid").sample() match {
      case Addr.Failed(_: IllegalArgumentException) => true
      case _ => false
    })
  }

  test("Namer.resolve: No resolution for empty Dtab.base") {
    Dtab.unwind {
      Namer.resolve(Path.read("/s/foo")).sample() match {
        case Addr.Neg =>
          () // pass
        case _ => fail()
      }
    }
  }

  test("Namer.resolve: resolve when Dtab.local is set") {
    Dtab.unwind {
      Dtab.limited = Dtab.empty
      Dtab.local = Dtab.read("/s/foo => /$/inet/5678")

      Namer.resolve(Path.read("/s/foo")).sample() match {
        case Addr.Bound(addr, _) =>
          // pass
          assert(addr == Set(Address(5678)))
        case e => fail(e.toString)
      }
    }
  }

  test("Namer.resolve: resolve when Dtab.limited is set") {
    Dtab.unwind {
      Dtab.limited = Dtab.read("/s/foo => /$/inet/1234")
      Dtab.local = Dtab.empty

      Namer.resolve(Path.read("/s/foo")).sample() match {
        case Addr.Bound(addr, _) =>
          assert(addr == Set(Address(1234)))
        case _ => fail()
      }
    }
  }

  test(
    "Namer.resolve: resolve prefers Dtab.local when both Dtab.local and Dtab.limited" +
      " are set for same Path") {
    Dtab.unwind {
      Dtab.limited = Dtab.read("/s/foo => /$/inet/1234")
      Dtab.local = Dtab.read("/s/foo => /$/inet/5678")

      Namer.resolve(Path.read("/s/foo")).sample() match {
        case Addr.Bound(addr, _) =>
          assert(addr == Set(Address(5678)))
        case _ => fail()
      }
    }
  }

  test("Namer.bind: max recursion level reached") {
    namerMaxDepth.let(2) {
      assert(Namer.resolve(Dtab.read("/s => /s/srv"), Path.read("/s/foo/bar")).sample() match {
        case Addr.Failed(exception: Exception) =>
          assert(exception.getMessage === "Max recursion level: 2 reached in Namer lookup")
          true
        case _ => false
      })
    }
  }
}

class TestNamer extends Namer {
  def lookup(path: Path): Activity[NameTree[Name]] =
    Activity.value(path match {
      case Path.Utf8("foo") => NameTree.Leaf(Name.Path(Path.Utf8("bar")))
      case _ => NameTree.Neg
    })
}

class PathServiceNamer extends ServiceNamer[Path, Path] {
  def lookupService(pfx: Path) = {
    val svc = Service.mk[Path, Path] { req => Future.value(pfx ++ req) }
    Some(svc)
  }
}
