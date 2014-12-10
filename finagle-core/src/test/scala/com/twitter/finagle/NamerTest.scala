package com.twitter.finagle

import com.twitter.util.{Return, Throw, Activity, Witness, Try}
import java.net.{InetSocketAddress, SocketAddress}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}
import scala.language.reflectiveCalls

@RunWith(classOf[JUnitRunner])
class NamerTest extends FunSuite with AssertionsForJUnit {
  trait Ctx {
    case class OrElse(fst: Namer, snd: Namer) extends Namer {
      def lookup(path: Path): Activity[NameTree[Name]] =
        (fst.lookup(path) join snd.lookup(path)) map {
          case (left, right) => NameTree.Alt(left, right)
        }
      def enum(prefix: Path): Activity[Dtab] =
        (fst.enum(prefix) join snd.enum(prefix)) map {
          case (left, right) => left.alt(right)
        }
    }

    def ia(i: Int) = new InetSocketAddress(i)

    val exc = new Exception {}

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
          case p@Path.Utf8(elems@_*) =>
            acts.get(p) match {
              case Some((a, _)) => a map { tree => tree.map(Name(_)) }
              case None =>
                val (act, _) = addPath(p)
                act map { tree => tree.map(Name(_)) }
            }
          case _ => Activity.value(NameTree.Neg)
        }

        def enum(prefix: Path): Activity[Dtab] = Activity.exception(new UnsupportedOperationException)
      }

      val namer = OrElse(pathNamer, Namer.global)
      def lookup(path: Path) = namer.lookup(path)
      def enum(prefix: Path): Activity[Dtab] = namer.enum(prefix)
    }
  }

  def assertEval(res: Activity[NameTree[Name.Bound]], ias: InetSocketAddress*) {
    res.sample().eval match {
      case Some(actual) => assert(actual.map(_.addr.sample) === ias.map(Addr.Bound(_)).toSet)
      case _ => assert(false)
    }
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

  test("NameTree.bind: Alt with Fail/Empty")(new Ctx {
    assert(namer.bind(NameTree.read("(! | /test/1 | /test/2)")).sample() == NameTree.Fail)
    assert(namer.bind(NameTree.read("(~ | /$/fail | /test/1)")).sample() == NameTree.Fail)
    assert(namer.bind(NameTree.read("(/$/nil | /$/fail | /test/1)")).sample() == NameTree.Empty)
  })

  def assertLookup(path: String, ias: SocketAddress*) {
    Namer.global.lookup(Path.read(path)).sample() match {
      case NameTree.Leaf(Name.Bound(addr)) => assert(addr.sample() === Addr.Bound(ias.toSet))
      case _ => assert(false)
    }
  }

  test("Namer.global: /$/inet") {
    assertLookup("/$/inet/1234", new InetSocketAddress(1234))
    assertLookup("/$/inet/127.0.0.1/1234", new InetSocketAddress("127.0.0.1", 1234))

    intercept[ClassNotFoundException] {
      Namer.global.lookup(Path.read("/$/inet")).sample()
    }

    intercept[ClassNotFoundException] {
      Namer.global.lookup(Path.read("/$/inet/1234/foobar")).sample()
    }
  }

  test("Namer.global: /$/fail") {
    assert(Namer.global.lookup(Path.read("/$/fail")).sample()
      === NameTree.Fail)
    assert(Namer.global.lookup(Path.read("/$/fail/foo/bar")).sample()
      === NameTree.Fail)
  }

  test("Namer.global: /$/nil") {
    assert(Namer.global.lookup(Path.read("/$/nil")).sample()
        === NameTree.Empty)
    assert(Namer.global.lookup(Path.read("/$/nil/foo/bar")).sample()
        === NameTree.Empty)
  }

  test("Namer.global: /$/{className}") {
    assert(Namer.global.lookup(Path.read("/$/com.twitter.finagle.TestNamer/foo")).sample()
      === NameTree.Leaf(Name.Path(Path.Utf8("bar"))))
  }

  test("Namer.global: negative resolution") {
    assert(Namer.global.lookup(Path.read("/foo/bar/bah/blah")).sample()
        === NameTree.Neg)
    assert(Namer.global.lookup(Path.read("/foo/bar")).sample()
        === NameTree.Neg)
  }

  test("Namer.expand") {
    def assertExpand(dtab: String, path: String, expected: String) {
      val expanded = Dtab.read(dtab).expand(Path.read(path)).sample
      assert(Equiv[Dtab].equiv(expanded, Dtab.read(expected)),
        "Expanded dtab \"%s\" does not match expected dtab \"%s\"".format(
          expanded.show, Dtab.read(expected).show))
    }

    assertExpand("""
      /x => /foo;
      /x/1 => /xx/1;
      /x/2 => /xx/2;
      /foo => /y;
      /y/1 => /yy/1;
      /y/3 => /yy/3
    """, "/x", """
      /1=>/yy/1;
      /3=>/yy/3;
      /1=>/xx/1;
      /2=>/xx/2
    """)

    assertExpand("""
      /x => /foo & /bar;
      /foo/1 => /foo1;
      /foo/2 => /foo2;
      /bar/1 => /bar1;
      /bar/3 => /bar3
    """, "/x", """
      /1=>/foo1&/bar1;
      /2=>/foo2;
      /3=>/bar3
      """)
  }

  test("Namer.resolve") {
    assert(Namer.resolve("invalid").sample() match {
      case Addr.Failed(_: IllegalArgumentException) => true
      case _ => false
    })
  }
}

class TestNamer extends Namer {
  def lookup(path: Path): Activity[NameTree[Name]] =
    Activity.value(
      path match {
        case Path.Utf8("foo") => NameTree.Leaf(Name.Path(Path.Utf8("bar")))
        case _ => NameTree.Neg
      })

  def enum(prefix: Path): Activity[Dtab] =
    Activity.exception(new UnsupportedOperationException)
}
