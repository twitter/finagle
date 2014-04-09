package com.twitter.finagle

import com.twitter.util.NonFatal
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class NameTreeTest extends FunSuite {
  val rng = new Random(1234L)

  val words = Seq("Lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipisicing", 
    "elit", "sed", "do", "eiusmod", "tempor", "incididunt", "ut", "labore", "et", "dolore", 
    "magna", "aliqua", "Ut", "enim", "ad", "minim", "veniam", "quis", "nostrud", "exercitation", 
    "ullamco", "laboris", "nisi", "ut", "aliquip", "ex", "ea", "commodo", "consequat", "Duis", 
    "aute", "irure", "dolor", "in", "reprehenderit", "in", "voluptate", "velit", "esse", "cillum", 
    "dolore", "eu", "fugiat", "nulla", "pariatur", "Excepteur", "sint", "occaecat", "cupidatat", 
    "non", "proident", "sunt", "in", "culpa", "qui", "officia", "deserunt", "mollit", "anim", 
    "id", "est", "laborum", "")
    
  def pick[T](xs: Seq[T]): T = xs(rng.nextInt(xs.length))

  test("NameTree.{read,show}") {
    def splice(tree: NameTree[Path]): NameTree[Path] = tree match {
      case NameTree.Alt(tree) => splice(tree)
      case NameTree.Alt(trees@_*) =>
        val spliced = trees map splice flatMap {
          case NameTree.Alt(trees1@_*) => trees1
          case other => Seq(other)
        }
        NameTree.Alt(spliced:_*)
 
      case NameTree.Union(tree) => splice(tree)
      case NameTree.Union(trees@_*) =>
        val spliced = trees map splice flatMap {
          case NameTree.Union(trees1@_*) => trees1
          case other => Seq(other)
        }
        NameTree.Union(spliced:_*)

      case leaf => leaf
    }

    def newPath(): NameTree[Path] = {
      val elems = Seq.fill(1+rng.nextInt(10)) { pick(words) }
      NameTree.Leaf(Path.Utf8(elems:_*))
    }
    
    val leaves = Seq[() => NameTree[Path]](
      // Note: atoms don't have a syntactic component yet.
      //      () => NameTree.Atom(new SocketAddress{}),
      () => NameTree.Empty,
      () => NameTree.Neg,
      () => newPath())

    def newLeaf(): NameTree[Path] = pick(leaves).apply()

    def newTree(depth: Int): NameTree[Path] = 
      rng.nextInt(3) match {
        case _ if depth == 0 => newLeaf()
        case 0 => newLeaf()

        case 1 => 
          val trees = Seq.fill(1+rng.nextInt(3)) { newTree(depth-1) }
          NameTree.Union(trees:_*)

        case 2 =>
          val trees = Seq.fill(1+rng.nextInt(3)) { newTree(depth-1) }
          NameTree.Alt(trees:_*)
      }
      
    val trees = Seq.fill(100) { newTree(2) }
    for (tree <- trees)
      try { 
        assert(splice(NameTree.read(tree.show)) == splice(tree))
      } catch {
        case NonFatal(exc) =>
          fail("Exception %s while parsing %s: %s; spliced: %s".format(
            exc, tree.show, tree, splice(tree)))
      }
  }

  test("NameTree.bindAndEval: infinite loop") {
    val dtab = Dtab.read("""
      /foo/bar => /bar/foo;
      /bar/foo => /foo/bar""")

    val Addr.Failed(_: IllegalArgumentException) = 
      dtab.bindAndEval(NameTree.read("/foo/bar")).sample()
  }
  
  test("NameTree.eval") {
    val cases = Seq[(String, Option[Set[String]])](
      "~" -> None,
      "/ok" -> Some(Set("/ok")),
      "/ok & /ok1" -> Some(Set("/ok", "/ok1")),
      "/ok & ~ & /blah & $" -> Some(Set("/ok", "/blah")),
      "$ & $ & $" -> Some(Set.empty),
      "~ & ~ & (~ | ~)" -> None,
      "~ | /foo | /bar" -> Some(Set("/foo")),
      "~ | $ | /blah" -> Some(Set.empty),
      "~ | (~ | $) | /blah" -> Some(Set.empty),
      "(~|$|/foo) & (/bar|/blah) & ~ & /FOO" -> Some(Set("/bar", "/FOO"))
      )
      
    for ((tree, res) <- cases) {
      val expect = res map { set =>
        set map { el: String => Path.read(el) }
      }

      assert(NameTree.read(tree).eval === expect)
    }
  }
}
