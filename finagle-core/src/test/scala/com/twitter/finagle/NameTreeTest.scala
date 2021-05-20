package com.twitter.finagle

import com.twitter.finagle.naming.{DefaultInterpreter, NamerExceededMaxDepthException}
import scala.util.Random
import scala.util.control.NonFatal
import org.scalatest.funsuite.AnyFunSuite

class NameTreeTest extends AnyFunSuite {
  val rng = new Random(1234L)

  val words = Seq(
    "Lorem",
    "ipsum",
    "dolor",
    "sit",
    "amet",
    "consectetur",
    "adipisicing",
    "elit",
    "sed",
    "do",
    "eiusmod",
    "tempor",
    "incididunt",
    "ut",
    "labore",
    "et",
    "dolore",
    "magna",
    "aliqua",
    "Ut",
    "enim",
    "ad",
    "minim",
    "veniam",
    "quis",
    "nostrud",
    "exercitation",
    "ullamco",
    "laboris",
    "nisi",
    "ut",
    "aliquip",
    "ex",
    "ea",
    "commodo",
    "consequat",
    "Duis",
    "aute",
    "irure",
    "dolor",
    "in",
    "reprehenderit",
    "in",
    "voluptate",
    "velit",
    "esse",
    "cillum",
    "dolore",
    "eu",
    "fugiat",
    "nulla",
    "pariatur",
    "Excepteur",
    "sint",
    "occaecat",
    "cupidatat",
    "non",
    "proident",
    "sunt",
    "in",
    "culpa",
    "qui",
    "officia",
    "deserunt",
    "mollit",
    "anim",
    "id",
    "est",
    "laborum"
  )

  def pick[T](xs: Seq[T]): T = xs(rng.nextInt(xs.length))

  test("NameTree.{read,show}") {
    def newPath(): NameTree[Path] = {
      val elems = Seq.fill(1 + rng.nextInt(10)) { pick(words) }
      NameTree.Leaf(Path.Utf8(elems: _*))
    }

    val leaves = Seq[() => NameTree[Path]](
      () => NameTree.Fail,
      () => NameTree.Empty,
      () => NameTree.Neg,
      () => newPath()
    )

    def newLeaf(): NameTree[Path] = pick(leaves).apply()

    def newTree(depth: Int): NameTree[Path] =
      rng.nextInt(3) match {
        case _ if depth == 0 => newLeaf()
        case 0 => newLeaf()

        case 1 =>
          val trees = Seq.fill(1 + rng.nextInt(3)) {
            // TODO(jdonham) test fractional weights
            val weight = rng.nextInt(10).toDouble
            NameTree.Weighted(weight, newTree(depth - 1))
          }
          if (trees.size == 1)
            trees(0).tree
          else
            NameTree.Union(trees: _*)

        case 2 =>
          val trees = Seq.fill(1 + rng.nextInt(3)) { newTree(depth - 1) }
          if (trees.size == 1)
            trees(0)
          else
            NameTree.Alt(trees: _*)
      }

    val trees = Seq.fill(100) { newTree(2) }
    for (tree <- trees)
      try {
        assert(NameTree.read(tree.show) == tree)
      } catch {
        case NonFatal(exc) =>
          fail("Exception %s while parsing %s: %s".format(exc, tree.show, tree))
      }
  }

  test("NameTree.bind: infinite loop") {
    val dtab = Dtab.read("""
      /foo/bar => /bar/foo;
      /bar/foo => /foo/bar""")

    intercept[NamerExceededMaxDepthException] {
      DefaultInterpreter.bind(dtab, Path.read("/foo/bar")).sample()
    }
  }

  test("NameTree.eval/simplified") {
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
      "(~|$|/foo) & (/bar|/blah) & ~ & /FOO" -> Some(Set("/bar", "/FOO")),
      "! | /ok" -> None,
      "/ok & !" -> Some(Set("/ok")),
      "! & ! & !" -> None,
      "~ | /ok | !" -> Some(Set("/ok"))
    )

    for ((tree, res) <- cases) {
      val expect = res map { set => set map { el: String => Path.read(el) } }

      assert(NameTree.read(tree).eval == expect)
      assert(NameTree.read(tree).simplified.eval == expect)
    }
  }
}
