package com.twitter.finagle

import collection.mutable
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GroupTest extends FunSuite {
  class Ctx {
    val group = Group.mutable[Int]()
    var mapped = mutable.Buffer[Int]()
    val derived = group map { i =>
      mapped += i
      i+1
    }
  }

  test("map each value once, lazily and persistently") {
    val ctx = new Ctx
    import ctx._

    assert(mapped.isEmpty)
    group() = Set(1, 2)
    assert(mapped.isEmpty)
    assert(derived() == Set(2, 3))
    assert(Set(mapped:_*) == Set(1, 2))
    assert(derived() eq derived())

    group() = Set(1, 2, 3)
    assert(derived() == Set(2, 3, 4))
    assert(Set(mapped:_*) == Set(1, 2, 3))
    assert(derived() eq derived())
  }

  test("remap elements that re-appear") {
    val ctx = new Ctx
    import ctx._

    group() = Set(1, 2)
    assert(derived() == Set(2, 3))
    group() = Set(2)
    assert(derived() == Set(3))
    group() = Set(1, 2)
    assert(derived() == Set(2, 3))
    assert(Set(mapped:_*) == Set(1, 2))
  }

  test("collect") {
    val ctx = new Ctx
    import ctx._

    val group2 = group collect { case i if i % 2 == 0 => i * 2 }
    assert(group2().isEmpty)
    group() = Set(1, 2, 3, 4, 5)
    assert(group2() == Set(4, 8))
    val snap = group2()
    group() = Set(1, 3, 4, 5, 6)
    assert((group2() &~ snap) == Set(12))
    assert((snap &~ group2()) == Set(4))

    // Object identity:
    assert(group2() eq group2())
  }

  test("convert from builder group") {
    val bc = builder.StaticCluster(Seq(1, 2, 3, 4))
    val group = Group.fromCluster(bc)
    assert(group() == Set(1, 2, 3, 4))
    assert(group() eq group())
  }

  test("convert from dynamic builder group") {
    val bc = new builder.ClusterInt
    val group = Group.fromCluster(bc)

    assert(group().isEmpty)
    bc.add(1)
    assert(group() == Set(1))
    bc.del(1)
    assert(group().isEmpty)
    bc.add(1)
    assert(group() == Set(1))
    bc.add(1)
    assert(group() == Set(1))
    bc.add(2)
    assert(group() == Set(1, 2))
  }

  test("combined groups") {
    val combined = Group[Int](1, 2) + Group[Int](3, 4) + Group[Int](5, 6)
    assert(combined.members == Set(1, 2, 3, 4, 5, 6))
    assert(combined.members == combined.members)
  }

  test("object identity") {
    val g = Group.mutable[Object]()
    g() += new Object {}
    g() += new Object {}

    assert(g.members.size == 2)
    assert(g() eq g())
    val snap = g()
    g() += new Object {}
    assert(g() ne snap)
    assert(g() eq g())
  }
}
