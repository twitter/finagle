package com.twitter.finagle

import org.specs.SpecificationWithJUnit
import collection.mutable

class GroupSpec extends SpecificationWithJUnit {
  "Group" should {
    val group = Group.mutable[Int]()
    var mapped = mutable.Buffer[Int]()
    val derived = group map { i =>
      mapped += i
      i+1
    }

    "map each value once, lazily, and persistently" in {
      mapped must beEmpty
      group() = Set(1,2)
      mapped must beEmpty
      derived() must haveTheSameElementsAs(Seq(2,3))
      mapped must haveTheSameElementsAs(Seq(1,2))
      derived() must be(derived())

      group() = Set(1,2,3)
      mapped must haveSize(2)
      derived() must haveTheSameElementsAs(Seq(2,3,4))
      mapped must haveTheSameElementsAs(Seq(1,2,3))
      derived() must be(derived())
    }

    "remap elements that re-appear" in {
      group() = Set(1,2)
      derived() must haveTheSameElementsAs(Seq(2,3))
      group() = Set(2)
      derived() must haveTheSameElementsAs(Seq(3))
      group() = Set(1,2)
      derived() must haveTheSameElementsAs(Seq(2,3))
      mapped must haveTheSameElementsAs(Seq(1,2,1))
    }

    "collect" in {
      val group2 = group collect { case i if i%2==0 => i*2 }
      group2() must beEmpty
      group() = Set(1,2,3,4,5)
      group2() must be_==(Set(4,8))
      val snap = group2()
      group() = Set(1,3,4,5,6)
      (group2() &~ snap) must be_==(Set(12))
      (snap &~ group2()) must be_==(Set(4))
    }

    "convert from builder group" in {
      val bc = builder.StaticCluster(Seq(1,2,3,4))
      val group = Group.fromCluster(bc)
      group() must haveTheSameElementsAs(Seq(1,2,3,4))
      group() must be(group())
    }

    "convert from dynamic builder group" in {
      val bc = new builder.ClusterInt
      val group = Group.fromCluster(bc)

      group() must beEmpty
      bc.add(1)
      group() must haveTheSameElementsAs(Seq(1))
      bc.del(1)
      group() must beEmpty
      bc.add(1)
      group() must haveTheSameElementsAs(Seq(1))
      bc.add(1)
      group() must haveTheSameElementsAs(Seq(1))
      bc.add(2)
      group() must haveTheSameElementsAs(Seq(1,2))
    }
  }
}
