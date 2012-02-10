package com.twitter.finagle.builder

import org.specs.Specification
import org.specs.mock.Mockito
import com.twitter.concurrent.Spool
import com.twitter.util.{Return, Promise}
import collection.mutable.HashSet

object ClusterSpec extends Specification with Mockito {
  class WrappedInt (val value: Int) {
  }

  class ClusterInt extends Cluster[Int] {
    var set = HashSet.empty[Int]
    var changes = new Promise[Spool[Cluster.Change[Int]]]

    def add (value: Int) = {
      set += value
      performChange(Cluster.Add(value))
    }

    def del (value: Int) = {
      set -= value
      performChange(Cluster.Rem(value))
    }

    private[this] def performChange (change: Cluster.Change[Int]) = {
      val newTail = new Promise[Spool[Cluster.Change[Int]]]
      changes() = Return(change *:: newTail)
      changes = newTail
    }

    def snap = (set.toSeq, changes)
  }

  "Cluster map" should {
    val N = 10
    val cluster1 = new ClusterInt()
    val cluster2 = cluster1.map(a => new WrappedInt(a))

    "provide 1-1 mapping to the result cluster" in {
      0 until N foreach { cluster1.add(_) }
      val (seq, changes) = cluster2.snap
      var set = seq.toSet
      changes foreach { spool =>
        spool foreach {
          case Cluster.Add(elem) => set += elem
          case Cluster.Rem(elem) => set -= elem
        }
      }
      set.size must be_==(N)
      0 until N foreach { cluster1.del(_) }
      set.size must be_==(0)
    }
  }
}