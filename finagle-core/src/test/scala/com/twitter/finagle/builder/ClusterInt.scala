package com.twitter.finagle.builder

import com.twitter.concurrent.Spool
import com.twitter.util.{Return, Promise}
import collection.mutable

case class WrappedInt(val value: Int)

class ClusterInt extends Cluster[Int] {
  var set = mutable.HashSet.empty[Int]
  var changes = new Promise[Spool[Cluster.Change[Int]]]

  def add(value: Int) = {
    set += value
    performChange(Cluster.Add(value))
  }

  def del(value: Int) = {
    set -= value
    performChange(Cluster.Rem(value))
  }

  private[this] def performChange(change: Cluster.Change[Int]) = {
    val newTail = new Promise[Spool[Cluster.Change[Int]]]
    changes() = Return(change *:: newTail)
    changes = newTail
  }

  def snap = (set.toSeq, changes)
}
