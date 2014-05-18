package com.twitter.finagle.integration

import com.twitter.finagle.builder.Cluster
import com.twitter.concurrent.Spool
import com.twitter.util.{Return, Promise}

class DynamicCluster[U](initial: Seq[U])
  extends Cluster[U] {

  def this() = this(Seq[U]())

  var set = initial.toSet
  var s = new Promise[Spool[Cluster.Change[U]]]

  def add(f: U) = {
    set += f
    performChange(Cluster.Add(f))
  }

  def del(f: U) = {
    set -= f
    performChange(Cluster.Rem(f))
  }


  private[this] def performChange(change: Cluster.Change[U]) = synchronized {
    val newTail = new Promise[Spool[Cluster.Change[U]]]
    s() = Return(change *:: newTail)
    s = newTail
  }

  def snap = (set.toSeq, s)

}
