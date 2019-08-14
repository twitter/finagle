package com.twitter.finagle

import scala.collection.immutable.VectorBuilder
import scala.collection.mutable.ReusableBuilder

/**
 * A Dtab--short for delegation table--comprises a sequence of
 * delegation rules. Together, these describe how to bind a
 * [[com.twitter.finagle.Path]] to a set of
 * [[com.twitter.finagle.Addr]]. [[com.twitter.finagle.naming.DefaultInterpreter]]
 * implements the default binding strategy.
 *
 * @see The [[https://twitter.github.io/finagle/guide/Names.html#interpreting-paths-with-delegation-tables user guide]]
 *      for further details.
 */
case class Dtab(dentries0: IndexedSeq[Dentry]) extends DtabBase

object Dtab extends DtabCompanionBase {
  
}

final class DtabBuilder extends ReusableBuilder[Dentry, Dtab] {
  private[this] val builder = new VectorBuilder[Dentry]

  def addOne(d: Dentry): this.type = {
    builder += d
    this
  }

  def clear(): Unit = builder.clear()

  def result(): Dtab = Dtab(builder.result)
}
