package com.twitter.finagle

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.Builder
import scala.collection.immutable.VectorBuilder
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
  implicit val canBuildFrom: CanBuildFrom[TraversableOnce[Dentry], Dentry, Dtab] =
  new CanBuildFrom[TraversableOnce[Dentry], Dentry, Dtab] {
    def apply(_ign: TraversableOnce[Dentry]): DtabBuilder = newBuilder
    def apply(): DtabBuilder = newBuilder
  }

}

final class DtabBuilder extends Builder[Dentry, Dtab] {
  private[this] val builder = new VectorBuilder[Dentry]

  def +=(d: Dentry): this.type = {
    builder += d
    this
  }

  def clear(): Unit = builder.clear()

  def result(): Dtab = Dtab(builder.result)
}