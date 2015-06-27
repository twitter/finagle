package com.twitter.finagle.naming

import com.twitter.finagle._
import com.twitter.util.Activity

/**
 * Intepret names against a Dtab. Differs from
 * [[com.twitter.finagle.Namer Namers]] in that the passed in
 * [[com.twitter.finagle.Dtab Dtab]] can affect the resolution process.
 */
trait NameInterpreter {
  /**
   * Bind `path` against the given `dtab`.
   */
  def bind(dtab: Dtab, path: Path): Activity[NameTree[Name.Bound]]
}

object NameInterpreter extends NameInterpreter {

  /** The default interpreter. Bind `path` using `dtab` as a namer. */
  val default = new NameInterpreter {
    override def bind(dtab: Dtab, path: Path) = dtab.bind(NameTree.Leaf(path))
  }

  /**
   * The global interpreter that resolves all names in Finagle.
   *
   * Can be modified to provide a different mechanism for name resolution.
   */
  @volatile var global: NameInterpreter = default

  override def bind(dtab: Dtab, tree: Path): Activity[NameTree[Name.Bound]] =
    global.bind(dtab, tree)
}
