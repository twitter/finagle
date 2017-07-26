package com.twitter.finagle.naming

import com.twitter.finagle._
import com.twitter.util.Activity

/**
 * Interpret names against a Dtab. Differs from
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

  /**
   * The global interpreter that resolves all names in Finagle.
   *
   * Can be modified to provide a different mechanism for name resolution.
   */
  @volatile var global: NameInterpreter = LoadedNameInterpreter

  /** Java API for setting the interpreter */
  def setGlobal(nameInterpreter: NameInterpreter): Unit =
    global = nameInterpreter

  def bind(dtab: Dtab, path: Path): Activity[NameTree[Name.Bound]] =
    global.bind(dtab, path)
}
