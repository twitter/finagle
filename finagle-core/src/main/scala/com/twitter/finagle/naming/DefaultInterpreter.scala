package com.twitter.finagle.naming

import com.twitter.finagle._
import com.twitter.util.Activity

/**
 * Interpret names against the Dtab, using the default evaluation
 * strategy. Recursively look up and rewrite paths according to entries
 * matching the path prefix in the Dtab. If a path does not match any
 * Dtab entry prefix, the global Namer is invoked to resolve it. This is
 * how paths starting with `/$/` are bound.
 */
object DefaultInterpreter extends NameInterpreter {

  override def bind(dtab: Dtab, path: Path): Activity[NameTree[Name.Bound]] = {
    def lookup(path: Path): Activity[NameTree[Name]] =
      dtab.lookup(path) match {
        case NameTree.Neg => Namer.global.lookup(path)
        case t => Activity.value(t)
      }

    Namer.bind(lookup, NameTree.Leaf(path))
  }
}
