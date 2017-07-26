package com.twitter.finagle.serverset2.naming

import com.twitter.finagle.{Path, Name, Namer, NameTree}
import com.twitter.util.Activity

/** A namer that maps over each bound leaf resolved by another namer. */
abstract class TransformingNamer(underlying: Namer) extends Namer {
  override def lookup(path: Path): Activity[NameTree[Name]] = {
    underlying.lookup(path).map { tree =>
      tree.map {
        case bound: Name.Bound => transform(bound)
        case name => name
      }
    }
  }

  protected def transform(bound: Name.Bound): Name.Bound
}
