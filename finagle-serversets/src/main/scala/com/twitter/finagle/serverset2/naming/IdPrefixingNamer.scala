package com.twitter.finagle.serverset2.naming

import com.twitter.finagle.{Path, Name, Namer}

/** A namer that prefixes each bound name's id with a prefix. */
class IdPrefixingNamer(idPrefix: Path, underlying: Namer) extends TransformingNamer(underlying) {

  protected def transform(bound: Name.Bound): Name.Bound = bound.id match {
    case id: Path => Name.Bound(bound.addr, idPrefix ++ id, bound.path)
    case _ => bound
  }
}
