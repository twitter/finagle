package com.twitter.finagle.http.exp.routing

/**
 * Representation for an HTTP routing path
 *
 * @param segments The [[Segment segments]] that define this [[Path path]].
 */
private[http] final case class Path private[routing] (
  segments: Iterable[Segment]) {
  override def toString: String = segments.mkString
}
