package com.twitter.finagle.http.exp.routing

import com.twitter.finagle.http.exp.routing.Path.{isParameterFn, parameterNameFn}
import com.twitter.finagle.http.exp.routing.Segment.Parameterized

object Path {
  // perf optimization to val up our functions
  private val isParameterFn: Segment => Boolean = _.isInstanceOf[Parameterized]
  private val parameterNameFn: PartialFunction[Segment, String] = {
    case Parameterized(p) => p.name
  }
}

/**
 * Representation for an HTTP routing path
 *
 * @param segments The [[Segment segments]] that define this [[Path path]].
 */
private[http] final case class Path(
  segments: Iterable[Segment]) {
  override def toString: String = segments.mkString

  /**
   * Are all [[Segment segments]] constant (i.e. not parameterized)? To be constant requires all
   * segments to be either [[Segment.Slash]] or [[Segment.Constant]]
   * and not [[Segment.Parameterized]].
   */
  private[routing] val isConstant: Boolean = !segments.exists(isParameterFn)

  private[routing] val requiredParamNames: Set[String] =
    if (isConstant) Set.empty
    else segments.collect(parameterNameFn).toSet
}
