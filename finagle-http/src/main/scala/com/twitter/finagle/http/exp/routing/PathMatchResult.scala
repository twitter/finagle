package com.twitter.finagle.http.exp.routing

/** The result type when executing a [[PathMatcher]]. */
private[routing] sealed abstract class PathMatchResult

/** A result that represents that no match was found. */
private[routing] object NoPathMatch extends PathMatchResult

/**
 * A result representing that a match was found for a constant [[Path]],
 * which contains no [[Segment.Parameterized parameterized segments]].
 */
private[routing] object ConstantPathMatch extends PathMatchResult

/**
 * A result representing that a match was found for a dynamic [[Path]],
 * which contains the [[ParameterMap parameters]] and their [[ParameterValue values]] that
 * were extracted during the matching process.
 *
 * @param parameters The [[ParameterMap parameters]] extracted as part of matching an input
 *                   against a dynamic [[Path]].
 */
private[routing] final case class ParameterizedPathMatch(parameters: ParameterMap)
    extends PathMatchResult
