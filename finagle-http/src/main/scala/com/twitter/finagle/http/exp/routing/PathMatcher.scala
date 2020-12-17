package com.twitter.finagle.http.exp.routing

import com.twitter.finagle.http.exp.routing.Segment.{Constant, Parameterized, Slash}
import scala.annotation.tailrec

// note: The below methods which result in returning a single String index use -1 as a magic number
//       to signify when a match cannot be found, which is non-idiomatic Scala. These functions will
//       be used on that hot path and we want to avoid allocations of the more idiomatic Option[Int].
//       Apologies in advance for the negative impact on code readability.
private[routing] object PathMatcher {

  // exposed for testing, should be treated as `private` to PathMatcher
  private[routing] sealed abstract class ParameterParseResult

  // exposed for testing, should be treated as `private` to PathMatcher
  private[routing] object UnableToParse extends ParameterParseResult

  // exposed for testing, should be treated as `private` to PathMatcher
  private[routing] final case class ParsedValue(nextScanIndex: Int, value: ParameterValue)
      extends ParameterParseResult

  // exposed for testing, should be treated as `private` to PathMatcher
  private[routing] sealed abstract class SegmentBoundaryResult

  // exposed for testing, should be treated as `private` to PathMatcher
  private[routing] object NoBoundary extends SegmentBoundaryResult

  // exposed for testing, should be treated as `private` to PathMatcher
  private[routing] final case class Boundary(startIdx: Int, nextIdx: Int)
      extends SegmentBoundaryResult

  private[routing] def requireNonNegativeIdx(idx: Int): Unit =
    require(idx >= 0, "idx cannot be negative")

  /**
   * Search for the case-sensitive contents of find in str.
   *
   * @param str The String to search.
   * @param idx The offset within str to start the search
   * @param find The String to verify exists within str.
   * @return The index in str following the characters in find if find exists within str,
   *         -1 otherwise.
   *
   * @note package scope exposed for testing
   */
  private[routing] def indexAfterRegionMatch(str: String, idx: Int, find: String): Int = {
    requireNonNegativeIdx(idx)
    val len = find.length
    val matches = str.regionMatches(false, idx, find, 0, len)
    if (matches) idx + len
    else -1
  }

  /**
   * Search for a '/' (forward-slash) separator in a String at an index. If a slash is found at the
   * index, this method will return the index of the character after the last consecutive '/'
   * character, starting at the index.
   *
   * @example {{{
   *            str -> '/http//123'
   *            idx -> 5
   *            return 7
   * }}}
   *
   * @example {{{
   *            str -> '/http'
   *            idx -> 3
   *            return -1
   * }}}
   *
   * @example {{{
   *            str -> '/http'
   *            idx -> 0
   *            return 1
   * }}}
   *
   * @param str The String to verify contains a '/' at idx.
   * @param idx The index of str to verify contains a '/'.
   * @return if '/' does not exists at idx of str: -1
   *         otherwise the index after the last consecutive '/' character starting at idx in
   *         str.
   *
   * @note package scope exposed for testing
   */
  private[routing] def indexAfterMatchingSlash(str: String, idx: Int): Int = {
    requireNonNegativeIdx(idx)
    val len = str.length
    var checkForSlash = idx
    while (checkForSlash < len && str.charAt(checkForSlash) == '/') {
      checkForSlash += 1
    }
    if (checkForSlash == idx) -1
    else checkForSlash
  }

  /**
   * Attempt to extract a [[ParameterValue]] based on the [[Parameterized parameterized segment]].
   *
   * @param str The String to search.
   * @param idx The starting index within str to begin the search.
   * @param param The [[Parameterized parameterized segment]] to match at the idx of str.
   * @param segmentAfterParam The [[Segment segment]] to find following the [[Parameterized start segment]].
   * @return [[UnableToParse]] if no matching segment could be extracted,
   *         otherwise ParsedValue(index of *after* the extracted parameter of str, the extracted parameter value).
   *
   * @note The [[ParsedValue.nextScanIndex]] will be greater then the search String's length if the
   *       segmentAfterParam is located at the end of the [[Path path's]] defined [[Segment segments]]
   *       (see the second example below).
   *
   * @example {{{
   *            str -> "/users/123/abc"
   *            idx -> 7
   *            param -> IntParam("id")
   *            segmentAfterParam -> Slash
   *            return -> ParsedValue(11, IntValue("123", 123))
   * }}}
   *
   * @example {{{
   *            str -> "/users/123.json"
   *            idx -> 7
   *            param -> IntParam("id")
   *            segmentAfterParam -> Constant(".json")
   *            return -> ParsedValue(15, IntValue("123", 123))
   * }}}
   *
   * @example {{{
   *            str -> "/users/123.xml"
   *            idx -> 7
   *            param -> IntParam("id")
   *            segmentAfterParam -> Constant(".json")
   *            return -> UnableToParse
   * }}}
   *
   * @note package scope exposed for testing
   */
  private[routing] def parameterized(
    str: String,
    idx: Int,
    param: Parameterized,
    segmentAfterParam: Segment
  ): ParameterParseResult = {
    requireNonNegativeIdx(idx)
    segmentBoundary(str, idx, segmentAfterParam) match {
      case Boundary(paramEnd, nextSegmentEnd) =>
        param.parse(str.substring(idx, paramEnd)) match {
          case Some(paramValue) =>
            ParsedValue(nextSegmentEnd, paramValue)
          case _ =>
            UnableToParse
        }
      case NoBoundary =>
        UnableToParse
    }
  }

  /**
   * Attempt to extract a [[ParameterValue]] based on the [[Parameterized parameterized segment]]
   * when there are no remaining segments after param to match.
   *
   * @param str The String to search.
   * @param idx The starting index within str to begin the search.
   * @param param The [[Parameterized parameterized segment]] to match at the idx of str.
   * @return None if no matching segment could be extracted
   *         Some(ParameterValue) otherwise
   *
   * @example {{{
   *            str -> "/users/123-abc"
   *            idx -> 7
   *            param -> StringParam("id")
   *            return -> Some(StringValue("123-abc"))
   * }}}
   *
   * @example {{{
   *            str -> "/users/123/abc"
   *            idx -> 7
   *            param -> StringParam("id")
   *            return -> None
   * }}}
   *
   * @example {{{
   *            str -> "/users/123"
   *            idx -> 7
   *            param -> IntParam("id")
   *            return -> Some(IntValue("123", 123))
   * }}}
   *
   * @example {{{
   *            str -> "/users/123-abc"
   *            idx -> 7
   *            param -> IntParam("id")
   *            return -> None
   * }}}
   *
   * @note package scope exposed for testing
   */
  private[routing] def parameterizedEnd(
    str: String,
    idx: Int,
    param: Parameterized
  ): Option[ParameterValue] = {
    requireNonNegativeIdx(idx)
    if (str.length <= idx) None
    else if (str.indexOf('/', idx) >= 0) None // TODO - spec compliant, but *will* break things
    else param.parse(str.substring(idx))
  }

  /**
   * Ensure that all of the [[requiredParamNames required parameter names]] are present as
   * keys within extracted.
   */
  private[routing] def verifyRequiredParams(
    requiredParamNames: Set[String],
    extracted: Map[String, ParameterValue]
  ): Boolean = requiredParamNames.forall(extracted.contains)

  /**
   * Look for find in str starting at index idx, where the starting index
   * of find within str will be returned if it is found, -1 otherwise.
   */
  private[this] def nextConstantIdx(str: String, idx: Int, find: String): Int = {
    requireNonNegativeIdx(idx)
    if (idx >= str.length) -1
    else str.indexOf(find, idx)
  }

  /**
   * Find the start and end index boundaries of a [[Segment segment]] within a String,
   * offset by an index.
   *
   * @param str The String to search for a match.
   * @param idx The index of str to start the search from.
   * @param segment The segment to match against.
   *
   * @return Boundary(start, end)) where the end is exclusive, if the segment boundary is found.
   *         NoBoundary otherwise.
   *
   * @example {{{
   *            str -> /abc///xyz
   *            idx -> 2
   *            segment -> Slash
   *            return -> Boundary(4, 7)
   * }}}
   * @example {{{
   *            str -> /abc.json
   *            idx -> 2
   *            segment -> Constant(".json")
   *            return -> Boundary(4, 9)
   * }}}
   * @example {{{
   *            str -> /abc-1234
   *            idx -> 1
   *            segment -> Slash
   *            return -> NoBoundary
   * }}}
   * @note package scope exposed for testing
   */
  private[routing] def segmentBoundary(
    str: String,
    idx: Int,
    segment: Segment
  ): SegmentBoundaryResult = {
    requireNonNegativeIdx(idx)
    segment match {
      case Segment.Slash =>
        val slashIdx = str.indexOf('/', idx)
        if (slashIdx >= 0) Boundary(slashIdx, indexAfterMatchingSlash(str, slashIdx))
        else NoBoundary
      case Segment.Constant(path) =>
        val constIdx = nextConstantIdx(str, idx, path)
        if (constIdx >= 0) Boundary(constIdx, constIdx + path.length) else NoBoundary
      case _: Segment.Parameterized =>
        throw new IllegalArgumentException(
          "A Parameterized segment does not have a defined boundary")
      case unknown =>
        throw new IllegalArgumentException(s"Unexpected segment type '$unknown'")
    }
  }
}

/**
 * A [[PathMatcher]] takes a defined [[Path]] and attempts to match that path against a String.
 * If the String matches the [[Path]], Some([[ParameterMap]]) will be returned with any required
 * extracted values. If the String does not match the [[Path]], None will be returned.
 */
private[routing] sealed abstract class PathMatcher extends ((Path, String) => PathMatchResult)

/**
 * A matcher that will iterate through a full [[Path path's]] [[Segment segments]] when attempting
 * to match an input.
 *
 * @note This is a simple reference implementation for matching against a spec. A Trie based
 *       implementation may be preferred for performance, but is too complex as an initial solution.
 */
private[routing] final object LinearPathMatcher extends PathMatcher {
  import PathMatcher._

  override def apply(
    path: Path,
    str: String
  ): PathMatchResult =
    if (path.isConstant) {
      // a constant path is a simpler string comparison, there will never be parameterized values
      if (matchConstant(path.segments.iterator, str)) ConstantPathMatch else NoPathMatch
    } else {
      // otherwise we need to ensure we extract all of the parameter values
      val params: Map[String, ParameterValue] =
        extractMatchingParameterValues(path.segments.iterator, str)
      val paramsValid: Boolean = verifyRequiredParams(path.requiredParamNames, params)
      if (paramsValid) {
        ParameterizedPathMatch(MapParameterMap(params))
      } else {
        NoPathMatch
      }
    }

  // note: using @tailrec avoids writing a larger while loop with mutable state. we also avoid
  // needing to allocate a fully realized String representation of the Path in memory.
  @tailrec
  private[this] def matchConstant(iter: Iterator[Segment], str: String, idx: Int = 0): Boolean = {
    requireNonNegativeIdx(idx)
    if (iter.hasNext) {
      iter.next() match {
        case Segment.Slash =>
          val slashIdx = indexAfterMatchingSlash(str, idx)
          (slashIdx >= idx && matchConstant(iter, str, slashIdx))
        case Segment.Constant(segment) =>
          (indexAfterRegionMatch(str, idx, segment) >= idx && matchConstant(
            iter,
            str,
            idx + segment.length))
        case _: Segment.Parameterized =>
          throw new IllegalStateException("A constant path cannot have a Parameterized segment")
        case unknown =>
          throw new IllegalStateException(s"Encountered an unrecognized Segment type: $unknown")
      }
    } else {
      idx == str.length
    }
  }

  /**
   * Extract a `Map[String, ParameterValue]` from [[Path path]] [[Segment segments]] from a matching
   * input String.
   *
   * @param iter The remaining [[Segment segments]] of a [[Path]] that need to be scanned for
   *             a match against the input String.
   * @param str The String to match against.
   * @param idx The index within the String to start matching against.
   * @param parameters The `Map[String, ParameterValue]` of matching [[Segment.Parameterized]] values
   *                   that have been extracted so far
   * @return The `Map[String, ParameterValue]` of extracted values found upon scanning the complete
   *         input String. If the scan was incomplete and/or not all parameters were matched,
   *         [[Map.empty]] will be returned.
   */
  @tailrec
  // note: using @tailrec avoids writing a larger while loop with mutable state
  private[this] def extractMatchingParameterValues(
    iter: Iterator[Segment],
    str: String,
    idx: Int = 0,
    parameters: Map[String, ParameterValue] = Map.empty
  ): Map[String, ParameterValue] = {
    requireNonNegativeIdx(idx)
    if (idx < str.length && iter.hasNext) {
      val segment = iter.next()

      segment match {
        case Slash =>
          val found = indexAfterMatchingSlash(str, idx)
          if (found >= 0) extractMatchingParameterValues(iter, str, found, parameters)
          else Map.empty

        case Constant(path) =>
          val found = indexAfterRegionMatch(str, idx, path)
          if (found >= 0) extractMatchingParameterValues(iter, str, found, parameters)
          else Map.empty

        case param: Parameterized if iter.hasNext =>
          // if we have remaining segments, we need to find the next segment in order to extract
          // the dynamic value between segments

          val nextSegment = iter.next()

          // note: we use match instead of flatMap to keep extractSegments in tail position
          parameterized(str, idx, param, nextSegment) match {
            case ParsedValue(nextEnd, parameterValue) =>
              extractMatchingParameterValues(
                iter,
                str,
                nextEnd,
                parameters + (param.param.name -> parameterValue))
            case _ =>
              Map.empty
          }

        case param: Parameterized =>
          // otherwise there is no next segment and we have a trailing parameter to extract
          parameterizedEnd(str, idx, param) match {
            case Some(parameterValue) =>
              val updatedMap = parameters + (param.param.name -> parameterValue)
              updatedMap
            case _ =>
              Map.empty
          }

        case _ =>
          Map.empty
      }
    } else if (idx == str.length) {
      parameters
    } else {
      Map.empty
    }
  }

}

// TODO - TrieSpecMatcher
