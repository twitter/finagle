package com.twitter.finagle.stats

import scala.annotation.tailrec

/**
 * Pretty damn fast Glob matching.
 */
private object Glob {

  /**
   * Glob matching made easy with:
   *
   * {{{
   *   val g = Glob("foo*bar,bar*foo")
   *
   *   g("foo/bar") // true
   *   g("bar/foo") // true
   *   g("foo")     // false
   *   g("bar")     // false
   * }}}
   *
   * Only two control characters are supported:
   *
   * `*` - wildcard (more than one subsequent asterisks are ignored)
   * `,` - or
   */
  def apply(glob: String)(input: String): Boolean = root(input, glob, 0)

  // Implementation details:
  //
  // This technique is known as top-down recursive descent parsing, which follows the grammar
  // (each function represents a grammar rule):
  //
  //   <root>     ::= <segment> | <wildcard> | ',' <root>
  //   <segment>  ::= CHAR+
  //   <wildcard> ::= '*'+
  //
  // Think of each function representing a state of the matcher (or parser), where:
  //
  //  - "root" means expecting a next comma-separated wildcard or a segment and
  //  - both "wildcard" and "segment" encode the actual matching logic and
  //  - "next" is used to fast-forward to the next comma-separated glob (next "root").
  //
  // An important implementation piece to understand is that each function either terminates
  // matching (with a successful match) or transitions to another state (or rule) based on the
  // given glob expression (what's being observed).
  //
  // Each function takes both `input` and `glob` strings and two indices (`i` and `j`) that
  // represent current positions in them.

  private def root(input: String, glob: String, j: Int): Boolean = {
    if (glob.length == j) false
    else if (glob.charAt(j) == ',') root(input, glob, j + 1)
    else if (glob.charAt(j) == '*') wildcard(input, 0, glob, j + 1)
    else segment(input, 0, glob, j)
  }

  private def wildcard(input: String, i: Int, glob: String, j: Int): Boolean = {
    @tailrec
    def loop(ii: Int): Boolean =
      if (ii == input.length) next(input, glob, j + 1)
      else if (segment(input, ii, glob, j)) true
      else loop(ii + 1)

    if (j == glob.length || glob.charAt(j) == ',') true
    else if (glob.charAt(j) == '*') wildcard(input, i, glob, j + 1)
    else loop(i)
  }

  private def segment(input: String, i: Int, glob: String, j: Int): Boolean = {
    @tailrec
    def loop(ii: Int, jj: Int): Boolean =
      if (jj == glob.length) ii == input.length
      else if (glob.charAt(jj) == ',') ii == input.length || root(input, glob, jj + 1)
      else if (glob.charAt(jj) == '*') wildcard(input, ii, glob, jj + 1)
      else if (ii < input.length && input.charAt(ii) == glob.charAt(jj)) loop(ii + 1, jj + 1)
      else next(input, glob, jj + 1)

    loop(i, j)
  }

  @tailrec
  private def next(input: String, glob: String, j: Int): Boolean =
    if (j == glob.length) false
    else if (glob.charAt(j) == ',') root(input, glob, j + 1)
    else next(input, glob, j + 1)
}
