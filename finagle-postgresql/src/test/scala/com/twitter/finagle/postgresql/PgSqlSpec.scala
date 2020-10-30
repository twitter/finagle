package com.twitter.finagle.postgresql

import org.specs2.matcher.Expectable
import org.specs2.matcher.MatchResult
import org.specs2.matcher.Matcher
import org.specs2.matcher.describe.Diffable
import org.specs2.mutable.Specification
import org.specs2.specification.core.Fragment
import org.specs2.specification.core.Fragments

trait PgSqlSpec extends Specification with FutureResult {

  def using[I <: java.lang.AutoCloseable, T](io: => I)(f: I => T) = {
    val c = io
    try f(c)
    finally c.close()
  }

  def fragments(f: Seq[Fragment]) = Fragments(f: _*)

  /**
   * A Matcher that uses [[Diffable]]'s definition of being equal.
   *
   * This allows defining a custom equals definition.
   *
   * {{{
   *   implicit lazy val myDiff: Diffable[Complicated] = ???
   *
   *   val complicated: Complicated = ???
   *
   *   complicated must beIdentical(expected)
   * }}}
   */
  def beIdentical[V](v: V)(implicit d: Diffable[V]): Matcher[V] = new Matcher[V] {
    override def apply[S <: V](t: Expectable[S]): MatchResult[S] = {
      val diff = d.diff(v, t.value)
      result(diff.identical, s"${t.description} == '$v'", diff.render, t)
    }
  }

}
