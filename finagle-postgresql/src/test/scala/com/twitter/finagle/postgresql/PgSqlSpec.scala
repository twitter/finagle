package com.twitter.finagle.postgresql

import org.specs2.mutable.Specification
import org.specs2.specification.core.Fragment
import org.specs2.specification.core.Fragments

trait PgSqlSpec extends Specification with FutureResult {

  def using[I <: java.lang.AutoCloseable, T](io: => I)(f: I => T) = {
    val c = io
    try { f(c) } finally { c.close() }
  }

  def fragments(f: List[Fragment]) = Fragments(f: _*)
}
