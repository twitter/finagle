package com.twitter.finagle.postgresql

import org.specs2.mutable.Specification
import org.specs2.specification.core.Fragment
import org.specs2.specification.core.Fragments

trait PgSqlSpec extends Specification with FutureResult {
  def fragments(f: List[Fragment]) = Fragments(f: _*)
}
