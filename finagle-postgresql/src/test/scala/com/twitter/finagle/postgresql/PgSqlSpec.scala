package com.twitter.finagle.postgresql

import org.scalatest.wordspec.AnyWordSpec

trait PgSqlSpec extends AnyWordSpec {
  def using[I <: java.lang.AutoCloseable, T](io: => I)(f: I => T) = {
    val c = io
    try f(c)
    finally c.close()
  }
}
