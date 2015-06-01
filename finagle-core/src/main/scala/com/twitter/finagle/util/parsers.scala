package com.twitter.finagle.util

import com.twitter.util.Duration

/**
 * Simple parsers for use in pattern matching; e.g.
 *
 * {{{
 * scala> import com.twitter.finagle.util.parsers._
 * import com.twitter.finagle.util.parsers._
 * 
 * scala> val list(duration(dur), str, double(dbl)) = "10.seconds:abc:123.32"
 * dur: com.twitter.util.Duration = 10.seconds
 * str: String = abc
 * dbl: Double = 123.32
 * }}}
 */
private[finagle] object parsers {

  object list {
    def unapplySeq(s: String): Option[List[String]] =
      if (s.isEmpty) Some(Nil)
      else Some(s.split(":").toList)
  }
  
  object double {
    def unapply(s: String): Option[Double] =
      try Some(s.toDouble) catch {
        case _: NumberFormatException => None
      }
  }
  
  object int {
    def unapply(s: String): Option[Int] =
      try Some(s.toInt) catch {
        case _: NumberFormatException => None
      }
  }
  
  object duration {
    def unapply(s: String): Option[Duration] =
      try Some(Duration.parse(s)) catch {
        case _: NumberFormatException => None
      }
  }

}
