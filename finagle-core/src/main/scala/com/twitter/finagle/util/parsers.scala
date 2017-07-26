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
 *
 * scala> val list(int(i), long(l), longHex(hex)) = "123:123:0xa"
 * i: Int = 123
 * l: Long = 123
 * hex: Long = 10
 * }}}
 */
private[twitter] object parsers {

  object list {
    def unapplySeq(s: String): Option[List[String]] =
      if (s.isEmpty) Some(Nil)
      else Some(s.split(":").toList)
  }

  object double {
    def unapply(s: String): Option[Double] =
      try Some(s.toDouble)
      catch {
        case _: NumberFormatException => None
      }
  }

  object int {
    def unapply(s: String): Option[Int] =
      try Some(s.toInt)
      catch {
        case _: NumberFormatException => None
      }
  }

  object long {
    def unapply(s: String): Option[Long] = {
      // strip off trailing 'L' if present
      val str =
        if (s.endsWith("L"))
          s.substring(0, s.length - 1)
        else s

      try Some(str.toLong)
      catch {
        case _: NumberFormatException => None
      }
    }
  }

  /**
   * Parse a string representing a base-16 encoding of
   * a long.
   */
  object longHex {
    def unapply(s: String): Option[Long] = {
      try Some(java.lang.Long.parseLong(s.stripPrefix("0x"), 16))
      catch {
        case _: NumberFormatException => None
      }
    }
  }

  object bool {
    def unapply(s: String): Option[Boolean] =
      s.toLowerCase match {
        case "1" | "true" => Some(true)
        case "0" | "false" => Some(false)
        case _ => None
      }
  }

  object duration {
    def unapply(s: String): Option[Duration] =
      try Some(Duration.parse(s))
      catch {
        case _: NumberFormatException => None
      }
  }

}
