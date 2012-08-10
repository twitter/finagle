package com.twitter.finagle.mysql.util

import java.util.regex.Pattern

class TooFewQueryParametersException(t: Throwable) extends Exception(t)
class TooManyQueryParametersException(t: Throwable) extends Exception(t)

object Query {
  val wildcard = Pattern.compile("\\?")

  def flatten(params: Seq[Any]): Seq[Any] = params flatMap {
    case p: Product => flatten(p.productIterator.toSeq)
    case i: Iterable[_] => flatten(i.toSeq)
    case x => Seq(x)
  }

  /**
   * Expands the wildcards in a prepared sql statement to its full representation
   * for Iterable and Product parameters.
   */
  def expandParams(sql: String, params: Seq[Any]): String = {
    if (params.isEmpty) {
      return sql
    }

    val matcher = Query.wildcard.matcher(sql)
    val result = new StringBuffer
    var i = 0

    def expand(param: Any): String = param match {
      case p: Product => expand(p.productIterator.toIterable)
      case a: Array[Byte] => "?"
      case i: Iterable[_] => i.toSeq.map(expand(_)).mkString(",")
      case _ => "?"
    }

    while (matcher.find) {
      try {
        matcher.appendReplacement(result, expand(params(i)))
      } catch {
        case e: ArrayIndexOutOfBoundsException =>
          throw new TooFewQueryParametersException(e)
        case e: NoSuchElementException =>
          throw new TooFewQueryParametersException(e)
      }
      i += 1
    }

    matcher.appendTail(result)
    result.toString
  }
}