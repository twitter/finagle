package com.twitter.finagle.mysql.util

import java.util.regex.Pattern

class TooFewQueryParametersException(t: Throwable) extends Exception(t)
class TooManyQueryParametersException(t: Throwable) extends Exception(t)

object Query {
  val wildcard = Pattern.compile("\\?")

  /**
   * Injects the string representation of each param sequentially
   * into the sql string.
   */
  def injectParams(sql: String, params: Seq[Any]): String = {
    def inject(param: Any): String = param match {
      case p: Product => inject(p.productIterator.toIterable)
      case i: Iterable[_] => i.toSeq.map(inject(_)).mkString(",")
      case any => any.toString
    }

    replace(sql, params, inject)
  }

  /**
   * Expands the wildcard in the sql to its full representation.
   * Useful for prepared statements.
   */
  def expandParams(sql: String, params: Seq[Any]): String = {
    def expand(param: Any): String = param match {
      case p: Product => expand(p.productIterator.toIterable)
      case a: Array[Byte] => "?"
      case i: Iterable[_] => i.toSeq.map(expand(_)).mkString(",")
      case _ => "?"
    }

    replace(sql, params, expand)
  }


  def flatten(params: Seq[Any]): Seq[Any] = params flatMap {
    case p: Product => flatten(p.productIterator.toSeq)
    case i: Iterable[_] => flatten(i.toSeq)
    case x => Seq(x)
  }

  /**
   * Replace each wildcard instance in sql with the output from
   * running f(param(i))
   */
  private def replace(sql: String, params: Seq[Any], f: Any => String): String = {
    if(params.isEmpty) {
      return sql
    }

    val matcher = Query.wildcard.matcher(sql)
    val result = new StringBuffer
    var i = 0

    while (matcher.find) {
      try {
        matcher.appendReplacement(result, f(params(i)))
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