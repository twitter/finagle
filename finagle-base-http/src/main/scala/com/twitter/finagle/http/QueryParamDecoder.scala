package com.twitter.finagle.http

import io.netty.handler.codec.http.QueryStringDecoder
import java.util.{ArrayList, Collections, LinkedHashMap, List => JList, Map => JMap}
import scala.annotation.tailrec

/** Query parameter decoder heavily inspired by the Netty projects QueryStringDecoder */
private object QueryParamDecoder {

  def decode(uri: String): JMap[String, JList[String]] = {
    val qPos = uri.indexOf('?')
    if (qPos < 0 || qPos == uri.length - 1) Collections.emptyMap[String, JList[String]]
    else decodeParams(uri.substring(qPos + 1, uri.length))
  }

  def decodeParams(s: String): JMap[String, JList[String]] = {
    // LinkedHashMap is known to handle hash collisions particularly well, instead of worst case
    // O(n) behavior, it will achieve O(log(n)) for keys which are of type `Comparable`, which
    // includes `String`, so there is no need to limit the number of keys.
    // https://openjdk.java.net/jeps/180
    val params = new LinkedHashMap[String, JList[String]]

    var name: String = null
    var mark: Int = 0 // Beginning of the unprocessed region

    @tailrec
    def go(i: Int): Unit = {
      if (i < s.length) {
        val c = s.charAt(i)
        if (c == '=' && name == null) {
          if (mark != i) {
            name = decodeComponent(s.substring(mark, i))
          }
          mark = i + 1
          // https://www.w3.org/TR/html401/appendix/notes.html#h-B.2.2
        } else if (c == '&' || c == ';') {
          if (name == null && mark != i) { // We haven't seen a '=' so far but moved forward.
            // Must be a param of the form '&a&' so add it with
            // an empty value.
            addParam(params, decodeComponent(s.substring(mark, i)), "")
          } else if (name != null) {
            addParam(params, name, decodeComponent(s.substring(mark, i)))
            name = null
          }
          mark = i + 1
        }
        go(i + 1)
      }
    }

    go(0)

    if (mark != s.length) { // Are there characters we haven't dealt with?
      if (name == null) { // Yes and we haven't seen any '='.
        addParam(params, decodeComponent(s.substring(mark, s.length)), "")
      } else { // Yes and this must be the last value.
        addParam(params, name, decodeComponent(s.substring(mark, s.length)))
      }
    } else if (name != null) { // Have we seen a name without value?
      addParam(params, name, "")
    }

    params
  }

  private[this] def addParam(
    params: JMap[String, JList[String]],
    name: String,
    value: String
  ): Unit = {
    val values = params.get(name) match {
      case null =>
        val list = new ArrayList[String](1) // Often there's only 1 value.
        params.put(name, list)
        list

      case list => list
    }

    values.add(value)
  }

  private[this] def decodeComponent(s: String): String =
    QueryStringDecoder.decodeComponent(s)
}
