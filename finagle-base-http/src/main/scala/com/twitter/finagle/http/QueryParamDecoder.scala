package com.twitter.finagle.http

import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.util.{ArrayList, Collections, LinkedHashMap, List => JList, Map => JMap}
import scala.annotation.tailrec

/** Query parameter decoder heavily inspired by the Netty projects QueryStringDecoder */
private object QueryParamDecoder {

  private[this] val CharsetName: String = StandardCharsets.UTF_8.name
  private[this] val MaxParams: Int = 1024

  def decode(uri: String): JMap[String, JList[String]] = {
    val qPos = uri.indexOf('?')
    if (qPos < 0 || qPos == uri.length - 1) Collections.emptyMap[String, JList[String]]
    else decodeParams(uri.substring(qPos + 1, uri.length))
  }

  def decodeParams(s: String): JMap[String, JList[String]] = {
    val params = new LinkedHashMap[String, JList[String]]

    // Limit the maximum number of params to 1024 to limit vulnerability to hash collision exploits
    // https://events.ccc.de/congress/2011/Fahrplan/attachments/2007_28C3_Effective_DoS_on_web_application_platforms.pdf
    var nParams = 0
    var name: String = null
    var mark: Int = 0 // Beginning of the unprocessed region

    @tailrec
    def go(i: Int): Unit = {
      if (i < s.length && nParams < MaxParams) {
        val c = s.charAt(i)
        if (c == '=' && name == null) {
          if (mark != i) {
            name = decodeComponent(s.substring(mark, i))
          }
          mark = i + 1
          // http://www.w3.org/TR/html401/appendix/notes.html#h-B.2.2
        } else if (c == '&' || c == ';') {
          if (name == null && mark != i) { // We haven't seen a '=' so far but moved forward.
            // Must be a param of the form '&a&' so add it with
            // an empty value.
            addParam(params, decodeComponent(s.substring(mark, i)), "")
            nParams += 1
          } else if (name != null) {
            addParam(params, name, decodeComponent(s.substring(mark, i)))
            nParams += 1
            name = null
          }
          mark = i + 1
        }
        go(i + 1)
      }
    }

    go(0)

    if (nParams == MaxParams) {
      // noop: We've hit the max params, so no need to address the tail
    } else if (mark != s.length) { // Are there characters we haven't dealt with?
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

  private[this] def addParam(params: JMap[String, JList[String]], name: String, value: String): Unit = {
    val values = params.get(name) match {
      case null =>
        val list = new ArrayList[String](1) // Often there's only 1 value.
        params.put(name, list)
        list

      case list => list
    }

    values.add(value)
  }

  // TODO: this is slow, so lets investigate making a faster version
  private[this] def decodeComponent(s: String): String =
    URLDecoder.decode(s, CharsetName)
}
