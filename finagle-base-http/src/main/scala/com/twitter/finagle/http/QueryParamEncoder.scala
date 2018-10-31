package com.twitter.finagle.http

import java.io.UnsupportedEncodingException
import java.net.URLEncoder
import java.nio.charset.{StandardCharsets, UnsupportedCharsetException}
import java.util.regex.Pattern
import scala.annotation.tailrec

private object QueryParamEncoder {

  private[this] val regex = Pattern.compile("""\+""")
  private[this] val charsetName: String = StandardCharsets.UTF_8.name

  def encode(values: Iterable[(String, String)]): String = {
    try {
      if (values.isEmpty) ""
      else {
        val it = values.iterator
        // The default value of 16 bytes is unlikely to be enough
        // so we go with an unscientific guess of 128 to start
        val sb = new StringBuilder(128, "?")

        // Assumes that it has a `.next` value
        @tailrec
        def loop(): Unit = {
          val (k, v) = it.next()
          appendComponent(sb, k)
          sb.append('=')
          appendComponent(sb, v)

          if (it.hasNext) {
            sb.append('&')
            loop()
          }
        }

        loop() // We know that there is at least one element in the iterator
        replacePluses(sb.mkString)
      }
    } catch {
      case _: UnsupportedEncodingException =>
        throw new UnsupportedCharsetException(charsetName)
    }
  }

  private[this] def appendComponent(sb: StringBuilder, s: String): Unit = {
    // TODO: URLEncoder is known to be slow, explore more efficient options.
    val encoded = URLEncoder.encode(s, charsetName)
    sb.append(encoded)
  }

  private[this] def replacePluses(s: String): String = {
    // TODO: we can do better than a regex
    s.indexOf('+') match {
      case -1 => s // fast path
      case _ => regex.matcher(s).replaceAll("%20")
    }
  }
}
