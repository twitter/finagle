package com.twitter.finagle.http.util

import com.twitter.finagle.http.HeaderMap
import scala.annotation.tailrec

/**
 * An Ordering used to order the keys in a sortable map.
 * This ordering makes keys case insensitive.
 */
private[http] object HeaderKeyOrdering extends Ordering[String] {
  override def compare(key1: String, key2: String): Int = {
    // Shorter strings are always less, regardless of their content
    val lengthDiff = key1.length - key2.length
    if (lengthDiff != 0) lengthDiff
    else {
      @tailrec
      def go(i: Int): Int = {
        if (i == key1.length) 0 // end, they are equal.
        else {
          val char1 = HeaderMap.hashChar(key1.charAt(i))
          val char2 = HeaderMap.hashChar(key2.charAt(i))
          val diff = char1 - char2
          if (diff == 0) go(i + 1)
          else diff
        }
      }
      go(0)
    }
  }
}
