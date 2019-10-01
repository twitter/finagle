package com.twitter.finagle.http.headers

import scala.collection.mutable
import scala.collection.AbstractIterator
import com.twitter.finagle.http.HeaderMap

private[http] sealed class Header private (final val name: String, final val value: String)
    extends HeaderMap.NameValue {
    
  def iterator: Iterator[HeaderMap.NameValue] =
    if (next == null) Iterator.single(this)
    else {
      var cur = this
      new AbstractIterator[HeaderMap.NameValue] {
        def hasNext: Boolean = cur != null
        def next(): HeaderMap.NameValue = {
          var n = cur
          cur = n.next
          n
        }
      }
    }

  final protected var _next: Header = null

  final def next: Header = _next
}

private[http] object Header {

  final class Root private[Header] (name: String, value: String) extends Header(name, value) {

    // We want to keep a reference to the last element of this linked list
    // so we don't need to traverse the whole list to add an element.
    private[this] var last: Header = null

    def add(name: String, value: String): Unit = {
      val n = new Header(name, value)
      if (next == null) {
        // the second element.
        _next = n
      } else {
        last._next = n
      }
      // Set the reference to the last element for future `add` calls.
      last = n
    }

    def values: Seq[String] =
      if (next == null) value :: Nil
      else {
        val result = new mutable.ListBuffer[String] += value

        var i = next
        do {
          result += i.value
          i = i.next
        } while (i != null)

        result.toList
      }
  }

  /** Create a new root node */
  def root(name: String, value: String): Root =
    new Root(name, value)
}
