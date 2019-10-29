package com.twitter.finagle.http.headers

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.AbstractIterator
import com.twitter.finagle.http.HeaderMap

private[http] sealed class Header private (final val name: String, final val value: String)
    extends HeaderMap.NameValue {

  final protected var _next: Header = null

  final def next: Header = _next
}

private[http] object Header {

  final class Root private[Header] (name: String, value: String) extends Header(name, value) {
    self =>

    def iterator: Iterator[HeaderMap.NameValue] =
      if (next == null) Iterator.single(this)
      else {
        new AbstractIterator[HeaderMap.NameValue] {
          private[this] var cur: Header = self
          def hasNext: Boolean = cur != null
          def next(): HeaderMap.NameValue = {
            var n = cur
            cur = n.next
            n
          }
        }
      }

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

  def uniqueNames(orig: Iterator[Header]): Iterator[String] = new Iterator[String] {
    private[this] val it = orig
    private[this] var current: Iterator[String] = Iterator.empty

    private[this] def collectUnique(from: Header): Iterator[String] = new Iterator[String] {

      // We need to keep `_next` current so that we can reliably determine `.hasNext`.
      // That means that if `_next` is not null, it is, in fact, the next value.
      private[this] var nextUnique = from

      // We keep a set of observed names so that we don't accidentally return
      // duplicates. Another way to have done this would be to simply add all names
      // to a set and return an iterator over that set, but this way is more lazy.
      private[this] var seen = Set.empty + from.name

      def hasNext: Boolean = nextUnique != null

      def next(): String = {
        if (!hasNext) throw new NoSuchElementException

        val result = nextUnique.name
        // We then need to determine the next `_next` so we can
        // again have a sane `.hasNext`.
        prepareNext()
        result
      }

      @tailrec
      private[this] def prepareNext(): Unit = {
        nextUnique = nextUnique.next
        if (nextUnique != null) {
          val prevSize = seen.size
          seen = seen + nextUnique.name
          if (seen.size == prevSize) {
            // already observed. Try again.
            prepareNext()
          }
        }
      }
    }

    def hasNext: Boolean =
      it.hasNext || current.hasNext

    def next(): String = {
      if (current.isEmpty) {
        val hs = it.next()
        if (hs.next == null) {
          // A shortcut to see if we have only a single entry for this name
          hs.name
        } else {
          // slow path: we need to make a sub-iterator
          current = collectUnique(hs)
          current.next()
        }
      } else {
        current.next()
      }
    }
  }
}
