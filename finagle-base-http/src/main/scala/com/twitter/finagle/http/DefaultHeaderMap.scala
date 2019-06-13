package com.twitter.finagle.http

import com.twitter.finagle.http.Rfc7230HeaderValidation.{
  ObsFoldDetected,
  ValidationFailure,
  ValidationSuccess
}
import com.twitter.logging.Logger
import scala.annotation.tailrec
import scala.collection.mutable

/**
 * Mutable, thread-safe [[HeaderMap]] implementation.
 */
private final class DefaultHeaderMap extends HeaderMap {

  // In general, HashSet/HashTables that are not thread safe are not
  // durable to concurrent modification and can result in infinite loops.
  // As such, we synchronize on the underlying `Headers` when performing
  // accesses to avoid this. In the common case of no concurrent access,
  // this should be cheap.
  private[this] val underlying = new DefaultHeaderMap.Headers

  // ---- HeaderMap -----

  def getAll(key: String): Seq[String] = underlying.synchronized {
    underlying.getAll(key)
  }

  // Validates key and value.
  def add(key: String, value: String): HeaderMap = {
    DefaultHeaderMap.validateName(key)
    addUnsafe(key, DefaultHeaderMap.foldReplacingValidateValue(key, value))
  }

  // Does not validate key and value.
  def addUnsafe(key: String, value: String): HeaderMap = underlying.synchronized {
    underlying.add(key, value)
    this
  }

  // Validates key and value.
  def set(key: String, value: String): HeaderMap = {
    DefaultHeaderMap.validateName(key)
    setUnsafe(key, DefaultHeaderMap.foldReplacingValidateValue(key, value))
  }

  // Does not validate key and value.
  def setUnsafe(key: String, value: String): HeaderMap = underlying.synchronized {
    underlying.set(key, value)
    this
  }

  // Overriding this for efficiency reasons.
  override def getOrNull(key: String): String = {
    underlying.getFirstOrNull(key)
  }

  // ---- Map/MapLike -----

  def get(key: String): Option[String] = underlying.synchronized {
    underlying.getFirst(key)
  }

  def iterator: Iterator[(String, String)] = underlying.synchronized {
    underlying.flattenIterator
  }

  def +=(kv: (String, String)): this.type = {
    set(kv._1, kv._2)
    this
  }

  def -=(key: String): this.type = underlying.synchronized {
    underlying.removeAll(key)
    this
  }

  override def keysIterator: Iterator[String] = underlying.synchronized {
    underlying.uniqueNamesIterator
  }

  private[finagle] override def nameValueIterator: Iterator[HeaderMap.NameValue] =
    underlying.synchronized {
      underlying.flattenedNameValueIterator
    }
}

private object DefaultHeaderMap {

  private[this] val logger = Logger.get(classOf[DefaultHeaderMap])

  // Exposed for testing
  private[http] val ObsFoldRegex = "\r?\n[\t ]+".r

  private def validateName(name: String): Unit =
    Rfc7230HeaderValidation.validateName(name) match {
      case ValidationSuccess => () // nop
      case ValidationFailure(ex) => throw ex
    }

  private def foldReplacingValidateValue(name: String, value: String): String =
    Rfc7230HeaderValidation.validateValue(name, value) match {
      case ValidationSuccess =>
        value
      case ValidationFailure(ex) =>
        throw ex
      case ObsFoldDetected =>
        logger.debug("`obs-fold` sequence replaced.")
        // Per https://tools.ietf.org/html/rfc7230#section-3.2.4, an obs-fold is equivalent
        // to a SP char and suggests that such header values should be 'fixed' before
        // interpreting or forwarding the message.
        Rfc7230HeaderValidation.replaceObsFold(value)
    }

  private final class Header(val name: String, val value: String, var next: Header = null)
      extends HeaderMap.NameValue {

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

    def names: Seq[String] =
      if (next == null) name :: Nil
      else {
        val result = new mutable.ListBuffer[String] += name

        var i = next
        do {
          result += i.name
          i = i.next
        } while (i != null)

        result.toList
      }

    def add(h: Header): Unit = {
      var i = this
      while (i.next != null) {
        i = i.next
      }

      i.next = h
    }
  }

  // An internal representation for a `MapHeaderMap` that enables efficient
  //
  // - iteration by gaining access to `entriesIterator` (protected method).
  // - get/add functions by providing custom hashCode and equals methods for a key
  private final class Headers extends mutable.HashMap[String, Header] { self =>

    private def hashChar(c: Char): Int =
      if (c >= 'A' && c <= 'Z') c + 32
      else c

    // Adopted from Netty 3 HttpHeaders.
    override protected def elemHashCode(key: String): Int = {
      var result = 0
      var i = key.length - 1

      while (i >= 0) {
        val c = hashChar(key.charAt(i))
        result = 31 * result + c
        i = i - 1
      }

      result
    }

    // Adopted from Netty 3 HttpHeaders.
    override protected def elemEquals(key1: String, key2: String): Boolean =
      if (key1 eq key2) true
      else if (key1.length != key2.length) false
      else {
        @tailrec
        def loop(i: Int): Boolean =
          if (i == key1.length) true
          else {
            val a = key1.charAt(i)
            val b = key2.charAt(i)

            if (a == b || hashChar(a) == hashChar(b)) loop(i + 1)
            else false
          }

        loop(0)
      }

    // This method must be called in a thread safe manner and as such, it is only called from within
    // the `DefaultHeaderMap.*iterator` methods that synchronize on this instance.
    //
    // The resulting iterator is not invariant of mutations of the HeaderMap, but
    // shouldn't result in corruption of the HashMap. To do that, we make a copy
    // of the underlying values, so by key, it is immutable. However, adding more
    // values to an existing key is still not thread safe in terms of observability
    // since it modifies the `Header` linked list structure, but that shouldn't
    // result in corruption of this HashMap.
    private[this] def copiedEntitiesIterator: Iterator[Header] = {
      val array = new Array[Header](self.size)
      val it = self.entriesIterator
      var i = 0
      while (it.hasNext) {
        array(i) = it.next().value
        i += 1
      }

      array.iterator
    }

    def uniqueNamesIterator: Iterator[String] = new Iterator[String] {
      private[this] val it = copiedEntitiesIterator
      private[this] var current: List[String] = Nil

      // The `contains` call here isn't a problem but a feature. We're anticipating a very few
      // (usually zero) duplicated header names so the linear search in the list becomes a constant
      // time lookup in the majority of the cases and is bounded by the total number of duplicated
      // headers for a given name in the worst case.
      //
      // As in other parts of Headers implementation we're biased towards headers with no
      // duplicated names hence the trade-off of using a linear search to track uniqueness of the
      // names instead of a hash-set lookup.
      @tailrec
      private[this] def collectUnique(from: Header, to: List[String]): List[String] = {
        if (from == null) to
        else if (to.contains(from.name)) collectUnique(from.next, to)
        else collectUnique(from.next, from.name :: to)
      }

      def hasNext: Boolean =
        it.hasNext || !current.isEmpty

      def next(): String = {
        if (current.isEmpty) {
          current = collectUnique(it.next(), Nil)
        }

        val result = current.head
        current = current.tail
        result
      }
    }

    def flattenIterator: Iterator[(String, String)] =
      flattenedNameValueIterator.map(nv => (nv.name, nv.value))

    def flattenedNameValueIterator: Iterator[HeaderMap.NameValue] =
      new Iterator[HeaderMap.NameValue] {
        private[this] val it = copiedEntitiesIterator
        private[this] var current: Header = _

        def hasNext: Boolean =
          it.hasNext || current != null

        def next(): HeaderMap.NameValue = {
          if (current == null) {
            current = it.next()
          }

          val result = current
          current = current.next
          result
        }
      }

    def getFirstOrNull(key: String): String =
      findEntry(key) match {
        case null => null
        case e => e.value.value
      }

    def getFirst(key: String): Option[String] =
      Option(getFirstOrNull(key))

    def getAll(key: String): Seq[String] =
      findEntry(key) match {
        case null => Nil
        case e => e.value.values
      }

    def add(key: String, value: String): Unit = {
      val h = new Header(key, value)
      findEntry(key) match {
        case null => update(key, h)
        case e => e.value.add(h)
      }
    }

    def set(key: String, value: String): Unit = {
      val h = new Header(key, value)
      update(key, h)
    }

    def removeAll(key: String): Unit = {
      remove(key)
    }
  }

  /** Construct a new `HeaderMap` with the header list
   *
   * @note the headers are added to this `HeaderMap` via an `add` operation.
   */
  def apply(headers: (String, String)*): HeaderMap = {
    val result = new DefaultHeaderMap
    headers.foreach(t => result.add(t._1, t._2))
    result
  }
}
