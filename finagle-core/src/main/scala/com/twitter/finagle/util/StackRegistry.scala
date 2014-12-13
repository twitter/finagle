package com.twitter.finagle.util

import com.twitter.finagle.Stack

object StackRegistry {
  /** Represents an entry in the registry. */
  case class Entry(name: String, addr: String, stack: Stack[_], params: Stack.Params)
}

/**
 * A registry that allows the registration of a string identifier with a
 * a [[com.twitter.finagle.Stack]] and its params. This is especially useful
 * in keeping a process global registry of Finagle clients and servers for
 * dynamic introspection.
 */
trait StackRegistry {
  import StackRegistry._

  private[this] var registry = Map.empty[String, Entry]

  /** Registers a `name` with an `addr` and `stk`. */
  def register(name: String, addr: String, stk: Stack[_], params: Stack.Params): Unit =
    synchronized { registry += name -> Entry(name, addr, stk, params) }

  /** Returns a list of all entries. */
  def registrants: Iterable[Entry] = synchronized { registry.values }

  // added for tests
  private[finagle] def clear(): Unit = synchronized { registry = Map.empty[String, Entry] }
}
