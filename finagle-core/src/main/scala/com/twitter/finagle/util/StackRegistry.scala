package com.twitter.finagle.util

import com.twitter.finagle.Stack
import com.twitter.util.registry.GlobalRegistry
import scala.language.existentials

object StackRegistry {
  /** Represents an entry in the registry. */
  case class Entry(name: String, addr: String, stack: Stack[_], params: Stack.Params) {
    // Introspect the entries stack and params. We limit the
    // reflection of params to case classes.
    // TODO: we might be able to make this avoid reflection with Showable
    val modules: Seq[Module] = (stack.tails.map { node =>
      val raw = node.head.parameters.map { p => params(p) }
      val reflected = raw.foldLeft(Seq.empty[(String, String)]) {
        case (seq, p: Product) =>
          // TODO: many case classes have a $outer field because they close over an outside scope.
          // this is not very useful, and it might make sense to filter them out in the future.
          val fields = p.getClass.getDeclaredFields.map(_.getName).toSeq
          val values = p.productIterator.map(_.toString).toSeq
          seq ++ (fields.zipAll(values, "<unknown>", "<unknown>"))

        case (seq, _) => seq
      }
      Module(node.head.role.name, node.head.description, reflected)
    }).toSeq
  }

  /**
   * The module describing a given Param for a Stack element.
   */
  case class Module(name: String, description: String, fields: Seq[(String, String)])
}

/**
 * A registry that allows the registration of a string identifier with a
 * a [[com.twitter.finagle.Stack]] and its params. This is especially useful
 * in keeping a process global registry of Finagle clients and servers for
 * dynamic introspection.
 */
trait StackRegistry {
  import StackRegistry._

  /** The name of the [[StackRegistry]], to be used for identification in the registry. */
  def registryName: String

  private[this] var registry = Map.empty[String, Entry]

  /** Registers a `name` with an `addr` and `stk`. */
  def register(name: String, addr: String, stk: Stack[_], params: Stack.Params): Unit = {
    val entry = Entry(name, addr, stk, params)
    val gRegistry = GlobalRegistry.get
    entry.modules.foreach { case Module(paramName, _, reflected) =>
      reflected.foreach { case (field, value) =>
        gRegistry.put(Seq(registryName, name, addr, paramName, field), value)
      }
    }
    synchronized { registry += name -> entry }
  }

  /** Returns a list of all entries. */
  def registrants: Iterable[Entry] = synchronized { registry.values }

  // added for tests
  private[finagle] def clear(): Unit = synchronized { registry = Map.empty[String, Entry] }
}
