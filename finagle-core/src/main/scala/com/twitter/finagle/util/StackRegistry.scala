package com.twitter.finagle.util

import com.twitter.finagle.Stack
import com.twitter.finagle.param.Label
import com.twitter.finagle.param.ProtocolLibrary
import com.twitter.finagle.stack.nilStack
import com.twitter.util.registry.GlobalRegistry
import java.util.concurrent.atomic.AtomicInteger

object StackRegistry {

  /**
   * Represents an entry in the registry.
   */
  case class Entry(addr: String, stack: Stack[_], params: Stack.Params) {

    def modules: Seq[Module] =
      stack.tails.flatMap { node =>
        if (node.head.role == nilStack.head.role) {
          // do not register `nilStack`
          None
        } else {
          val raw = node.head.parameters
          val reflected = raw.foldLeft(Seq.empty[(String, () => String)]) {
            case (seq, s) if s.show(params(s)).nonEmpty =>
              seq ++ s.show(params(s))

            // If Stack.Param.show() returns an empty Seq, and the parameter is a case class, obtain the names
            // and values via reflection.
            case (seq, s) =>
              params(s) match {
                case p: Product =>
                  // TODO: many case classes have a $outer field because they close over an outside scope.
                  // this is not very useful, and it might make sense to filter them out in the future.
                  val fields = p.getClass.getDeclaredFields.map(_.getName)
                  val valueFunctions = p.productIterator.map(v => () => v.toString).toSeq
                  seq ++ fields.zipAll(valueFunctions, "<unknown>", () => "<unknown>")
                case _ => seq
              }
          }
          Some(
            Module(
              node.head.role.name,
              node.head.description,
              reflected.map { case (n, v) => (n, v()) }
            ))
        }
      }.toSeq

    val name: String = params[Label].label
    val protocolLibrary: String = params[ProtocolLibrary].name
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

  // thread-safe updates via synchronization on `this`
  private[this] var registry = Map.empty[String, Entry]

  private[this] val numEntries = new AtomicInteger(0)

  // thread-safe updates via synchronization on `this`
  private[this] var duplicates: Map[String, Seq[Entry]] =
    Map.empty[String, Seq[Entry]]

  /**
   * Returns any registered [[Entry Entries]] that had the same [[Label]].
   */
  def registeredDuplicates: Seq[Entry] = synchronized {
    duplicates.values.flatten.toSeq
  }

  /** Registers an `addr`, `Stack`, and `Params`. */
  def register(addr: String, stk: Stack[_], params: Stack.Params): Unit = {
    val entry = Entry(addr, stk, params)
    addEntries(entry)
    synchronized {
      if (registry.contains(entry.name)) {
        val updated = duplicates.get(entry.name) match {
          case Some(values) => values :+ entry
          case None => Seq(entry)
        }
        duplicates += entry.name -> updated
      }
      registry += entry.name -> entry
    }
  }

  /**
   * Utility for getting the registry key's prefix for an [[Entry]].
   */
  protected def registryPrefix(entry: Entry): Seq[String] =
    Seq(registryName, entry.protocolLibrary, entry.name, entry.addr)

  /** Unregisters an `addr`, `Stack`, and `Params`. */
  def unregister(addr: String, stk: Stack[_], params: Stack.Params): Unit = {
    val entry = Entry(addr, stk, params)
    synchronized {
      duplicates.get(entry.name) match {
        case Some(dups) =>
          if (dups.size == 1)
            duplicates -= entry.name
          else
            // We may not remove the exact same entry, but since they are duplicates,
            // it does not matter.
            duplicates += entry.name -> dups.drop(1)
        case None =>
          // only remove when there is no more duplications
          registry -= entry.name
      }
    }
    removeEntries(entry)
  }

  private[this] def addEntries(entry: Entry): Unit = {
    val prefix = registryPrefix(entry)
    entry.modules.foreach {
      case Module(paramName, _, reflected) =>
        reflected.foreach {
          case (field, value) =>
            val key = prefix ++ Seq(paramName, field)
            add(key, value)
        }
    }
  }

  protected[this] def add(key: Seq[String], value: String): Unit = {
    if (GlobalRegistry.get.put(key, value).isEmpty)
      numEntries.incrementAndGet()
  }

  private[this] def removeEntries(entry: Entry): Unit = {
    val prefix = registryPrefix(entry)
    entry.modules.foreach {
      case Module(paramName, _, reflected) =>
        reflected.foreach {
          case (field, _) =>
            val key = prefix ++ Seq(paramName, field)
            remove(key)
        }
    }
  }

  protected[this] def remove(key: Seq[String]): Unit = {
    if (GlobalRegistry.get.remove(key).isDefined)
      numEntries.decrementAndGet()
  }

  /** Returns the number of entries */
  def size: Int = numEntries.get

  /** Returns a list of all entries. */
  def registrants: Iterable[Entry] = synchronized { registry.values }

  // added for tests
  private[finagle] def clear(): Unit = synchronized {
    registry = Map.empty[String, Entry]
    duplicates = Map.empty[String, Seq[Entry]]
  }
}
