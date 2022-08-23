package com.twitter.finagle.exp.fiber_scheduler.util

import java.util.HashSet

/**
 * Utility class used by `toString` methods to avoid stack
 * overflows due to reference loops. It keeps a set of seen
 * objects and invokes one of the provided functions accordingly.
 */
object AvoidLoop {

  private val local = ThreadLocal.withInitial(() => new HashSet[Any])

  def apply[T](obj: Any, ifNotLoop: => T, ifLoop: => T): T = {
    val seen = local.get()
    if (seen.contains(obj)) {
      ifLoop
    } else {
      seen.add(obj)
      try ifNotLoop
      finally seen.remove(obj)
    }
  }
}
