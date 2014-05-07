package com.twitter.finagle.mux.lease.exp

import java.util.TreeMap
import java.util.logging.Logger
import scala.collection.JavaConverters.mapAsScalaMapConverter

trait LogsReceiver {
  def record(name: String, value: String)

  def flush()
}

object NullLogsReceiver extends LogsReceiver {
  def record(name: String, value: String) {}

  def flush() {}
}


/**
 * This is not threadsafe.
 * The assumption is that this is only used in a single-threaded context.
 */
class DedupingLogsReceiver(log: Logger) extends LogsReceiver {
  // uses a sorted map so the ordering is deterministic
  private[this] val map: TreeMap[String, String] = new TreeMap()

  def record(name: String, value: String) {
    map.put(name, value)
  }

  def flush() {
    val strings = map.asScala map { case (left, right) =>
      "%s=%s".format(left, right)
    }
    log.info(strings.mkString(", "))

    // if we didn't clear, this would represent a memory leak.
    // however, this has worrying gc implications.
    // TODO: can we recycle logs so not so many of them end up in oldgen?
    // barring that, can we ensure all strings are the same length?
    map.clear()
  }
}
