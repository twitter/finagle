package com.twitter.finagle.tracing

import com.twitter.logging._
import java.util.{logging => javalog}

/**
 * A logging Handler that sends log information via tracing
 */
class TracingLogHandler(
  formatter: Formatter = BareFormatter,
  level: Option[Level] = None
) extends Handler(formatter, level) {

  def flush() {}
  def close() {}

  def publish(record: javalog.LogRecord) {
    Trace.record(getFormatter.format(record))
  }
}
