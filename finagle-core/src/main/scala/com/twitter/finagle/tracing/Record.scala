package com.twitter.finagle.tracing

import com.twitter.util.Duration
import com.twitter.util.Time
import com.twitter.util.TimeFormatter

/**
 * Records information of interest to the tracing system. For example when an event happened,
 * the service name or ip addresses involved.
 *
 * @param traceId Which trace is this record a part of?
 * @param timestamp When did the event happen?
 * @param annotation What kind of information should we record?
 * @param duration Did this event have a duration? For example: how long did a certain code block take to run
 */
case class Record(
  traceId: TraceId,
  timestamp: Time,
  annotation: Annotation,
  duration: Option[Duration]) {
  override def toString: String = s"${Record.timeFormat.format(timestamp)} $traceId] $annotation"
}

object Record {
  def apply(traceId: TraceId, timestamp: Time, annotation: Annotation): Record = {
    Record(traceId, timestamp, annotation, None)
  }

  private val timeFormat = TimeFormatter("MMdd HH:mm:ss.SSS")

}
