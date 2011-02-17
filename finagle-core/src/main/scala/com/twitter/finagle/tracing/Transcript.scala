package com.twitter.finagle.tracing

/** 
 * Transcripts are programmer-provided records of events and are
 * contained in a trace context. They are equivalent to a sequence of
 * "annotations" in Dapper.
 */ 

import collection.mutable.ArrayBuffer
import com.twitter.util.Time

case class Record(
  traceID: TraceID,
  timestamp: Time,   // (nanosecond granularity)
  message: String)    // an arbitrary string message
{
  override def toString = "[%s] @ %s: %s".format(traceID, timestamp, message)
}

trait Transcript extends Iterable[Record] {
  // TODO: support log levels?

  def record(message: => String)
  def isRecording = true
  def merge(other: Iterator[Record])

  def print() { foreach { println(_) } }
}

/**
 * The default transcript uses the current transcript as of per trace
 * context.
 */
object Transcript extends Transcript {
  def record(message: => String) { Trace().transcript.record(message) }
  def iterator = Trace().transcript.iterator
  override def isRecording = Trace().transcript.isRecording
  def merge(other: Iterator[Record]) = Trace().transcript.merge(other)
}

/**
 * Sinks all messages
 */
object NullTranscript extends Transcript {
  def record(message: => String) {}
  def iterator = Iterator.empty
  override def isRecording = false
  def merge(other: Iterator[Record]) {}
}

/**
 * Buffers messages to an ArrayBuffer.
 */
class BufferingTranscript(traceID: TraceID) extends Transcript {
  private[this] val buffer = new ArrayBuffer[Record]

  def record(message: => String) = synchronized {
    buffer += Record(traceID, Time.now, message)
  }

  def iterator = buffer.iterator

  def clear() = synchronized {
    buffer.clear()
  }

  def merge(other: Iterator[Record]) = synchronized {
    // TODO: resolve time drift by causality
    var combined = buffer ++ other
    combined = combined sortWith { (a, b) => a.timestamp < b.timestamp }
    combined = combined distinct

    buffer.clear()
    buffer.appendAll(combined)
  }
}
