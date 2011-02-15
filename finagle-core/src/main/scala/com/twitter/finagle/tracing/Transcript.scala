package com.twitter.finagle.tracing

/** 
 * Transcripts are records of events and are contained in a trace
 * context.
 */ 

import collection.mutable.ArrayBuffer
import com.twitter.util.Time

case class Record(
  host: Int,         // 32-bit IP address
  vmID: String,      // virtual machine identifier
  spanID: Long,
  parentSpanID: Option[Long],
  timestamp: Time,   // (nanosecond granularity)
  message: String    // an arbitrary string message
)

trait Transcript extends Iterable[Record] {
  // Log levels?

  def record(message: => String)
  def isRecording = true
}

/**
 * The default transcript uses the current transcript as of per trace
 * context.
 */
object Transcript extends Transcript {
  def record(message: => String) { TraceContext().transcript.record(message) }
  def iterator = TraceContext().transcript.iterator
  override def isRecording = TraceContext().transcript.isRecording
}

/**
 * Sinks all messages
 */
object NullTranscript extends Transcript {
  def record(message: => String) {}
  def iterator = Iterator.empty
  override def isRecording = false
}

/**
 * Buffers messages to an ArrayBuffer.
 */
class BufferingTranscript extends Transcript {
  private[this] val buffer = new ArrayBuffer[Record]

  def record(message: => String) = synchronized {
    buffer += Record(
      Host(), VMID(),
      TraceContext().spanID, TraceContext().parentSpanID,
      Time.now,
      message)
  }

  def iterator = buffer.iterator

  def clear() = synchronized {
    buffer.clear()
  }
}
