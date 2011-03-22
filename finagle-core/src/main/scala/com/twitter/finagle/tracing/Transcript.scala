package com.twitter.finagle.tracing

/** 
 * Transcripts are programmer-provided records of events and are
 * contained in a trace context. They are equivalent to a sequence of
 * "annotations" in Dapper.
 */ 

import collection.mutable.ArrayBuffer
import com.twitter.util.Time

sealed trait Annotation
object Annotation {
  case class ClientSend()             extends Annotation
  case class ClientRecv()             extends Annotation
  case class ServerSend()             extends Annotation
  case class ServerRecv()             extends Annotation
  case class Message(content: String) extends Annotation
}

case class Record(timestamp: Time, annotation: Annotation) {
  override def toString = "%s: %s".format(timestamp, annotation)
}

trait Transcript extends Iterable[Record] {
  // TODO: support log levels?

  def record(annotation: => Annotation)
  def recordAll(other: Iterator[Record])

  // XXX remove.
  def merge(other: Transcript) = recordAll(other.iterator)

  def isRecording = true
  def print() { foreach { println(_) } }
}

/**
 * Sinks all messages
 */
object NullTranscript extends Transcript {
  def record(annotation: => Annotation) {}
  def recordAll(other: Iterator[Record]) {}
  def iterator = Iterator.empty
  override def isRecording = false
}

/**
 * Buffers messages to an ArrayBuffer.
 */
class BufferingTranscript extends Transcript {
  private[this] val buffer = new ArrayBuffer[Record]

  def record(annotation: => Annotation) = synchronized {
    // TODO: insertion sort?
    buffer += Record(Time.now, annotation)
  }

  def recordAll(other: Iterator[Record]) = synchronized {
    // TODO: resolve time drift by causality
    var combined = buffer ++ other
    combined = combined sortWith { (a, b) => a.timestamp < b.timestamp }
    combined = combined distinct

    buffer.clear()
    buffer.appendAll(combined)
  }

  def iterator = buffer.iterator

  def clear() = synchronized {
    buffer.clear()
  }
}

class FrozenTranscript(underlying: Iterable[Record]) extends Transcript {
  def record(annotation: => Annotation) {}
  def recordAll(other: Iterator[Record]) {}
  def iterator = underlying.iterator
  override def isRecording = false
}
