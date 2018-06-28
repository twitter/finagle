package com.twitter.finagle.tracing

/**
 * A tracer that buffers each record in memory. These may then be
 * iterated over.
 */
class BufferingTracer extends Tracer with Iterable[Record] {
  private[this] var buf: List[Record] = Nil

  def record(record: Record): Unit = synchronized {
    buf ::= record
  }

  def iterator: Iterator[Record] = synchronized(buf).reverseIterator

  def clear(): Unit = synchronized { buf = Nil }

  def sampleTrace(traceId: TraceId): Option[Boolean] = None

  override def isActivelyTracing(traceId: TraceId): Boolean = true
}
