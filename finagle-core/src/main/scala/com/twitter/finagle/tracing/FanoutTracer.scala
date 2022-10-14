package com.twitter.finagle.tracing

/**
 * A proxy that wraps any [[Tracer]] so that annotations can retroactively be assigned new traceIds.
 * Annotations added prior to partitioning (or any filter that creates peer requests) on the stack
 * are stored in a buffer to be flushed once we have traceIds for the individual fanout requests.
 * This allows fanout spans to hold the complete sum of annotations for that RPC.
 *
 * This works in tandem with [[Trace.letPeerId]], which will add those individual traceIds to the
 * tids buffer via [[flushRecordsToNewId]] so that those pre-partitioning annotations are flushed
 * to the correct peer spans.
 */
private[tracing] final case class FanoutTracer(underlying: Tracer) extends Tracer {
  var records = List.empty[Record]
  var tids = Set.empty[TraceId]
  var flushed = false

  override def record(record: Record): Unit = synchronized {
    // If the record's traceId is already stored in tids, we know it is the id of a fanout span.
    // We can go ahead and record it immediately rather than put it in our records buffer
    // because we only want to buffer pre-partitioning records.
    if (tids.contains(record.traceId)) underlying.record(record)
    else records = record +: records
  }

  /**
   * Adds newTid to the tids buffer, so that buffered records can be flushed to this id.
   */
  def flushRecordsToNewId(newTid: TraceId): Unit = synchronized { tids += newTid }

  /**
   * Iterates through the buffered [[records]] and submits them to the [[underlying]] tracer to
   * record with the new tids, provided by the [[tids]] buffer.
   *
   * A given [[FanoutTracer]] can only be flushed once. This is guarded against via [[flushed]]
   */
  def flush(): Unit = synchronized {
    if (!flushed) {
      if (tids.nonEmpty && records.nonEmpty) {
        for {
          r <- records
          id <- tids
        } underlying.record(r.copy(traceId = id))
      }
    }
    flushed = true
  }

  override def sampleTrace(traceId: TraceId): Option[Boolean] = underlying.sampleTrace(traceId)

  override def getSampleRate: Float = underlying.getSampleRate
}
