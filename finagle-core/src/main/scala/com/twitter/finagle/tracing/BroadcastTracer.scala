package com.twitter.finagle.tracing

import scala.Float.NaN

object BroadcastTracer {

  def apply(tracers: Seq[Tracer]): Tracer = tracers.filterNot(_.isNull) match {
    case Seq() => NullTracer
    case Seq(tracer) => tracer
    case Seq(first, second) => new Two(first, second)
    case Seq(first, second, third) => new Three(first, second, third)
    case _ => new N(tracers)
  }

  // cheaper than calling `o.contains(b)` as it avoids the allocations from boxing
  private def containsBool(b: Boolean, o: Option[Boolean]): Boolean = {
    o match {
      case Some(v) => b == v
      case None => false
    }
  }

  private class Two(first: Tracer, second: Tracer) extends Tracer {
    override def toString: String =
      s"BroadcastTracer($first, $second)"

    def record(record: Record): Unit = {
      first.record(record)
      second.record(record)
    }

    def sampleTrace(traceId: TraceId): Option[Boolean] = {
      val firstSample = first.sampleTrace(traceId)
      if (containsBool(true, firstSample)) {
        Tracer.SomeTrue
      } else {
        val secondSample = second.sampleTrace(traceId)
        if (containsBool(true, secondSample)) {
          Tracer.SomeTrue
        } else if (containsBool(false, firstSample) && containsBool(false, secondSample)) {
          Tracer.SomeFalse
        } else {
          None
        }
      }
    }

    def getSampleRate: Float = NaN

    override def isActivelyTracing(traceId: TraceId): Boolean =
      first.isActivelyTracing(traceId) || second.isActivelyTracing(traceId)
  }

  private class Three(first: Tracer, second: Tracer, third: Tracer) extends Tracer {
    override def toString: String =
      s"BroadcastTracer($first, $second, $third)"

    def record(record: Record): Unit = {
      first.record(record)
      second.record(record)
      third.record(record)
    }

    def sampleTrace(traceId: TraceId): Option[Boolean] = {
      val s1 = first.sampleTrace(traceId)
      if (containsBool(true, s1))
        return Tracer.SomeTrue
      val s2 = second.sampleTrace(traceId)
      if (containsBool(true, s2))
        return Tracer.SomeTrue
      val s3 = third.sampleTrace(traceId)
      if (containsBool(true, s3))
        return Tracer.SomeTrue

      if (containsBool(false, s1) &&
        containsBool(false, s2) &&
        containsBool(false, s3)) {
        Tracer.SomeFalse
      } else {
        None
      }
    }

    def getSampleRate: Float = NaN

    override def isActivelyTracing(traceId: TraceId): Boolean =
      first.isActivelyTracing(traceId) ||
        second.isActivelyTracing(traceId) ||
        third.isActivelyTracing(traceId)
  }

  private class N(tracers: Seq[Tracer]) extends Tracer {

    override def toString: String =
      s"BroadcastTracer(${tracers.mkString(", ")})"

    def record(record: Record): Unit = {
      tracers.foreach { _.record(record) }
    }

    def sampleTrace(traceId: TraceId): Option[Boolean] = {
      if (tracers.exists { t => containsBool(true, t.sampleTrace(traceId)) })
        Tracer.SomeTrue
      else if (tracers.forall { t => containsBool(false, t.sampleTrace(traceId)) })
        Tracer.SomeFalse
      else
        None
    }

    def getSampleRate: Float = NaN

    override def isActivelyTracing(traceId: TraceId): Boolean =
      tracers.exists { _.isActivelyTracing(traceId) }
  }
}
