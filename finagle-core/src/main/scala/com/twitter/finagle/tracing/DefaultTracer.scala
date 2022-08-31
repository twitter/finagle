package com.twitter.finagle.tracing

import com.twitter.finagle.util.LoadService
import java.util.logging.Logger

object DefaultTracer extends Tracer with Proxy {
  private[this] val tracers = LoadService[Tracer]()
  private[this] val log = Logger.getLogger(getClass.getName)
  tracers.foreach { tracer => log.info("Tracer: %s".format(tracer.getClass.getName)) }

  // Note, `self` can be null during part of app initialization
  @volatile var self: Tracer = BroadcastTracer(tracers)

  def record(record: Record): Unit =
    if (self != null)
      self.record(record)

  def sampleTrace(traceId: TraceId): Option[Boolean] =
    if (self == null) None
    else self.sampleTrace(traceId)

  val get: DefaultTracer.type = this

  def getSampleRate: Float = self.getSampleRate

  override def isActivelyTracing(traceId: TraceId): Boolean =
    self != null && self.isActivelyTracing(traceId)
}
