package com.twitter.finagle.tracing

object Tracer {

  /**
   * Useful constant for the return value of [[Tracer.sampleTrace]]
   */
  val SomeTrue: Option[Boolean] = Some(true)

  /**
   * Useful constant for the return value of [[Tracer.sampleTrace]]
   */
  val SomeFalse: Option[Boolean] = Some(false)
}

/**
 * Tracers record trace events.
 */
trait Tracer {
  def record(record: Record): Unit

  /**
   * Indicates whether or not this tracer instance is [[NullTracer]].
   */
  def isNull: Boolean = false

  /**
   * Should we sample this trace or not? Could be decided
   * that a percentage of all traces will be let through for example.
   * True: keep it
   * False: false throw the data away
   * None: i'm going to defer making a decision on this to the child service
   *
   * @see [[Tracer.SomeTrue]] and [[Tracer.SomeFalse]] as constant return values.
   */
  def sampleTrace(traceId: TraceId): Option[Boolean]

  /**
   * What is the percentage of traces we're sampling?
   * The sample rate is a float value between 0.0 and 1.0. If sampling
   * is decided based on factors other than the sampleRate, this value should NaN
   */
  def getSampleRate: Float

  /**
   * Is this tracer actively tracing this traceId?
   *
   * Return:
   * If [[TraceId.sampled]] == None
   *   [[sampleTrace()]] has not been called yet or the tracer still wants to
   *   receive traces but not make the decision for child services. In either
   *   case return true so that this tracer is still considered active for this
   *   traceId.
   *
   * If [[TraceId.sampled]] == Some(decision)
   *   [[sampleTrace()]] has already been called, or a previous service has already
   *   made a decision whether to sample this trace or not. So respect that decision
   *   and return it.
   */
  def isActivelyTracing(traceId: TraceId): Boolean =
    traceId.sampled match {
      case None => true
      case Some(sample) => sample
    }
}
