package com.twitter.finagle.tracing

/**
 * This is a tracing system similar to Dapper:
 *
 *   “Dapper, a Large-Scale Distributed Systems Tracing Infrastructure”,
 *   Benjamin H. Sigelman, Luiz André Barroso, Mike Burrows, Pat
 *   Stephenson, Manoj Plakal, Donald Beaver, Saul Jaspan, Chandan
 *   Shanbhag, 2010.
 *
 * It is meant to be independent of whatever underlying RPC mechanism
 * is being used, and it is up to the underlying codec to implement
 * the transport.
 */

import com.twitter.util.{Future, Duration, Time, Local, Stopwatch}
import java.net.InetSocketAddress
import scala.util.Random

/**
 * `Trace` maintains the state of the tracing stack
 * The current `TraceId` has a terminal flag, indicating whether it
 * can be overridden with a different `TraceId`. Setting the current
 * `TraceId` as terminal forces all future annotations to share that
 * `TraceId`.
 * When reporting, we report to all tracers in the list of `Tracer`s.
 */
object Trace  {
  sealed trait TraceState
  private case class State(id: Option[TraceId], terminal: Boolean, tracers: List[Tracer]) extends TraceState
  private case object NoState extends TraceState

  private[this] val rng = new Random

  private[this] val defaultId = TraceId(None, None, SpanId(rng.nextLong()), None, Flags())
  private[this] val local = new Local[State]
  @volatile private[this] var tracingEnabled = true

  /**
   * Get the current trace identifier.  If no identifiers have been
   * pushed, a default one is provided.
   */
  def id: TraceId = idOption getOrElse defaultId

  /**
   * Get the current identifier, if it exists.
   */
  def idOption: Option[TraceId] = local() flatMap { _.id }

  /**
   * @return true if the current trace id is terminal
   */
  def isTerminal: Boolean = local() exists { _.terminal }

  /**
   * @return the current list of tracers
   */
  def tracers: List[Tracer] = local() map { _.tracers } getOrElse Nil

  /**
   * Completely clear the trace stack.
   */
  def clear() {
    local.clear()
  }

  /**
   * Turn trace recording on.
   */
  def enable() = tracingEnabled = true

  /**
   * Turn trace recording off.
   */
  def disable() = tracingEnabled = false

    /**
   * Create a derivative TraceId. If there isn't a
   * current ID, this becomes the root id.
   */
  def nextId: TraceId = {
    val currentId = idOption
    TraceId(currentId map { _.traceId },
      currentId map { _.spanId },
      SpanId(rng.nextLong()),
      currentId map { _.sampled } getOrElse None,
      currentId map { _.flags} getOrElse Flags())
  }

  @deprecated("use setId() instead", "5.0.1")
  def pushId(): TraceId = pushId(nextId)

  @deprecated("use setId() instead", "5.0.1")
  def pushId(traceId: TraceId): TraceId = setId(traceId)

  /**
   * Set the current trace id
   * Should be used with Trace.unwind for stack-like properties
   *
   * @param traceId  the TraceId to set as the current trace id
   * @param terminal true if traceId is a terminal id. Future calls to set() after a terminal
   *                 id is set will not set the traceId
   */
  def setId(traceId: TraceId, terminal: Boolean = false): TraceId = {
    if (!isTerminal)
      local() match {
        case None    => local() = State(Some(traceId), terminal, tracers)
        case Some(s) => local() = s.copy(id = Some(traceId), terminal = terminal)
      }

    traceId
  }

  def setTerminalId(traceId: TraceId): TraceId = setId(traceId, true)

  /**
   * Push the given tracer.
   */
  def pushTracer(tracer: Tracer) {
    local() match {
      case None    => local() = State(None, false, tracer :: Nil)
      case Some(s) => local() = s.copy(tracers = tracer :: this.tracers)
    }
  }

  /**
   * Push the given tracer, create a derivative TraceId and set it to be
   * the current trace id.
   * The implementation of this function is more efficient than calling
   * pushTracer, nextId and setId sequentially as it minimizes the number
   * of reads and writes to `local`.
   *
   * @param tracer the tracer to be pushed
   * @param terminal true if the next traceId is a terminal id. Future
   *                 attempts to set nextId will be ignored.
   */
  def pushTracerAndSetNextId(tracer: Tracer, terminal: Boolean = false) {
    val currentState = local()
    val _nextId = nextId
    val newState = currentState match {
      case None => State(None, false, tracer :: Nil)
      case Some(s) => s.copy(tracers = tracer :: s.tracers)
    }
    /* Only set traceId of the current State if terminal is not set */
    if (!newState.terminal) {
      _nextId.sampled match {
        case None =>
          local() = newState.copy(terminal = terminal,
            id = Some(_nextId.copy(_sampled = tracer.sampleTrace(_nextId))))
        case Some(_) => local() = newState.copy(terminal = terminal, id = Some(_nextId))
      }
    } else
      local() = newState
  }

  def state: TraceState = local() getOrElse NoState
  def state_=(state: TraceState) = state match {
    case NoState =>
    case s: State => local.set(Some(s))
  }

  /**
   * Convenience method for event loops in services.  Put your service handling
   * code inside this to get proper tracing with all the correct fields filled in.
   */
  def traceService[T](service: String, rpc: String, hostOpt: Option[InetSocketAddress]=None)(f: => T): T = {
    unwind {
      Trace.setId(Trace.nextId)
      Trace.recordRpcname(service, rpc)
      hostOpt map { Trace.recordServerAddr(_) }
      Trace.record(Annotation.ServerRecv())
      try f finally {
        Trace.record(Annotation.ServerSend())
      }
    }
  }

  /**
   * Invoke `f` and then unwind the stack to the starting point.
   */
  def unwind[T](f: => T): T = {
    val saved = local()
    try f finally local.set(saved)
  }

  /**
   * Returns true if tracing is enabled with a good tracer pushed and the current
   * trace is sampled
   */
  def isActivelyTracing: Boolean = {
    if (!tracingEnabled) false else { // short circuit
      local() match {
        case Some(State(Some(TraceId(_, _, _, Some(false), Flags(0L))), _, _)) => false
        case None => false
        case Some(State(_, _, Nil)) => false
        case Some(State(_, _, List(NullTracer))) => false
        case Some(State(Some(TraceId(_, _, _, _, Flags(Flags.Debug))), _, _)) => true
        case _ => true // backwards compat: default to trace on incomplete data
      }
    }
  }

  /**
   * record a raw record without checking if it's sampled/enabled/etc
   */
  private[this] def uncheckedRecord(rec: Record) {
    tracers.distinct.foreach { t: Tracer => t.record(rec) }
  }

  /**
   * Record a raw ''Record''.  This will record to a _unique_ set of
   * tracers in the stack.
   */
  def record(rec: => Record) {
    if (isActivelyTracing) uncheckedRecord(rec)
  }

  /**
   * Time an operation and add an annotation with that duration on it
   * @param message The message describing the operation
   * @param f operation to perform
   * @tparam T return type
   * @return return value of the operation
   */
  def time[T](message: String)(f: => T): T = {
    val elapsed = Stopwatch.start()
    val rv = f
    record(message, elapsed())
    rv
  }

  /**
   * Runs the function f and logs that duration until the future is satisfied with the given name.
   */
  def timeFuture[T](message: String)(f: Future[T]): Future[T] = {
    val start = Time.now
    f.ensure {
      record(message, start.untilNow)
    }
    f
  }

   /*
    * Convenience methods that construct records of different kinds.
    */
  def record(ann: Annotation) {
    if (isActivelyTracing)
      uncheckedRecord(Record(id, Time.now, ann, None))
   }

  def record(ann: Annotation, duration: Duration) {
    if (isActivelyTracing)
      uncheckedRecord(Record(id, Time.now, ann, Some(duration)))
  }

  def record(message: String) {
    record(Annotation.Message(message))
  }

  def record(message: String, duration: Duration) {
    record(Annotation.Message(message), duration)
  }

  def recordRpcname(service: String, rpc: String) {
    record(Annotation.Rpcname(service, rpc))
  }

  def recordClientAddr(ia: InetSocketAddress) {
    record(Annotation.ClientAddr(ia))
  }

  def recordServerAddr(ia: InetSocketAddress) {
    record(Annotation.ServerAddr(ia))
  }

  def recordLocalAddr(ia: InetSocketAddress) {
    record(Annotation.LocalAddr(ia))
  }

  def recordBinary(key: String, value: Any) {
    record(Annotation.BinaryAnnotation(key, value))
  }

  def recordBinaries(annotations: Map[String, Any]) {
    if (isActivelyTracing) {
      for ((key, value) <- annotations) {
        recordBinary(key, value)
      }
    }
  }
}
