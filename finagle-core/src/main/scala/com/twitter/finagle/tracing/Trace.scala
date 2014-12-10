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

import com.twitter.finagle.Init
import com.twitter.util.{Future, Duration, Time, Local, Stopwatch, Try, Throw, Return}
import java.net.InetSocketAddress
import scala.util.Random
import com.twitter.app.GlobalFlag
import com.twitter.finagle.context.Contexts
import com.twitter.io.Buf
import com.twitter.finagle.util.ByteArrays
 
object debugTrace extends GlobalFlag(false, "Print all traces to the console.")

/**
 * `Trace` maintains the state of the tracing stack
 * The current `TraceId` has a terminal flag, indicating whether it
 * can be overridden with a different `TraceId`. Setting the current
 * `TraceId` as terminal forces all future annotations to share that
 * `TraceId`.
 * When reporting, we report to all tracers in the list of `Tracer`s.
 */
object Trace {
  private case class TraceCtx(terminal: Boolean, tracers: List[Tracer]) {
    def withTracer(tracer: Tracer) = copy(tracers=tracer :: this.tracers)
    def withTerminal(terminal: Boolean) = 
      if (terminal == this.terminal) this
      else copy(terminal=terminal)
  }
  
  private object TraceCtx {
    val empty = TraceCtx(false, Nil)
  }

  private val traceCtx = new Contexts.local.Key[TraceCtx]

  private val someTrue = Some(true)
  
  private[finagle] val idCtx = new Contexts.broadcast.Key[TraceId] {
    private val local = new ThreadLocal[Array[Byte]] {
      override def initialValue() = new Array[Byte](32)
    }

    val marshalId = Buf.Utf8("com.twitter.finagle.tracing.TraceContext")

    def marshal(id: TraceId) =
      Buf.ByteArray(TraceId.serialize(id))

    /**
     * The wire format is (big-endian):
     *     ''spanId:8 parentId:8 traceId:8 flags:8''
     */
    def tryUnmarshal(body: Buf): Try[TraceId] = {
      if (body.length != 32)
        return Throw(new IllegalArgumentException("Expected 32 bytes"))

      val bytes = local.get()
      body.write(bytes, 0)

      val span64 = ByteArrays.get64be(bytes, 0)
      val parent64 = ByteArrays.get64be(bytes, 8)
      val trace64 = ByteArrays.get64be(bytes, 16)
      val flags64 = ByteArrays.get64be(bytes, 24)
  
      val flags = Flags(flags64)
      val sampled = if (flags.isFlagSet(Flags.SamplingKnown)) {
        Some(flags.isFlagSet(Flags.Sampled))
      } else None
  
      val traceId = TraceId(
        if (trace64 == parent64) None else Some(SpanId(trace64)),
        if (parent64 == span64) None else Some(SpanId(parent64)),
        SpanId(span64),
        sampled,
        flags)
      
      Return(traceId)
    }
  }

  private val rng = new Random
  private val defaultId = TraceId(None, None, SpanId(rng.nextLong()), None, Flags())
  @volatile private var tracingEnabled = true

  private def ctx: TraceCtx = Contexts.local.get(traceCtx) match {
    case Some(ctx) => ctx
    case None => TraceCtx.empty
  }

  /**
   * Get the current trace identifier.  If no identifiers have been
   * pushed, a default one is provided.
   */
  def id: TraceId =
    idOption match {
      case Some(id) => id
      case None => defaultId
    }

  /**
   * Get the current identifier, if it exists.
   */
  def idOption: Option[TraceId] =
    Contexts.broadcast.get(idCtx)

  /**
   * @return true if the current trace id is terminal
   */
  def isTerminal: Boolean = ctx.terminal

  /**
   * @return the current list of tracers
   */
  def tracers: List[Tracer] = ctx.tracers

  /**
   * Turn trace recording on.
   */
  def enable() = tracingEnabled = true

  /**
   * Turn trace recording off.
   */
  def disable() = tracingEnabled = false

  /**
   * Create a derived id from the current TraceId.
   */
  def nextId: TraceId = {
    val currentId = idOption
    TraceId(currentId map { _.traceId },
      currentId map { _.spanId },
      SpanId(rng.nextLong()),
      currentId map { _.sampled } getOrElse None,
      currentId map { _.flags} getOrElse Flags())
  }

  /**
   * Run computation `f` with the given traceId.
   *
   * @param traceId  the TraceId to set as the current trace id
   * @param terminal true if traceId is a terminal id. Future calls to set() after a terminal
   *                 id is set will not set the traceId
   */
  def letId[R](traceId: TraceId, terminal: Boolean = false)(f: => R): R = {
    if (isTerminal) f
    else if (terminal) {
      Contexts.local.let(traceCtx, ctx.withTerminal(terminal)) {
        Contexts.broadcast.let(idCtx, traceId)(f)
      }
    } else Contexts.broadcast.let(idCtx, traceId)(f)
  }
  
  /**
   * A version of [com.twitter.finagle.tracing.Trace.letId] providing an
   * optional ID. If the argument is None, the computation `f` is run without
   * altering the trace environment.
   */
  def letIdOption[R](traceIdOpt: Option[TraceId])(f: => R): R =
    traceIdOpt match {
      case Some(traceId) => letId(traceId)(f)
      case None => f
    }

  /**
   * Run computation `f` with `tracer` added onto the tracer stack.
   */
  def letTracer[R](tracer: Tracer)(f: => R): R =
    Contexts.local.let(traceCtx, ctx.withTracer(tracer))(f)

  /**
   * Run computation `f` with the given tracer, and a derivative TraceId.
   * The implementation of this function is more efficient than calling
   * letTracer, nextId and letId sequentially as it minimizes the number
   * of request context changes.
   *
   * @param tracer the tracer to be pushed
   * @param terminal true if the next traceId is a terminal id. Future
   *                 attempts to set nextId will be ignored.
   */
  def letTracerAndNextId[R](tracer: Tracer, terminal: Boolean = false)(f: => R): R =
    letTracerAndId(tracer, nextId, terminal)(f)

  /**
   * Run computation `f` with the given tracer and trace id.
   *
   * @param terminal true if the next traceId is a terminal id. Future
   *                 attempts to set nextId will be ignored.
   */
  def letTracerAndId[R](tracer: Tracer, id: TraceId, terminal: Boolean = false)(f: => R): R = {
    if (ctx.terminal) {
      letTracer(tracer)(f)
    } else {
      val newCtx = ctx.withTracer(tracer).withTerminal(terminal)
      val newId = id.sampled match {
        case None => id.copy(_sampled = tracer.sampleTrace(id))
        case Some(_) => id
      }
      Contexts.local.let(traceCtx, newCtx) {
        Contexts.broadcast.let(idCtx, newId)(f)
      }
    }
  }
  
  /**
   * Run computation `f` with all tracing state (tracers, trace id)
   * cleared.
   */
  def letClear[R](f: => R): R =
    Contexts.local.letClear(traceCtx) {
      Contexts.broadcast.letClear(idCtx) {
        f
      }
    }

  /**
   * Convenience method for event loops in services.  Put your
   * service handling code inside this to get proper tracing with all
   * the correct fields filled in.
   */
  def traceService[T](service: String, rpc: String, hostOpt: Option[InetSocketAddress]=None)(f: => T): T = {
    Trace.letId(Trace.nextId) {
      Trace.recordBinary("finagle.version", Init.finagleVersion)
      Trace.recordServiceName(service)
      Trace.recordRpc(rpc)
      hostOpt map { Trace.recordServerAddr(_) }
      Trace.record(Annotation.ServerRecv())
      try f finally {
        Trace.record(Annotation.ServerSend())
      }
    }
  }

  /**
   * Returns true if tracing is enabled with a good tracer pushed and the current
   * trace is sampled
   */
  def isActivelyTracing: Boolean = 
    tracingEnabled && (id match {
        case TraceId(_, _, _, Some(false), flags) if !flags.isDebug => false
        case TraceId(_, _, _, _, Flags(Flags.Debug)) => true
        case _ =>
          tracers.nonEmpty && (tracers.size > 1 || tracers.head != NullTracer)
      })

  /**
   * Record a raw record without checking if it's sampled/enabled/etc.
   */
  private[this] def uncheckedRecord(rec: Record) {
    tracers.distinct.foreach { t: Tracer => t.record(rec) }
  }

  /**
   * Record a raw ''Record''.  This will record to a _unique_ set of
   * tracers in the stack.
   */
  def record(rec: => Record) {
    if (debugTrace())
      System.err.println(rec)
    if (isActivelyTracing)
      uncheckedRecord(rec)
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
    if (debugTrace())
      System.err.println(Record(id, Time.now, ann, None))
    if (isActivelyTracing)
      uncheckedRecord(Record(id, Time.now, ann, None))
   }

  def record(ann: Annotation, duration: Duration) {
    if (debugTrace())
      System.err.println(Record(id, Time.now, ann, Some(duration)))
    if (isActivelyTracing)
      uncheckedRecord(Record(id, Time.now, ann, Some(duration)))
  }

  def record(message: String) {
    record(Annotation.Message(message))
  }

  def record(message: String, duration: Duration) {
    record(Annotation.Message(message), duration)
  }

  @deprecated("Use recordRpc and recordServiceName", "6.13.x")
  def recordRpcname(service: String, rpc: String) {
    record(Annotation.Rpcname(service, rpc))
  }

  def recordServiceName(serviceName: String) {
    record(Annotation.ServiceName(serviceName))
  }

  def recordRpc(name: String) {
    record(Annotation.Rpc(name))
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
