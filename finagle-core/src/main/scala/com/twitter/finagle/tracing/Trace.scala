package com.twitter.finagle.tracing

import com.twitter.finagle.Init
import com.twitter.finagle.context.Contexts
import com.twitter.io.Buf
import com.twitter.util._
import java.net.InetSocketAddress
import com.twitter.app.GlobalFlag

object traceId128Bit
    extends GlobalFlag[Boolean](
      false,
      "When true, new root spans will have 128-bit trace IDs. Defaults to false (64-bit)."
    )

object enabled
    extends GlobalFlag[Boolean](
      true,
      """
      |When false, disables any tracing for this process (default: enabled). Note: it's never
      |recommended to disable tracing in production applications.
    """.stripMargin
    )

/**
 * A singleton instance of [[Tracing]] (a facade-style API) that performs a number of [[Contexts]]
 * lookups on each operation. Prefer "capturing" a [[Tracing]] instance for batching lookups.
 *
 * @see [[Tracing]]
 */
object Trace extends Tracing {

  /**
   * Captures both tracers and the trace id in this simple wrapper so we don't look up them
   * multiple times.
   */
  private final class Capture(
    val tracers: List[Tracer],
    val idOption: Option[TraceId]
  ) extends Tracing

  private[this] val tracersCtx = new Contexts.local.Key[List[Tracer]]

  private[finagle] val idCtx = new Contexts.broadcast.Key[TraceId](
    "com.twitter.finagle.tracing.TraceContext"
  ) {

    def marshal(id: TraceId): Buf =
      Buf.ByteArray.Owned(TraceId.serialize(id))

    /**
     * The wire format is (big-endian):
     *     ''spanId:8 parentId:8 traceId:8 flags:8 (traceIdHigh:8)''
     */
    def tryUnmarshal(body: Buf): Try[TraceId] =
      TraceId.deserialize(Buf.ByteArray.Owned.extract(body))
  }

  // It's ok to either write or read this value without synchronizing as long as we're not
  // doing read-modify-write concurrently (which we don't).
  @volatile private var _enabled = com.twitter.finagle.tracing.enabled()

  /**
   * Returns a [[Tracing]] instance with captured [[Contexts]] so it's cheap to reuse.
   */
  def apply(): Tracing = new Capture(tracers, idOption)

  def tracers: List[Tracer] = {
    Contexts.local.get(tracersCtx) match {
      case Some(ts) => ts
      case None => Nil
    }
  }

  def idOption: Option[TraceId] = Contexts.broadcast.get(idCtx)

  override def hasId: Boolean = Contexts.broadcast.contains(idCtx)

  /**
   * Turn trace recording on.
   */
  def enable(): Unit = _enabled = true

  /**
   * Turn trace recording off.
   */
  def disable(): Unit = _enabled = false

  /**
   * Whether or not trace recording is enabled on this process: `false` indicates it
   * was shutdown either via `-com.twitter.finagle.tracing.enabled` flag or `Trac.disable()` API.
   */
  def enabled: Boolean = _enabled

  /**
   * Run computation `f` with the given traceId.
   *
   * @param traceId  the TraceId to set as the current trace id
   * @param terminal true if traceId is a terminal id. Future calls to set() after a terminal
   *                 id is set will not set the traceId
   */
  def letId[R](traceId: TraceId, terminal: Boolean = false)(f: => R): R =
    if (isTerminal) f
    else {
      val tid = if (terminal) traceId.copy(terminal = terminal) else traceId
      Contexts.broadcast.let(idCtx, tid)(f)
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
  def letTracer[R](tracer: Tracer)(f: => R): R = {
    val ts = tracers
    if (ts.contains(tracer)) f
    else Contexts.local.let(tracersCtx, tracer :: ts)(f)
  }

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
  def letTracerAndNextId[R](tracer: Tracer, terminal: Boolean = false)(f: => R): R = {
    val tid = if (terminal) nextId.copy(terminal = terminal) else nextId
    letTracerAndId(tracer, tid)(f)
  }

  /**
   * Run computation `f` with the given tracer and trace id.
   *
   * @param terminal true if the next traceId is a terminal id. Future
   *                 attempts to set nextId will be ignored.
   */
  def letTracerAndId[R](tracer: Tracer, traceId: TraceId, terminal: Boolean = false)(f: => R): R = {
    if (isTerminal) letTracer(tracer)(f)
    else {
      val oldId = if (terminal) traceId.copy(terminal = terminal) else traceId
      val newId = oldId.sampled match {
        case None => oldId.copy(_sampled = tracer.sampleTrace(oldId))
        case Some(_) => oldId
      }

      val ts = tracers
      if (ts.contains(tracer)) Contexts.broadcast.let(idCtx, newId)(f)
      else {
        Contexts.local.let(tracersCtx, tracer :: ts) {
          Contexts.broadcast.let(idCtx, newId)(f)
        }
      }
    }
  }

  /**
   * Run computation `f` with all tracing state (tracers, trace id)
   * cleared.
   */
  def letClear[R](f: => R): R =
    Contexts.local.letClear(tracersCtx) {
      Contexts.broadcast.letClear(idCtx) {
        f
      }
    }

  /**
   * Convenience method for event loops in services.  Put your
   * service handling code inside this to get proper tracing with all
   * the correct fields filled in.
   */
  def traceService[T](
    service: String,
    rpc: String,
    hostOpt: Option[InetSocketAddress] = None
  )(
    f: => T
  ): T = Trace.letId(Trace.nextId) {
    val trace = Trace()
    if (trace.isActivelyTracing) {
      trace.recordBinary("finagle.version", Init.finagleVersion)
      trace.recordServiceName(service)
      trace.recordRpc(rpc)

      hostOpt match {
        case Some(addr) => trace.recordServerAddr(addr)
        case None =>
      }

      trace.record(Annotation.ServerRecv)

      try f
      finally {
        trace.record(Annotation.ServerSend)
      }
    } else f
  }

  /**
   * Time an operation and add an annotation with that duration on it
   * @param message The message describing the operation
   * @param f operation to perform
   * @tparam T return type
   * @return return value of the operation
   */
  def time[T](message: String)(f: => T): T = {
    val trace = Trace()
    if (trace.isActivelyTracing) {
      val elapsed = Stopwatch.start()
      val rv = f
      trace.record(message, elapsed())
      rv
    } else f
  }

  /**
   * Runs the function f and logs that duration until the future is satisfied with the given name.
   */
  def timeFuture[T](message: String)(f: Future[T]): Future[T] = {
    val trace = Trace()
    if (trace.isActivelyTracing) {
      val start = Time.now
      f.ensure(trace.record(message, start.untilNow))
    } else f
  }
}
