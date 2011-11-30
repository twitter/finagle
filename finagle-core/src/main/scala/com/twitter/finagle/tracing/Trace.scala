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

import scala.util.Random
import java.nio.ByteBuffer
import java.net.InetSocketAddress

import com.twitter.util.{Time, Local}

/**
 * `Trace` maintains an interleaved stack of `TraceId`s and `Tracer`s.
 * The semantics are as follows: when reporting, we always report the
 * topmost `TraceId`.  That action is reported to all the `Tracer`s
 * that are _below_ that point in the stack.
 */
object Trace {
  private[this] type Stack = List[Either[TraceId, Tracer]]
  private[this] val rng = new Random

  private[this] val defaultId = TraceId(None, None, SpanId(rng.nextLong()), None)
  private[this] val local = new Local[Stack]
  @volatile private[this] var tracingEnabled = true

  /**
   * Get the current trace identifier.  If no identifiers have been
   * pushed, a default one is provided.
   */
  def id: TraceId = idOption getOrElse defaultId

  /**
   * Get the current identifier, if it exists.
   */
  def idOption: Option[TraceId] =
    local() flatMap { stack =>
      stack collect { case Left(id) => id } headOption
    }

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
      None)
  }

  /**
   * Create a derivative TraceId and push it.  If there isn't a
   * current ID, this becomes the root id.
   */
  def pushId(): TraceId = {
    pushId(nextId)
  }

  /**
   * Push a new trace id.
   */
  def pushId(traceId: TraceId): TraceId = {
    // todo: should this to parent/trace management?
    local() = Left(traceId) :: (local() getOrElse Nil)
    traceId
  }

  /**
   * Pop the topmost trace id and return it.
   */
  def popId(): Option[TraceId] = {
    local() match {
      case None | Some(Nil) => None
      case Some(Left(topmost@_) :: rest) =>
        local() = rest
        Some(topmost)
      case Some(Right(_) :: rest) =>
        local() = rest
        popId()
    }
  }

  /**
   * Push the given tracer.
   */
  def pushTracer(tracer: Tracer) {
    local() = Right(tracer) :: (local() getOrElse Nil)
  }

  /**
   * Invoke `f` and then unwind the stack to the starting point.
   */
  def unwind[T](f: => T): T = {
    val saved = local()
    try f finally local.set(saved)
  }

  /*
   * Recording methods report the topmost trace id to every tracer
   * lower in the stack.
   */

   /**
    * Find the set of tracers appropriate for the given ID.
    */
   private[this] def tracers: (Stack, Option[TraceId], List[Tracer]) => Seq[Tracer] = {
     case (Nil, _, ts) => (Set() ++ ts).toSeq
     case (Left(stackId) :: rest, Some(lookId), _) if stackId == lookId => tracers(rest, None, Nil)
     case (Left(_) :: rest, id, ts) => tracers(rest, id, ts)
     case (Right(t) :: rest, id, ts) => tracers(rest, id, t :: ts)
   }

   /**
    * Record a raw ''Record''.  This will record to a _unique_ set of
    * tracers:
    *
    *  1.  if the ID specified is in the stack, the record will be
    *  recorded to those traces _below_ the first ID entry with that
    *  value in the stack.
    *
    *  2.  if the ID is *not* in the stack, we report it to all of the
    *  tracers in the stack.
    */
   def record(rec: Record) {
     if (tracingEnabled)
       tracers(local() getOrElse Nil, Some(rec.traceId), Nil) foreach { _.record(rec) }
   }

   /*
    * Convenience methods that construct records of different kinds.
    */
  def record(ann: Annotation) {
    record(Record(id, Time.now, ann))
  }

  def record(message: String) {
    record(Annotation.Message(message))
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

  def recordBinary(key: String, value: Any) {
    record(Annotation.BinaryAnnotation(key, value))
  }
}
