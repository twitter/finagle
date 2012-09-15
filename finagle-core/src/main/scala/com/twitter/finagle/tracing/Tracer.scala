package com.twitter.finagle.tracing

/**
 * Tracers record trace events.
 */

import com.twitter.finagle.util.{CloseNotifier, Disposable, Managed}
import com.twitter.util.{Duration, Future, Time, TimeFormat}

import java.nio.ByteBuffer
import java.net.InetSocketAddress
import scala.collection.mutable.ArrayBuffer

private[tracing] object RecordTimeFormat
  extends TimeFormat("MMdd HH:mm:ss.SSS")

object Record {
  def apply(traceId: TraceId, timestamp: Time, annotation: Annotation): Record = {
    Record(traceId, timestamp, annotation, None)
  }
}

/**
 * Records information of interest to the tracing system. For example when an event happened,
 * the service name or ip addresses involved.
 * @param traceId Which trace is this record a part of?
 * @param timestamp When did the event happen?
 * @param annotation What kind of information should we record?
 * @param duration Did this event have a duration? For example: how long did a certain code block take to run
 */
case class Record(traceId: TraceId, timestamp: Time, annotation: Annotation, duration: Option[Duration]) {
  override def toString = "%s %s] %s".format(
    RecordTimeFormat.format(timestamp), traceId, annotation)
}

sealed trait Annotation
object Annotation {
  case class ClientSend()                          extends Annotation
  case class ClientRecv()                          extends Annotation
  case class ServerSend()                          extends Annotation
  case class ServerRecv()                          extends Annotation
  case class Message(content: String)              extends Annotation
  case class Rpcname(service: String, rpc: String) extends Annotation
  case class ClientAddr(ia: InetSocketAddress)     extends Annotation
  case class ServerAddr(ia: InetSocketAddress)     extends Annotation

  case class BinaryAnnotation(key: String, value: Any) extends Annotation {
    /* Needed to not break backwards compatibility.  Can be removed later */
    def this(key: String, value: ByteBuffer) = this(key, value: Any)
  }
}

object Tracer {
  type Factory = (CloseNotifier) => Tracer

  def mkManaged(tracerFactory: Factory): Managed[Tracer] = new Managed[Tracer] {
    private[this] val notifier = new CloseNotifier {
      var handler = { () => {} }
      def onClose(h: => Unit) = handler = { () => h }
    }
    def make() = new Disposable[Tracer] {
      val underlying = tracerFactory(notifier)
      def get = underlying
      def dispose(deadline: Time) = {
        notifier.handler()
        Future.value(())
      }
    }
  }
}

trait Tracer {
  def record(record: Record)

  /**
   * Should we sample this trace or not? Could be decided
   * that a percentage of all traces will be let through for example.
   * True: keep it
   * False: false throw the data away
   * None: i'm going to defer making a decision on this to the child service
   */
  def sampleTrace(traceId: TraceId): Option[Boolean]

  def release() {}
}

object NullTracer extends Tracer {
  val factory: Tracer.Factory = (_) => this
  def record(record: Record) {/*ignore*/}
  def sampleTrace(traceId: TraceId): Option[Boolean] = None
}

class BufferingTracer extends Tracer
  with Iterable[Record]
{
  private[this] val buf = new ArrayBuffer[Record]

  def record(record: Record) { buf += record }
  def iterator = buf.iterator
  def clear() = buf.clear()

  def sampleTrace(traceId: TraceId): Option[Boolean] = None
}

object ConsoleTracer extends Tracer {
  val factory: Tracer.Factory = (_) => this

  def record(record: Record) {
    println(record)
  }

  def sampleTrace(traceId: TraceId): Option[Boolean] = None
}

