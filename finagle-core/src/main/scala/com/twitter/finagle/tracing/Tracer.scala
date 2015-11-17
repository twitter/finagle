package com.twitter.finagle.tracing

import com.twitter.finagle.util.LoadService
import com.twitter.util.{Duration, Time, TimeFormat}
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.logging.Logger

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
case class Record(
    traceId: TraceId,
    timestamp: Time,
    annotation: Annotation,
    duration: Option[Duration]) {
  override def toString: String = s"${RecordTimeFormat.format(timestamp)} $traceId] $annotation"
}

sealed trait Annotation
object Annotation {
  case object WireSend                             extends Annotation
  case object WireRecv                             extends Annotation
  case class WireRecvError(error: String)          extends Annotation
  case class ClientSend()                          extends Annotation
  case class ClientRecv()                          extends Annotation
  case class ClientRecvError(error: String)        extends Annotation
  case class ServerSend()                          extends Annotation
  case class ServerRecv()                          extends Annotation
  case class ServerSendError(error: String)        extends Annotation
  case class ClientSendFragment()                  extends Annotation
  case class ClientRecvFragment()                  extends Annotation
  case class ServerSendFragment()                  extends Annotation
  case class ServerRecvFragment()                  extends Annotation
  case class Message(content: String)              extends Annotation
  case class ServiceName(service: String)          extends Annotation
  case class Rpc(name: String)                     extends Annotation
  @deprecated("Use ServiceName and Rpc", "6.13.x")
  case class Rpcname(service: String, rpc: String) extends Annotation
  case class ClientAddr(ia: InetSocketAddress)     extends Annotation
  case class ServerAddr(ia: InetSocketAddress)     extends Annotation
  case class LocalAddr(ia: InetSocketAddress)      extends Annotation

  case class BinaryAnnotation(key: String, value: Any) extends Annotation {
    /* Needed to not break backwards compatibility.  Can be removed later */
    def this(key: String, value: ByteBuffer) = this(key, value: Any)
  }
}

object Tracer {
  // Deprecated.
  type Factory = () => Tracer
}

/**
 * Tracers record trace events.
 */
trait Tracer {
  def record(record: Record): Unit

  /**
   * Should we sample this trace or not? Could be decided
   * that a percentage of all traces will be let through for example.
   * True: keep it
   * False: false throw the data away
   * None: i'm going to defer making a decision on this to the child service
   */
  def sampleTrace(traceId: TraceId): Option[Boolean]
}

class NullTracer extends Tracer {
  val factory: Tracer.Factory = () => this
  def record(record: Record): Unit = {/*ignore*/}
  def sampleTrace(traceId: TraceId): Option[Boolean] = None
}

object NullTracer extends NullTracer

object BroadcastTracer {
  def apply(tracers: Seq[Tracer]): Tracer = tracers.filterNot(_ == NullTracer) match {
    case Seq() => NullTracer
    case Seq(tracer) => tracer
    case Seq(first, second) => new Two(first, second)
    case _ => new N(tracers)
  }

  private class Two(first: Tracer, second: Tracer) extends Tracer {
    def record(record: Record): Unit = {
      first.record(record)
      second.record(record)
    }

    def sampleTrace(traceId: TraceId): Option[Boolean] = {
      val sampledByFirst = first.sampleTrace(traceId)
      val sampledBySecond = second.sampleTrace(traceId)
      if (sampledByFirst == Some(true) || sampledBySecond == Some(true))
        Some(true)
      else if (sampledByFirst == Some(false) && sampledBySecond == Some(false))
        Some(false)
      else
        None
    }
  }

  private class N(tracers: Seq[Tracer]) extends Tracer {
    def record(record: Record): Unit = {
      tracers foreach { _.record(record) }
    }

    def sampleTrace(traceId: TraceId): Option[Boolean] = {
      if (tracers exists { _.sampleTrace(traceId) == Some(true) })
        Some(true)
      else if (tracers forall { _.sampleTrace(traceId) == Some(false) })
        Some(false)
      else
        None
    }
  }
}

object DefaultTracer extends Tracer with Proxy {
  private[this] val tracers = LoadService[Tracer]()
  private[this] val log = Logger.getLogger(getClass.getName)
  tracers.foreach { tracer =>
    log.info("Tracer: %s".format(tracer.getClass.getName))
  }

  // Note, `self` can be null during part of app initialization
  @volatile var self: Tracer = BroadcastTracer(tracers)

  def record(record: Record): Unit =
    if (self == null) () else self.record(record)

  def sampleTrace(traceId: TraceId): Option[Boolean] =
    if (self == null) None else self.sampleTrace(traceId)

  val get = this
}

/**
 * A tracer that buffers each record in memory. These may then be
 * iterated over.
 */
class BufferingTracer extends Tracer
  with Iterable[Record]
{
  private[this] var buf: List[Record] = Nil

  def record(record: Record): Unit = synchronized {
    buf ::= record
  }

  def iterator: Iterator[Record] = synchronized(buf).reverseIterator

  def clear(): Unit = synchronized { buf = Nil }

  def sampleTrace(traceId: TraceId): Option[Boolean] = None
}

object ConsoleTracer extends Tracer {
  val factory: Tracer.Factory = () => this

  def record(record: Record): Unit = {
    println(record)
  }

  def sampleTrace(traceId: TraceId): Option[Boolean] = None
}
