package com.twitter.finagle.tracing

import com.twitter.finagle.util.LoadService
import com.twitter.util.{Duration, Time, TimeFormat}
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.logging.Logger

private[tracing] object RecordTimeFormat extends TimeFormat("MMdd HH:mm:ss.SSS")

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
  duration: Option[Duration]
) {
  override def toString: String = s"${RecordTimeFormat.format(timestamp)} $traceId] $annotation"
}

sealed trait Annotation
object Annotation {
  case object WireSend extends Annotation
  case object WireRecv extends Annotation
  case class WireRecvError(error: String) extends Annotation
  case class ClientSend() extends Annotation
  case class ClientRecv() extends Annotation
  case class ClientRecvError(error: String) extends Annotation
  case class ServerSend() extends Annotation
  case class ServerRecv() extends Annotation
  case class ServerSendError(error: String) extends Annotation
  case class ClientSendFragment() extends Annotation
  case class ClientRecvFragment() extends Annotation
  case class ServerSendFragment() extends Annotation
  case class ServerRecvFragment() extends Annotation
  case class Message(content: String) extends Annotation
  case class ServiceName(service: String) extends Annotation
  case class Rpc(name: String) extends Annotation
  case class ClientAddr(ia: InetSocketAddress) extends Annotation
  case class ServerAddr(ia: InetSocketAddress) extends Annotation
  case class LocalAddr(ia: InetSocketAddress) extends Annotation

  case class BinaryAnnotation(key: String, value: Any) extends Annotation {
    /* Needed to not break backwards compatibility.  Can be removed later */
    def this(key: String, value: ByteBuffer) = this(key, value: Any)
  }
}

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

/**
 * A no-op [[Tracer]].
 */
class NullTracer extends Tracer {
  def record(record: Record): Unit = ()
  def sampleTrace(traceId: TraceId): Option[Boolean] = None
  override def isNull: Boolean = true
  override def isActivelyTracing(traceId: TraceId): Boolean = false
  override def toString: String = "NullTracer"
}

/**
 * A singleton instance of a no-op [[NullTracer]].
 */
object NullTracer extends NullTracer

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
      if (tracers.exists { t =>
          containsBool(true, t.sampleTrace(traceId))
        })
        Tracer.SomeTrue
      else if (tracers.forall { t =>
          containsBool(false, t.sampleTrace(traceId))
        })
        Tracer.SomeFalse
      else
        None
    }

    override def isActivelyTracing(traceId: TraceId): Boolean =
      tracers.exists { _.isActivelyTracing(traceId) }
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
    if (self != null)
      self.record(record)

  def sampleTrace(traceId: TraceId): Option[Boolean] =
    if (self == null) None
    else self.sampleTrace(traceId)

  val get: DefaultTracer.type = this

  override def isActivelyTracing(traceId: TraceId): Boolean =
    self != null && self.isActivelyTracing(traceId)
}

/**
 * A tracer that buffers each record in memory. These may then be
 * iterated over.
 */
class BufferingTracer extends Tracer with Iterable[Record] {
  private[this] var buf: List[Record] = Nil

  def record(record: Record): Unit = synchronized {
    buf ::= record
  }

  def iterator: Iterator[Record] = synchronized(buf).reverseIterator

  def clear(): Unit = synchronized { buf = Nil }

  def sampleTrace(traceId: TraceId): Option[Boolean] = None

  override def isActivelyTracing(traceId: TraceId): Boolean = true
}

object ConsoleTracer extends Tracer {
  def record(record: Record): Unit = {
    println(record)
  }

  def sampleTrace(traceId: TraceId): Option[Boolean] = None

  override def isActivelyTracing(traceId: TraceId): Boolean = true
}
