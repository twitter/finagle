package com.twitter.finagle.tracing

/**
 * Tracers record trace events.
 */

import java.nio.ByteBuffer
import java.net.InetSocketAddress
import scala.collection.mutable.ArrayBuffer

import com.twitter.util.{Time, TimeFormat}

private[tracing] object RecordTimeFormat
  extends TimeFormat("MMdd HH:mm:ss.SSS")

case class Record(traceId: TraceId, timestamp: Time, annotation: Annotation) {
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
  case class BinaryAnnotation(key: String, value: ByteBuffer) extends Annotation
}

trait Tracer {
  def record(record: Record)
}

object NullTracer extends Tracer {
  def record(record: Record) {/*ignore*/}
}

class BufferingTracer extends Tracer with Iterable[Record] {
  private[this] val buf = new ArrayBuffer[Record]

  def record(record: Record) { buf += record }
  def iterator = buf.iterator
  def clear() = buf.clear()
}

object ConsoleTracer extends Tracer {
  def record(record: Record) {
    println(record)
  }
}
