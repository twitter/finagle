package com.twitter.finagle.tracing

import java.net.InetSocketAddress
import java.nio.ByteBuffer

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
