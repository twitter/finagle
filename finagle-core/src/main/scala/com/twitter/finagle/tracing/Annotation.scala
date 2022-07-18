package com.twitter.finagle.tracing

import java.net.InetSocketAddress

/**
 * An ADT describing a tracing annotation. Prefer [[Tracing]] API to creating raw [[Annotation]]
 * instances (especially, when used from Java).
 */
sealed abstract class Annotation

object Annotation {

  case object WireSend extends Annotation
  case object WireRecv extends Annotation
  final case class WireRecvError(error: String) extends Annotation
  case object ClientSend extends Annotation
  case object ClientRecv extends Annotation
  final case class ClientRecvError(error: String) extends Annotation
  case object ServerSend extends Annotation
  case object ServerRecv extends Annotation
  final case class ServerSendError(error: String) extends Annotation
  case object ClientSendFragment extends Annotation
  case object ClientRecvFragment extends Annotation
  case object ServerSendFragment extends Annotation
  case object ServerRecvFragment extends Annotation
  final case class Message(content: String) extends Annotation
  final case class ServiceName(service: String) extends Annotation
  final case class Rpc(name: String) extends Annotation
  final case class ClientAddr(ia: InetSocketAddress) extends Annotation
  final case class ServerAddr(ia: InetSocketAddress) extends Annotation
  final case class LocalAddr(ia: InetSocketAddress) extends Annotation

  final case class BinaryAnnotation(key: String, value: Any) extends Annotation
}
