package com.twitter.finagle.http2

import com.twitter.finagle.Status
import com.twitter.finagle.transport.{LegacyContext, Transport, TransportContext}
import com.twitter.util.{Future, Time}
import java.net.SocketAddress
import java.security.cert.Certificate

private[http2] final class DeadTransport(
  exn: Throwable,
  val remoteAddress: SocketAddress
) extends Transport[Any, Any] {
  override type Context = TransportContext

  private[this] lazy val opsResult = Future.exception(exn)

  lazy val onClose: Future[Throwable] = Future.value(exn)
  lazy val context: TransportContext = new LegacyContext(this)
  lazy val localAddress: SocketAddress = new SocketAddress {}

  def peerCertificate: Option[Certificate] = None
  def write(req: Any): Future[Unit] = opsResult
  def read(): Future[Any] = opsResult
  def status: Status = Status.Closed
  def close(deadline: Time): Future[Unit] = Future.Done
}
