package com.twitter.finagle.memcached.protocol.text.client

import com.twitter.finagle.Status
import com.twitter.finagle.memcached.protocol.text._
import com.twitter.finagle.transport.Transport
import com.twitter.io.Buf
import com.twitter.util.{Future, Time}
import java.net.SocketAddress
import java.security.cert.Certificate

/**
 * A Transport that handles encoding Commands to Bufs and decoding framed Bufs to Responses.
 */
private[finagle] class ClientTransport[Command, Response](
  commandToBuf: AbstractCommandToBuf[Command],
  underlying: Transport[Buf, Response]
) extends Transport[Command, Response] {

  def read(): Future[Response] = underlying.read()

  def write(command: Command): Future[Unit] = {
    val buf: Buf = commandToBuf.encode(command)
    underlying.write(buf)
  }

  def remoteAddress: SocketAddress = underlying.remoteAddress

  def peerCertificate: Option[Certificate] = underlying.peerCertificate

  def onClose: Future[Throwable] = underlying.onClose

  def localAddress: SocketAddress = underlying.localAddress

  def status: Status = underlying.status

  def close(deadline: Time): Future[Unit] = underlying.close(deadline)
}
