package com.twitter.finagle.memcached.protocol.text.client

import com.twitter.finagle.Status
import com.twitter.finagle.memcached.protocol.text._
import com.twitter.finagle.transport.Transport
import com.twitter.io.Buf
import com.twitter.util.{Time, Future}
import java.net.SocketAddress
import java.security.cert.Certificate

/**
 * A Transport that handles encoding Commands to Bufs and decoding framed Bufs to Responses.
 */
private[finagle] class ClientTransport[Command, Response](
    commandToEncoding: AbstractCommandToEncoding[Command],
    decodingToResponse: AbstractDecodingToResponse[Response],
    underlying: Transport[Buf, Buf])
  extends Transport[Command, Response] {

  private[this] val decoder = new ClientDecoder
  private[this] val encoder = new Encoder

  // Decoding must be in a read loop because read() must return a response,
  // but we may get only get a partial message from the transport,
  // necessitating a further read.
  private[this] val decode: Buf => Future[Response] = buf => {
    val decoding: Decoding = decoder.decode(buf)
    if (decoding != null) {
      Future.value(decodingToResponse.decode(decoding))
    } else {
      readLoop()
    }
  }

  private[this] def readLoop(): Future[Response] = underlying.read().flatMap(decode)

  def read(): Future[Response] = readLoop()

  def write(command: Command): Future[Unit] = {
    val decoding: Decoding = commandToEncoding.encode(command)
    val buf: Buf = encoder.encode(decoding)
    underlying.write(buf)
  }

  def remoteAddress: SocketAddress = underlying.remoteAddress

  def peerCertificate: Option[Certificate] = underlying.peerCertificate

  def onClose: Future[Throwable] = underlying.onClose

  def localAddress: SocketAddress = underlying.localAddress

  def status: Status = underlying.status

  def close(deadline: Time): Future[Unit] = underlying.close(deadline)
}
