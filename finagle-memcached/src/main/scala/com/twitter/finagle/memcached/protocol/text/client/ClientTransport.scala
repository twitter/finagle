package com.twitter.finagle.memcached.protocol.text.client

import com.twitter.finagle.Status
import com.twitter.finagle.memcached.protocol.text._
import com.twitter.finagle.transport.Transport
import com.twitter.io.Buf
import com.twitter.util.{Future, Time}
import java.net.SocketAddress
import java.security.cert.Certificate
import scala.util.control.NonFatal

/**
 * A Transport that handles encoding Commands to Bufs and decoding framed Bufs to Responses.
 */
private[finagle] class ClientTransport[Command, Response >: Null](
    commandToBuf: AbstractCommandToBuf[Command],
    decoder: ClientDecoder[Response],
    underlying: Transport[Buf, Buf])
  extends Transport[Command, Response] {

  // Decoding must be in a read loop because read() must return a response,
  // but we may get only get a partial message from the transport,
  // necessitating a further read.
  private[this] val decode: Buf => Future[Response] = buf => {
    // we wrap the decoder call in a try / catch since it can throw exceptions and we don't
    // want to leave the connection open with the decoder in an undefined state.
    try {
      val result = decoder.decode(buf)
      if (result != null) Future.value(result)
      else readLoop()
    } catch { case NonFatal(t) =>
      underlying.close().transform(_ => Future.exception(t))
    }
  }

  private[this] def readLoop(): Future[Response] = underlying.read().flatMap(decode)

  def read(): Future[Response] = readLoop()

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
