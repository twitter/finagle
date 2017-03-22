package com.twitter.finagle.memcached.protocol.text.server

import com.twitter.finagle.Status
import com.twitter.finagle.memcached.protocol.text.{ResponseToEncoding, Decoding, Encoder}
import com.twitter.finagle.memcached.protocol.{Response, Command}
import com.twitter.finagle.memcached.protocol.StorageCommand.StorageCommands
import com.twitter.finagle.transport.Transport
import com.twitter.io.Buf
import com.twitter.util.{Time, Future}
import java.net.SocketAddress
import java.security.cert.Certificate

/**
 * A Transport that handles encoding Responses to Bufs and decoding framed Bufs to Commands.
 */
private[finagle] class ServerTransport(
    underlying: Transport[Buf, Buf]
) extends Transport[Response, Command] {

  private[this] val encoder = new Encoder
  private[this] val decoder = new ServerDecoder(StorageCommands)
  private[this] val responseToEncoding = new ResponseToEncoding
  private[this] val decodingToCommand = new DecodingToCommand

  // Decoding must be in a read loop because read() must return a response,
  // but we may get only get a partial message from the transport,
  // necessitating a further read.
  private[this] val decode: Buf => Future[Command] = buf => {
    val decoding: Decoding = decoder.decode(buf)
    if (decoding != null) {
      Future.value(decodingToCommand.decode(decoding))
    } else {
      readLoop()
    }
  }

  private[this] def readLoop(): Future[Command] = underlying.read().flatMap(decode)

  def read(): Future[Command] = readLoop()

  def write(response: Response): Future[Unit] = {
    val decoding: Decoding = responseToEncoding.encode(response)
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
