package com.twitter.finagle.memcached.protocol.text.server

import com.twitter.finagle.Status
import com.twitter.finagle.memcached.protocol.{Command, Response}
import com.twitter.finagle.memcached.protocol.StorageCommand.StorageCommands
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.io.Buf
import com.twitter.util.{Future, Time}

/**
 * A Transport that handles encoding Responses to Bufs and decoding framed Bufs to Commands.
 */
private[finagle] class ServerTransport(underlying: Transport[Buf, Buf])
    extends Transport[Response, Command] {

  type Context = TransportContext

  private[this] val decoder = new MemcachedServerDecoder(StorageCommands)

  // Decoding must be in a read loop because read() must return a response,
  // but we may get only get a partial message from the transport,
  // necessitating a further read.
  private[this] val decode: Buf => Future[Command] = buf => {
    val command: Command = decoder.decode(buf)
    if (command != null) {
      Future.value(command)
    } else {
      readLoop()
    }
  }

  private[this] def readLoop(): Future[Command] = underlying.read().flatMap(decode)

  def read(): Future[Command] = readLoop()

  def write(response: Response): Future[Unit] = {
    val buf: Buf = ResponseToBuf.encode(response)
    underlying.write(buf)
  }

  def onClose: Future[Throwable] = underlying.onClose

  def status: Status = underlying.status

  def close(deadline: Time): Future[Unit] = underlying.close(deadline)

  val context: TransportContext = underlying.context
}
