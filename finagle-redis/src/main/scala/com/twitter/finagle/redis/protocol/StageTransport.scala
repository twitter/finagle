package com.twitter.finagle.redis.protocol

import com.twitter.finagle.Status
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.io.Buf
import com.twitter.util.{Future, Time}
import java.net.SocketAddress

/**
 * A [[Transport]] implementation that uses [[StageDecoder]] to decode replies.
 */
private[finagle] final class StageTransport(underlying: Transport[Buf, Buf])
    extends Transport[Command, Reply] {

  type Context = TransportContext

  // StageDecoder is thread-safe so we don't have to worry about synchronizing calls to it.
  private[this] val decoder = new StageDecoder(Reply.decode)

  private[this] def readLoop(buf: Buf): Future[Reply] = decoder.absorb(buf) match {
    case null => underlying.read().flatMap(readLoop)
    case reply => Future.value(reply)
  }

  def write(c: Command): Future[Unit] = underlying.write(Command.encode(c))

  // We're starting the an empty buffer so we _drain_ the decoder hoping
  // there is something we can decode w/o reading the underlying transport.
  def read(): Future[Reply] = readLoop(Buf.Empty)

  def close(deadline: Time): Future[Unit] = underlying.close(deadline)

  override def status: Status = underlying.status
  override def onClose: Future[Throwable] = underlying.onClose
  val context: TransportContext = underlying.context
}
