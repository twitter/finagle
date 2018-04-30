package com.twitter.finagle.netty4.http

import com.twitter.finagle.dispatch.GenSerialClientDispatcher.wrapWriteException
import com.twitter.finagle.http._
import com.twitter.finagle.http.exp.{Multi, StreamTransportProxy}
import com.twitter.finagle.netty4.ByteBufConversion
import com.twitter.finagle.transport.Transport
import com.twitter.io.{Buf, Reader, Writer}
import com.twitter.logging.Logger
import com.twitter.util._
import io.netty.handler.codec.{http => NettyHttp}
import java.net.InetSocketAddress

private[http] object StreamTransports {
  val log = Logger.get()

  /**
   * Drain [[Transport]] messages into a [[Writer]] until `eos` indicates end
   * of stream. Reads from the transport are interleaved with writes to the
   * [[Writer]] so that no more than one message is ever buffered.
   *
   *  @note this method is identical to [[Transport.copyToWriter]] except for the
   *        fact that this variant allows EOS messages to also carry data.
   */
  def copyToWriter[A](
    trans: Transport[_, A],
    writer: Writer
  )(eos: A => Boolean)(chunkOfA: A => Buf): Future[Unit] =
    trans.read().flatMap { a: A =>
      val chunk = chunkOfA(a)
      val writeF =
        if (!chunk.isEmpty) writer.write(chunk)
        else Future.Done
      if (eos(a))
        writeF
      else
        writeF.before(copyToWriter(trans, writer)(eos)(chunkOfA))
    }

  /**
   * Collate [[Transport]] messages into a [[Reader]]. Processing terminates when an
   * EOS message arrives.
   *
   * @ note This implementation differentiates itself from [[Transport.collate]]
   *        by allowing EOS messages to carry data.
   */
  def collate[A](
    trans: Transport[_, A],
    chunkOfA: A => Buf
  )(eos: A => Boolean): Reader with Future[Unit] = new Promise[Unit] with Reader {
    private[this] val rw = Reader.writable()

    // Ensure that collate's future is satisfied _before_ its reader
    // is closed. This allows callers to observe the stream completion
    // before readers are notified.
    private[this] val writes = copyToWriter(trans, rw)(eos)(chunkOfA)
    forwardInterruptsTo(writes)
    writes.respond {
      case ret @ Throw(t) =>
        updateIfEmpty(ret)
        rw.fail(t)
      case r @ Return(_) =>
        updateIfEmpty(r)
        rw.close()
    }

    def read(n: Int): Future[Option[Buf]] = rw.read(n)

    def discard(): Unit = {
      rw.discard()
      raise(new Reader.ReaderDiscarded)
    }
  }

  def readChunk(chunk: Any): Buf = chunk match {
    case chunk: NettyHttp.HttpContent if chunk.content.readableBytes == 0 =>
      Buf.Empty
    case chunk: NettyHttp.HttpContent =>
      ByteBufConversion.byteBufAsBuf(chunk.content)

    case other =>
      throw new IllegalArgumentException(
        "Expected a HttpContent, but read an instance of " + other.getClass.getSimpleName)
  }

  def chunkOfBuf(buf: Buf): NettyHttp.HttpContent =
    new NettyHttp.DefaultHttpContent(ByteBufConversion.bufAsByteBuf(buf))

  /**
   * Drain a [[Reader]] into a [[Transport]]. The inverse of collation.
   */
  def streamChunks(
    trans: Transport[Any, Any],
    r: Reader,
    // TODO Find a better number for bufSize, e.g. 32KiB - Buf overhead
    bufSize: Int = Int.MaxValue
  ): Future[Unit] = {
    r.read(bufSize).flatMap {
      case None =>
        trans.write(NettyHttp.LastHttpContent.EMPTY_LAST_CONTENT)
      case Some(buf) =>
        trans.write(chunkOfBuf(buf)).transform {
          case Return(_) => streamChunks(trans, r, bufSize)
          case Throw(t) => {
            log.debug(t, "Failure while writing chunk to stream")
            Future(r.discard())
          }
        }
    }
  }

  val isLast: Any => Boolean = _.isInstanceOf[NettyHttp.LastHttpContent]
}

private[finagle] class Netty4ServerStreamTransport(rawTransport: Transport[Any, Any])
    extends StreamTransportProxy[Response, Request](rawTransport) {
  import StreamTransports._

  private[this] val transport =
    Transport.cast[NettyHttp.HttpResponse, NettyHttp.HttpRequest](rawTransport)

  def write(in: Response): Future[Unit] = {
    val nettyRep =
      if (in.isChunked)
        Bijections.finagle.responseHeadersToNetty(in)
      else
        Bijections.finagle.fullResponseToNetty(in)

    transport.write(nettyRep).rescue(wrapWriteException).before {
      if (in.isChunked) streamChunks(rawTransport, in.reader)
      else Future.Done
    }
  }

  def read(): Future[Multi[Request]] = {
    transport.read().flatMap {
      case req: NettyHttp.FullHttpRequest =>
        val finagleReq = Bijections.netty.fullRequestToFinagle(req, transport.remoteAddress match {
          case ia: InetSocketAddress => ia
          case _ => new InetSocketAddress(0)
        })
        Future.value(Multi(finagleReq, Future.Done))

      case req: NettyHttp.HttpRequest =>
        assert(!req.isInstanceOf[NettyHttp.HttpContent]) // chunks are handled via collation

        val coll = collate(rawTransport, readChunk)(isLast)
        val finagleReq = Bijections.netty.chunkedRequestToFinagle(
          req,
          coll,
          transport.remoteAddress match {
            case ia: InetSocketAddress => ia
            case _ => new InetSocketAddress(0)
          }
        )
        Future.value(Multi(finagleReq, coll))

      case invalid =>
        // relies on GenSerialClientDispatcher satisfying `p`
        Future.exception(new IllegalArgumentException(s"invalid message '$invalid'"))
    }
  }
}

private[finagle] class Netty4ClientStreamTransport(rawTransport: Transport[Any, Any])
    extends StreamTransportProxy[Request, Response](rawTransport) {
  import StreamTransports._

  private[this] val transport =
    Transport.cast[NettyHttp.HttpResponse, NettyHttp.HttpRequest](rawTransport)

  def write(in: Request): Future[Unit] = {
    val nettyReq = Bijections.finagle.requestToNetty(in)
    rawTransport.write(nettyReq).rescue(wrapWriteException).before {
      if (in.isChunked) streamChunks(rawTransport, in.reader)
      else Future.Done
    }
  }

  def read(): Future[Multi[Response]] = {
    rawTransport.read().flatMap {
      // fully buffered message
      case rep: NettyHttp.FullHttpResponse =>
        val finagleRep: Response = Bijections.netty.fullResponseToFinagle(rep)
        Future.value(Multi(finagleRep, Future.Done))

      // chunked message, collate the transport
      case rep: NettyHttp.HttpResponse =>
        assert(!rep.isInstanceOf[NettyHttp.HttpContent]) // chunks are handled via collation
        val coll = collate(rawTransport, readChunk)(isLast)
        val finagleRep: Response = Bijections.netty.chunkedResponseToFinagle(rep, coll)
        Future.value(Multi(finagleRep, coll))

      case invalid =>
        // relies on GenSerialClientDispatcher satisfying `p`
        Future.exception(new IllegalArgumentException(s"invalid message '$invalid'"))
    }
  }
}
