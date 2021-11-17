package com.twitter.finagle.netty4.http

import com.twitter.finagle.dispatch.ClientDispatcher.wrapWriteException
import com.twitter.finagle.http._
import com.twitter.finagle.netty4.ByteBufConversion
import com.twitter.finagle.transport.Transport
import com.twitter.io.Pipe
import com.twitter.io.Reader
import com.twitter.io.ReaderDiscardedException
import com.twitter.io.StreamTermination
import com.twitter.util._
import io.netty.handler.codec.http._
import java.net.InetSocketAddress

private[http] object Netty4StreamTransport {

  /**
   * Collate [[Transport]] messages into a [[Reader]]. Processing terminates when an
   * EOS message arrives.
   */
  def streamIn(trans: Transport[Any, Any]): Reader[Chunk] with Future[Unit] =
    new Promise[Unit] with Reader[Chunk] {

      private[this] val pipe = new Pipe[Chunk]

      private def copyLoop(): Future[Unit] =
        trans.read().flatMap {
          case chunk: LastHttpContent =>
            val last =
              if (!chunk.content.isReadable && chunk.trailingHeaders().isEmpty)
                Chunk.lastEmpty
              else
                Chunk.last(
                  ByteBufConversion.byteBufAsBuf(chunk.content()),
                  Bijections.netty.headersToFinagle(chunk.trailingHeaders())
                )

            pipe.write(last)

          case chunk: HttpContent =>
            val cons = Chunk(ByteBufConversion.byteBufAsBuf(chunk.content))
            pipe.write(cons).before(copyLoop())

          case other =>
            Future.exception(
              new IllegalArgumentException(
                "Expected a HttpContent, but read an instance of " + other.getClass.getSimpleName
              ))
        }

      // Ensure that collate's future is satisfied _before_ its reader
      // is closed. This allows callers to observe the stream completion
      // before readers are notified.
      private[this] val writes = copyLoop()

      forwardInterruptsTo(writes)

      writes.respond {
        case ret @ Throw(t) =>
          updateIfEmpty(ret)
          pipe.fail(t)
        case r @ Return(_) =>
          updateIfEmpty(r)
          pipe.close()
      }

      def read(): Future[Option[Chunk]] = pipe.read()

      def discard(): Unit = {
        // The order in which these two are running matters. We want to fail the underlying transport
        // before we discard the user-facing reader, thereby releasing the connection in the stack.
        // If we do these in the reverse order, the connection that's about to get failed, becomes
        // available for reuse.
        raise(new ReaderDiscardedException)
        pipe.discard()
      }

      def onClose: Future[StreamTermination] = pipe.onClose
    }

  /**
   * Drain a [[Reader]] into a [[Transport]]. The inverse of collation.
   */
  def streamOut(
    trans: Transport[Any, Any],
    r: Reader[Chunk],
    contentLength: Option[Long]
  ): Future[Unit] = {

    // A helper to ensure that we don't send netty more (or less) data that the
    // content-length header requires.
    def verifyContentLength(chunk: Option[Chunk], written: Long): Unit = contentLength match {
      case None => // nop: we don't have a content-length header so no constraints
      case Some(contentLength) =>
        chunk match {
          // Short write case: the reader doesn't contain as much data is its
          // content-length header advertised which is an illegal message. We
          // handle this by surfacing an exception that will close the channel.
          case None if contentLength != written =>
            r.discard()
            throw new IllegalStateException(
              s"HTTP stream terminated before enough content was written. " +
                s"Provided content length: ${contentLength}, observed: $written.")

          // Attempting to write trailers which don't honor the content-length
          // header, either too little or too much data.
          case Some(chunk) if chunk.isLast && chunk.content.length + written != contentLength =>
            r.discard()
            throw new IllegalStateException(
              s"HTTP stream terminated with incorrect amount of data written. " +
                s"Provided content length: ${contentLength}, observed: ${chunk.content.length + written}.")

          // Attempting to write a chunk that overflows the length
          // dictated by the content-length header
          case Some(chunk) if contentLength < written + chunk.content.length =>
            r.discard()
            throw new IllegalStateException(
              s"HTTP stream attempted to write more data than the content-length header allows. " +
                s"Provided content length: ${contentLength}, observed (so far): ${written + chunk.content.length}")

          case _ => // nop: nothing illegal observed with this chunk.
        }
    }

    def continue(written: Long): Future[Unit] = r.read().flatMap { chunk =>
      verifyContentLength(chunk, written)
      chunk match {
        case None =>
          trans.write(LastHttpContent.EMPTY_LAST_CONTENT)

        case Some(chunk) if chunk.isLast =>
          terminate(chunk)

        case Some(chunk) =>
          trans
            .write(
              new DefaultHttpContent(ByteBufConversion.bufAsByteBuf(chunk.content))
            ).before(continue(written + chunk.content.length))
      }
    }

    // We need to read one more time before writing last chunk to ensure the stream isn't malformed.
    def terminate(last: Chunk): Future[Unit] = r.read().flatMap {
      case None =>
        // TODO (vk): PR against Netty; we need to construct out of given Headers so we avoid
        // copying afterwards.

        if (last.content.isEmpty && last.trailers.isEmpty) {
          trans.write(LastHttpContent.EMPTY_LAST_CONTENT)
        } else {
          val contentAndTrailers = new DefaultLastHttpContent(
            ByteBufConversion.bufAsByteBuf(last.content),
            false /*validateHeaders*/
          )

          if (!last.trailers.isEmpty) {
            Bijections.finagle
              .writeFinagleHeadersToNetty(last.trailers, contentAndTrailers.trailingHeaders())
          }

          trans.write(contentAndTrailers)
        }

      case _ =>
        Future.exception(
          new IllegalStateException("HTTP stream is malformed: only EOS can follow trailers")
        )
    }

    // Begin the loop.
    continue(written = 0L)
  }
}

private[finagle] class Netty4ServerStreamTransport(rawTransport: Transport[Any, Any])
    extends StreamTransportProxy[Response, Request](rawTransport) {
  import Netty4StreamTransport._

  private[this] val transport =
    Transport.cast[HttpResponse, HttpRequest](rawTransport)

  def write(in: Response): Future[Unit] = {
    val nettyRep =
      if (in.isChunked)
        Bijections.finagle.chunkedResponseToNetty(in)
      else
        Bijections.finagle.fullResponseToNetty(in)

    transport.write(nettyRep).transform {
      case Throw(exc) =>
        wrapWriteException(exc)
      case Return(_) =>
        if (in.isChunked) streamOut(rawTransport, in.chunkReader, in.contentLength)
        else Future.Done
    }
  }

  def read(): Future[Multi[Request]] = {
    transport.read().flatMap {
      case req: FullHttpRequest =>
        val finagleReq = Bijections.netty.fullRequestToFinagle(
          req,
          // We have to match/cast as remoteAddress is stored as SocketAddress but Request's
          // constructor expects InetSocketAddress. In practice, this is always a successful match
          // given all our transports operate on inet addresses.
          transport.context.remoteAddress match {
            case ia: InetSocketAddress => ia
            case _ => new InetSocketAddress(0)
          }
        )
        Future.value(Multi(finagleReq, Future.Done))

      case req: HttpRequest =>
        assert(!req.isInstanceOf[HttpContent]) // chunks are handled via collation

        val coll = streamIn(rawTransport)
        val finagleReq = Bijections.netty.chunkedRequestToFinagle(
          req,
          coll,
          // We have to match/cast as remoteAddress is stored as SocketAddress but Request's
          // constructor expects InetSocketAddress. In practice, this is always a successful match
          // given all our transports operate on inet addresses.
          transport.context.remoteAddress match {
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
  import Netty4StreamTransport._

  def write(in: Request): Future[Unit] = {
    val contentLengthHeader = in.contentLength
    val nettyReq = Bijections.finagle.requestToNetty(in, contentLengthHeader)
    rawTransport.write(nettyReq).transform {
      case Throw(exc) =>
        wrapWriteException(exc)
      case Return(_) =>
        if (in.isChunked) streamOut(rawTransport, in.chunkReader, contentLengthHeader)
        else Future.Done
    }
  }

  def read(): Future[Multi[Response]] = {
    rawTransport.read().flatMap {
      // fully buffered message
      case rep: FullHttpResponse =>
        val finagleRep: Response = Bijections.netty.fullResponseToFinagle(rep)
        Future.value(Multi(finagleRep, Future.Done))

      // chunked message, collate the transport
      case rep: HttpResponse =>
        assert(!rep.isInstanceOf[HttpContent]) // chunks are handled via collation
        val coll = streamIn(rawTransport)
        val finagleRep = Bijections.netty.chunkedResponseToFinagle(rep, coll)
        Future.value(Multi(finagleRep, coll))

      case invalid =>
        // relies on GenSerialClientDispatcher satisfying `p`
        Future.exception(new IllegalArgumentException(s"invalid message '$invalid'"))
    }
  }
}
