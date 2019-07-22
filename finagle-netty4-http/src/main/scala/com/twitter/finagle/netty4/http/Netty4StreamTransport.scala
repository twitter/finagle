package com.twitter.finagle.netty4.http

import com.twitter.finagle.dispatch.ClientDispatcher.wrapWriteException
import com.twitter.finagle.http._
import com.twitter.finagle.http.exp.{Multi, StreamTransportProxy}
import com.twitter.finagle.netty4.ByteBufConversion
import com.twitter.finagle.transport.Transport
import com.twitter.io.{Pipe, Reader, ReaderDiscardedException, StreamTermination}
import com.twitter.util._
import io.netty.buffer.Unpooled
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
  def streamOut(trans: Transport[Any, Any], r: Reader[Chunk]): Future[Unit] = {
    def continue(): Future[Unit] = r.read().flatMap {
      case None =>
        trans.write(LastHttpContent.EMPTY_LAST_CONTENT)

      case Some(chunk) if chunk.isLast =>
        // Chunk.trailers produces an empty last content so we check against it here
        // and potentially short-circuit to termination.
        if (chunk.content.isEmpty) terminate(chunk.trailers)
        else
          trans
            .write(
              new DefaultHttpContent(ByteBufConversion.bufAsByteBuf(chunk.content))
            ).before(terminate(chunk.trailers))

      case Some(chunk) =>
        trans
          .write(
            new DefaultHttpContent(ByteBufConversion.bufAsByteBuf(chunk.content))
          ).before(continue())
    }

    // We need to read one more time before writing trailers to ensure HTTP stream isn't malformed.
    def terminate(trailers: HeaderMap): Future[Unit] = r.read().flatMap {
      case None =>
        // TODO (vk): PR against Netty; we need to construct out of given Headers so we avoid
        // copying afterwards.
        val last =
          if (trailers.isEmpty) LastHttpContent.EMPTY_LAST_CONTENT
          else {
            val content =
              new DefaultLastHttpContent(Unpooled.EMPTY_BUFFER, false /*validateHeaders*/ )

            Bijections.finagle.writeFinagleHeadersToNetty(trailers, content.trailingHeaders())
            content
          }

        trans.write(last)

      case _ =>
        Future.exception(
          new IllegalStateException("HTTP stream is malformed: only EOS can follow trailers")
        )
    }

    continue()
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
        if (in.isChunked) streamOut(rawTransport, in.chunkReader)
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
    val nettyReq = Bijections.finagle.requestToNetty(in)
    rawTransport.write(nettyReq).transform {
      case Throw(exc) =>
        wrapWriteException(exc)
      case Return(_) =>
        if (in.isChunked) streamOut(rawTransport, in.chunkReader)
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
