package com.twitter.finagle.stream

import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.http._

import com.twitter.concurrent.Offer
import com.twitter.util.{Try, Return, Throw}
import com.twitter.finagle.netty3.Conversions._

import com.twitter.finagle.channel.BrokerChannelHandler

/**
 * Convert a StreamResponse into a chunked (streaming) HTTP response.
 */
class HttpChunker extends BrokerChannelHandler {
  // this process is always entered with downstream events already proxied.
  private[this] def write(
    ctx: ChannelHandlerContext,
    res: StreamResponse,
    ack: Option[Offer[Try[Unit]]] = None)
  {
    def close() {
      res.release()
      if (ctx.getChannel.isOpen) ctx.getChannel.close()
      upstreamEvent foreach {
        case Message(_, _) => /* drop */
        case e => e.sendUpstream()
      }
    }

    Offer.select(
      ack match {
        // if we're awaiting an ack, don't offer to synchronize
        // on messages. thus we exert backpressure.
        case Some(ack) =>
          ack {
            case Return(_) =>
              write(ctx, res, None)
            case Throw(_) =>
              close()
          }

        case None =>
          res.messages { bytes =>
            val chunk = new DefaultHttpChunk(bytes)
            val writeFuture = Channels.future(ctx.getChannel)
            Channels.write(ctx, writeFuture, chunk)
            write(ctx, res, Some(writeFuture.toTwitterFuture.toOffer))
         }
     },

     res.error { _ =>
       val future = Channels.future(ctx.getChannel)
       val trailer =
         new DefaultHttpChunkTrailer {
           override def isLast(): Boolean = res.httpResponse.isChunked
         }
       Channels.write(ctx, future, trailer)
       future {
         // Close only after we sucesfully write the trailer.
         // todo: can this be a source of resource leaks?
         case _ => close()
       }
     },

     upstreamEvent {
       case Message(_, _) =>
         // A pipelined request. We don't support this,
         // and will just drop it.
         write(ctx, res)
       case e@(Closed(_, _) | Disconnected(_, _)) =>
         e.sendUpstream()
         close()
       case e =>
         e.sendUpstream()
         write(ctx, res)
     }
    )
  }

  private[this] def awaitResponse(dead: Boolean) {
    Offer.select(
      downstreamEvent {
        case e@WriteValue(res: StreamResponse, ctx) if !dead =>
          val httpRes = res.httpResponse
          val chunked = httpRes.getProtocolVersion == HttpVersion.HTTP_1_1 && httpRes.getHeader(HttpHeaders.Names.CONTENT_LENGTH) == null
          httpRes.setChunked(chunked)
          if (chunked)
            HttpHeaders.setHeader(httpRes, HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED)
          else
            HttpHeaders.setHeader(httpRes, HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE)

          val writeComplete = Channels.future(ctx.getChannel)
          Channels.write(ctx, writeComplete, httpRes)
          writeComplete.proxyTo(e.e.getFuture)
          proxyDownstream()
          write(ctx, res)

        case WriteValue(res: StreamResponse, _) if dead =>
          res.release()
          awaitResponse(dead)

        case WriteValue(invalid, ctx) =>
          Channels.fireExceptionCaught(ctx,
            new IllegalArgumentException(
              "Invalid reply \"%s\"".format(invalid)))
          awaitResponse(dead)

        case e@Close(_, _) =>
          e.sendDownstream()
          awaitResponse(true)

        case e =>
          e.sendDownstream()
          awaitResponse(dead)
      },

      upstreamEvent { e =>
        e.sendUpstream()
        awaitResponse(dead)
      }
    )
  }

  awaitResponse(false)
}
