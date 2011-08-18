package com.twitter.finagle.stream

import java.util.concurrent.atomic.AtomicReference
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.http._

import com.twitter.concurrent.Offer
import com.twitter.util.{Try, Return, Throw}
import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.util.{Cancelled, Ok, Error}

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
      proxyUpstream()
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
       Channels.write(ctx, future, new DefaultHttpChunkTrailer)
       future {
         // Close only after we sucesfully write the trailer.
         // todo: can this be a source of resource leaks?
         case _ => close()
       }
     },

     upstreamEvent {
       case e@(Closed(_, _) | Disconnected(_, _)) =>
         e.sendUpstream()
         close()
       case e =>
         e.sendUpstream()
         write(ctx, res)
     }
    )
  }

  private[this] def awaitResponse() {
    Offer.select(
      downstreamEvent {
        case e@WriteValue(res: StreamResponse, ctx) =>
          val httpRes = res.httpResponse
          httpRes.setChunked(true)
          HttpHeaders.setHeader(httpRes, "Transfer-Encoding", "Chunked")

          val writeComplete = Channels.future(ctx.getChannel)
          Channels.write(ctx, writeComplete, httpRes)
          writeComplete.proxyTo(e.e.getFuture)

          proxyDownstream()
          write(ctx, res)

        case WriteValue(invalid, ctx) =>
          Channels.fireExceptionCaught(ctx,
            new IllegalArgumentException(
              "Invalid reply \"%s\"".format(invalid)))
          proxy()

        case e@Close(_, _) =>
          e.sendDownstream()
          proxy()

        case e =>
          e.sendDownstream()
          awaitResponse()
      },

      upstreamEvent { e =>
        e.sendUpstream()
        awaitResponse()
      }
    )
  }

  awaitResponse()
}
