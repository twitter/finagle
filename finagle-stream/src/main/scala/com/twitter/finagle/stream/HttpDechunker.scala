package com.twitter.finagle.stream

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.http._
import com.twitter.finagle.channel.BrokerChannelHandler
import com.twitter.concurrent.{Offer, Broker}

/**
 * Convert a streaming HTTP response into a StreamResponse.
 */
class HttpDechunker extends BrokerChannelHandler {
  private[this] def error(err: Broker[Throwable], t: Throwable) {
    err ! t
    proxyUpstream()
  }

  // read chunks from the channel, translating
  // each into a message on the `out' broker.
  private[this] def read(
    ch: Channel,
    out: Broker[ChannelBuffer],
    err: Broker[Throwable],
    close: Offer[Unit]
  ) {
    Offer.select(
      close { _=>
        ch.close()
        error(err, EOF)
      },
      upstreamEvent {
        case MessageValue(chunk: HttpChunk, _) =>
          val content = chunk.getContent

          val sendOf =
            if (content.readable) out.send(content) else Offer.const(())

          if (chunk.isLast) {
            ch.close()
            sendOf.sync() ensure { error(err, EOF) }
          } else {
            ch.setReadable(false)
            sendOf.sync() ensure {
              ch.setReadable(true)
              read(ch, out, err, close)
            }
          }

        case MessageValue(invalid, ctx) =>
          Channels.fireExceptionCaught(ctx,
            new IllegalArgumentException(
              "invalid message \"%s\"".format(invalid)))
          proxyUpstream()

        case e@(Closed(_, _) | Disconnected(_, _)) =>
          e.sendUpstream()
          error(err, EOF)

        case e@Exception(exc, ctx) =>
          e.sendUpstream()
          error(err, exc.getCause)

        case e =>
          e.sendUpstream()
          read(ch, out, err, close)
      }
    )
  }

  // listen for the first event.  if it is an HttpResponse, we wrap it
  // in a StreamResponse, and dispatch further chunks on the
  // associated broker.
  private[this] def awaitReply() {
    (upstreamEvent?) foreach {
      case MessageValue(httpRes: HttpResponse, ctx) =>
        val out = new Broker[ChannelBuffer]
        val err = new Broker[Throwable]
        val close = new Broker[Unit]

        if (!httpRes.isChunked) {
          val content = httpRes.getContent
          if (content.readable) out ! content
        }

        val res = new StreamResponse {
          val httpResponse = httpRes
          val messages = out.recv
          def error = err.recv
          def release() { close ! () }
        }

        Channels.fireMessageReceived(ctx, res)
        read(ctx.getChannel, out, err, close.recv)

      case MessageValue(invalid, ctx) =>
        Channels.fireExceptionCaught(
          ctx, new IllegalArgumentException(
            "invalid message \"%s\"".format(invalid)))
        proxyUpstream()

      case e =>
        e.sendUpstream()
        awaitReply()
    }
  }

  proxyDownstream()
  awaitReply()
}
