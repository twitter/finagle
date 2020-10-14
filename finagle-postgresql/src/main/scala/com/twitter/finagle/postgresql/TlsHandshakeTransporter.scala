package com.twitter.finagle.postgresql

import java.net.SocketAddress

import com.twitter.finagle.Stack
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.decoder.Framer
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.netty4.codec.BufCodec
import com.twitter.finagle.netty4.decoder.DecoderHandler
import com.twitter.finagle.netty4.ssl.client.Netty4ClientSslChannelInitializer
import com.twitter.finagle.netty4.ssl.client.Netty4ClientSslChannelInitializer.OnSslHandshakeComplete
import com.twitter.finagle.netty4.transport.ChannelTransportContext
import com.twitter.finagle.postgresql.transport.MessageEncoder
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.transport.TransportContext
import com.twitter.io.Buf
import com.twitter.util.Future
import com.twitter.util.Promise
import io.netty.channel.Channel

class TlsHandshakeTransporter(
  val remoteAddress: SocketAddress,
  params: Stack.Params,
  framer: Framer,
) extends Transporter[Buf, Buf, TransportContext] {

  /** Unfortunately, the Postgres protocol doesn't use its standard packet format
   * during TLS negotiation. It responds simply with a single, unframed byte.
   *
   * So to compensate, we insert the framer manually into the netty pipeline after TLS negotiation.
   */
  private[this] val netty4Transporter =
    Netty4Transporter.framedBuf(
      None, // skip the framer during tls handshake
      remoteAddress,
      params + Transport.ClientSsl(None) // ensure no Tls params
    )

  override def apply(): Future[Transport[Buf, Buf] {
    type Context <: TransportContext
  }] = {
    netty4Transporter().flatMap { transport =>
      transport
        .write(MessageEncoder.sslRequestEncoder.toPacket(FrontendMessage.SslRequest).toBuf)
        .flatMap { _ =>
          transport.read()
        }
        .flatMap { buf =>
          buf.get(0) match {
            case 'S' => negotiateTls(transport)
            case 'N' => Future.exception(PgSqlTlsUnsupportedError)
            case b => Future.exception(new IllegalStateException(s"invalid server response to SslRequest: $b"))
          }
        }
        .map { _ =>
          transport
        }
    }
  }

  private[this] def negotiateTls(transport: Transport[Buf, Buf]): Future[Unit] = {
    val p = new Promise[Unit]
    val sslParams = params + OnSslHandshakeComplete(result => p.updateIfEmpty(result))
    val context: TransportContext = transport.context
    context match {
      case ctContext: ChannelTransportContext =>
        val channel: Channel = ctContext.ch
        channel.pipeline.addFirst("pgSqlSslInit", new Netty4ClientSslChannelInitializer(sslParams))

        // Manually add the framer in the pipeline where [[Netty4ClientChannelInitializer]] would have inserted it
        ctContext.ch.pipeline.addAfter(BufCodec.Key, "decoder", new DecoderHandler(framer))
        p
      case other =>
        Future.exception(
          new IllegalStateException(s"TlsHandshake requires a channel to negotiate SSL/TLS. Found: $other")
        )
    }
  }
}
