package com.twitter.finagle.mysql

import com.twitter.finagle.mysql.transport.Packet
import com.twitter.finagle.netty4.ssl.client.Netty4ClientSslChannelInitializer
import com.twitter.finagle.netty4.ssl.client.Netty4ClientSslChannelInitializer.OnSslHandshakeComplete
import com.twitter.finagle.netty4.transport.ChannelTransportContext
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.finagle.Stack
import com.twitter.util.{Future, Promise, Try}
import io.netty.channel.Channel

private[mysql] final class SecureHandshake(
  params: Stack.Params,
  transport: Transport[Packet, Packet])
    extends Handshake(params, transport) {

  private[this] def onHandshakeComplete(p: Promise[Unit])(result: Try[Unit]): Unit =
    p.updateIfEmpty(result)

  // We purposefully do not set an interrupt handler on this promise, since
  // there is no meaningful way to gracefully interrupt an SSL/TLS handshake.
  // This is very similar to how SSL/TLS negotiation works in finagle-mux.
  // See `Negotiation` for more details.
  private[this] def negotiateTls(): Future[Unit] = {
    val p = new Promise[Unit]
    val sslParams = params + OnSslHandshakeComplete(onHandshakeComplete(p))
    val context: TransportContext = transport.context
    context match {
      case ctContext: ChannelTransportContext =>
        val channel: Channel = ctContext.ch
        channel.pipeline.addFirst("mysqlSslInit", new Netty4ClientSslChannelInitializer(sslParams))
        p
      case other =>
        Future.exception(
          new IllegalStateException(
            s"SecureHandshake requires a channel to negotiate SSL/TLS. Found: $other"))
    }
  }

  private[this] def writeSslConnectionRequest(handshakeInit: HandshakeInit): Future[Unit] = {
    val request = SslConnectionRequest(
      settings.sslCalculatedClientCapabilities,
      settings.charset,
      settings.maxPacketSize.inBytes.toInt)
    transport.write(request.toPacket)
  }

  private[this] def makeSecureHandshakeResponse(handshakeInit: HandshakeInit): HandshakeResponse =
    SecureHandshakeResponse(
      settings.username,
      settings.password,
      settings.database,
      settings.sslCalculatedClientCapabilities,
      handshakeInit.salt,
      handshakeInit.serverCapabilities,
      settings.charset,
      settings.maxPacketSize.inBytes.toInt
    )

  // For the `SecureHandshake`, after the init,
  // we return an `SslConnectionRequest`,
  // neogtiate SSL/TLS, and then return a handshake response.
  def connectionPhase(): Future[Result] = {
    readHandshakeInit()
      .flatMap { handshakeInit =>
        writeSslConnectionRequest(handshakeInit)
          .flatMap(_ => negotiateTls())
          .map(_ => handshakeInit)
      }
      .map(makeSecureHandshakeResponse)
      .flatMap(messageDispatch)
      .onFailure(_ => transport.close())
  }

}
