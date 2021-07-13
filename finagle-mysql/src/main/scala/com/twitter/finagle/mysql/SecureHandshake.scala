package com.twitter.finagle.mysql

import com.twitter.finagle.mysql.transport.Packet
import com.twitter.finagle.netty4.ssl.client.Netty4ClientSslChannelInitializer
import com.twitter.finagle.netty4.ssl.client.Netty4ClientSslChannelInitializer.OnSslHandshakeComplete
import com.twitter.finagle.netty4.transport.ChannelTransportContext
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.finagle.Stack
import com.twitter.finagle.param.OppTls
import com.twitter.finagle.ssl.OpportunisticTls
import com.twitter.util.{Future, Promise, Try}
import io.netty.channel.Channel

private[mysql] final class SecureHandshake(
  params: Stack.Params,
  transport: Transport[Packet, Packet])
    extends Handshake(params, transport) {

  private[this] val tlsLevel = params[OppTls].level.getOrElse(OpportunisticTls.Required)

  private[this] def onHandshakeComplete(p: Promise[Unit])(result: Try[Unit]): Unit =
    p.updateIfEmpty(result)

  /**
   * Returns modified stack params based on the initial server handshake message.
   *
   * @note We purposefully do not set an interrupt handler on this promise, since
   *       there is no meaningful way to gracefully interrupt an SSL/TLS handshake.
   *       This is very similar to how SSL/TLS negotiation works in finagle-mux.
   *       See `Negotiation` for more details.
   * @note We call the `HandshakeModifier` to make configuration decisions based on values
   *       only known at connection time. For example, community version 5.x of MySQL uses yaSSL,
   *       a non-standard security library. yaSSL only supports up to TLSv1.1 and fails to negotiate
   *       TLS properly when version TLSv1.2 is specified as an option. Therefore, for versions of
   *       MySQL using yaSSL, we only want to specify TLSv1.1, but we want to specify TLSv1.2 in all
   *       other cases.
   *
   * @param handshakeInit the initial server handshake message
   * @param p the promise updated on SSL handshake completion
   * @return the stack params modified according to the handshake message
   */
  // Visible for testing
  private[finagle] def getTlsParams(handshakeInit: HandshakeInit, p: Promise[Unit]): Stack.Params =
    params[HandshakeStackModifier].modifyParams(params, handshakeInit) + OnSslHandshakeComplete(
      onHandshakeComplete(p))

  private[this] def negotiateTls(handshakeInit: HandshakeInit): Future[Unit] = {
    val p = new Promise[Unit]
    val tlsParams = getTlsParams(handshakeInit, p)
    val context: TransportContext = transport.context
    context match {
      case ctContext: ChannelTransportContext =>
        val channel: Channel = ctContext.ch
        channel.pipeline.addFirst("mysqlSslInit", new Netty4ClientSslChannelInitializer(tlsParams))
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
      settings.maxPacketSize.inBytes.toInt,
      settings.enableCachingSha2PasswordAuth
    )

  private[this] def makePlainHandshakeResponse(handshakeInit: HandshakeInit): HandshakeResponse = {
    PlainHandshakeResponse(
      settings.username,
      settings.password,
      settings.database,
      settings.calculatedClientCapabilities,
      handshakeInit.salt,
      handshakeInit.serverCapabilities,
      settings.charset,
      settings.maxPacketSize.inBytes.toInt,
      settings.enableCachingSha2PasswordAuth
    )
  }

  private[this] def initiateAuthNegotiation(
    handshakeInit: HandshakeInit,
    secure: Boolean
  ): Future[Result] = {
    val handshakeResponse =
      if (secure) {
        makeSecureHandshakeResponse(handshakeInit)
      } else {
        makePlainHandshakeResponse(handshakeInit)
      }
    val authInfo =
      AuthInfo(handshakeInit.version, settings, fastAuthSuccessCounter, tlsEnabled = secure)

    new AuthNegotiation(transport, decodeSimpleResult).doAuth(handshakeResponse, authInfo)
  }

  // For the `SecureHandshake`, after the init,
  // we return an `SslConnectionRequest`,
  // negotiate SSL/TLS, and then return a handshake response.
  def connectionPhase(): Future[Result] = {
    readHandshakeInit()
      .flatMap { handshakeInit =>
        if (tlsLevel == OpportunisticTls.Off) {
          initiateAuthNegotiation(handshakeInit, secure = false)
        } else {
          val serverTlsEnabled = handshakeInit.serverCapabilities.has(Capability.SSL)
          if (serverTlsEnabled) {
            writeSslConnectionRequest(handshakeInit)
              .flatMap(_ => negotiateTls(handshakeInit))
              .map(_ => handshakeInit)
              .flatMap(initiateAuthNegotiation(_, secure = true))
          } else if (tlsLevel == OpportunisticTls.Desired) {
            initiateAuthNegotiation(handshakeInit, secure = false)
          } else {
            Future.exception(
              new InsufficientServerCapabilitiesException(
                required = Capability(Capability.SSL),
                available = handshakeInit.serverCapabilities
              )
            )
          }
        }
      }
      .onFailure(_ => transport.close())
  }
}
