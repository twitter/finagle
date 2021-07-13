package com.twitter.finagle.mysql

import com.twitter.finagle.mysql.transport.Packet
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.Stack
import com.twitter.util.Future

private[mysql] final class PlainHandshake(
  params: Stack.Params,
  transport: Transport[Packet, Packet])
    extends Handshake(params, transport) {

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

  private[this] def initiateAuthNegotiation(handshakeInit: HandshakeInit): Future[Result] = {
    val handshakeResponse = makePlainHandshakeResponse(handshakeInit)
    val authInfo = AuthInfo(handshakeInit.version, settings, fastAuthSuccessCounter)
    new AuthNegotiation(transport, decodeSimpleResult).doAuth(handshakeResponse, authInfo)
  }

  // For the `PlainHandshake`, after the init
  // we just return a handshake response.
  def connectionPhase(): Future[Result] =
    readHandshakeInit()
      .flatMap(initiateAuthNegotiation)
      .onFailure(_ => transport.close())

}
