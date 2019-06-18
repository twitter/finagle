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
      settings.maxPacketSize.inBytes.toInt
    )
  }

  // For the `PlainHandshake`, after the init
  // we just return a handshake response.
  def connectionPhase(): Future[Result] =
    readHandshakeInit()
      .map(makePlainHandshakeResponse)
      .flatMap(messageDispatch)
      .onFailure(_ => transport.close())

}
