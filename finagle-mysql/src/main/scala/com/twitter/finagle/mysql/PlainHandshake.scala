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
      settings.calculatedClientCap,
      handshakeInit.salt,
      handshakeInit.serverCap,
      settings.charset,
      settings.maxPacketSize.inBytes.toInt
    )
  }

  def connectionPhase(): Future[Result] =
    readHandshakeInit()
      .map(makePlainHandshakeResponse)
      .flatMap(messageDispatch)
      .onFailure(_ => transport.close())

}
