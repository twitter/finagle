package com.twitter.finagle.mysql

import com.twitter.finagle.mysql.transport.{MysqlBuf, Packet}
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Future, Return, Throw}

/**
 * A base class for exceptions related to client incompatibility with an
 * upstream MySQL server.
 */
class IncompatibleServerError(msg: String) extends Exception(msg)

/**
 * Indicates that the server to which the client is connected is running
 * a version of MySQL that the client is incompatible with.
 */
case object IncompatibleVersion
    extends IncompatibleServerError(
      "This client is only compatible with MySQL version 4.1 and later"
    )

/**
 * Indicates that the server to which the client is connected is configured to use
 * a charset that the client is incompatible with.
 */
case object IncompatibleCharset
    extends IncompatibleServerError(
      "This client is only compatible with UTF-8 and Latin-1 charset encoding"
    )

/**
 * A `Handshake` is responsible for using an open `Transport`
 * to a MySQL server to establish a MySQL session by performing
 * the `Connection Phase` of the MySQL Client/Server Protocol.
 *
 * https://dev.mysql.com/doc/internals/en/connection-phase.html
 *
 * @note At this time, the `Handshake` class only supports a
 * `Plain Handshake`.
 *
 * @param settings A [[HandshakeSettings]] collection of MySQL
 * specific settings.
 *
 * @param transport A `Transport` connected to a MySQL server which
 * understands the reading and writing of MySQL packets.
 */
private[mysql] final class Handshake(
  settings: HandshakeSettings,
  transport: Transport[Packet, Packet]) {

  private[this] def isCompatibleVersion(init: HandshakeInit) =
    if (init.serverCap.has(Capability.Protocol41)) Return.True
    else Throw(IncompatibleVersion)

  private[this] def isCompatibleCharset(init: HandshakeInit) =
    if (MysqlCharset.isCompatible(init.charset)) Return.True
    else Throw(IncompatibleCharset)

  private[this] def simpleDispatch(req: Request): Future[Result] = {
    for {
      _ <- transport.write(req.toPacket)
      packet <- transport.read()
      result <- decodeSimpleResult(packet)
    } yield result
  }

  private[this] def decodeSimpleResult(packet: Packet): Future[Result] =
    MysqlBuf.peek(packet.body) match {
      case Some(Packet.OkByte) => LostSyncException.const(OK(packet))
      case Some(Packet.ErrorByte) =>
        LostSyncException.const(Error(packet)).flatMap { err =>
          Future.exception(ServerError(err.code, err.sqlState, err.message))
        }
      case _ => LostSyncException.AsFuture
    }

  private[this] def readHandshakeInit(): Future[HandshakeInit] = {
    for {
      packet <- transport.read()
      handshakeInit <- LostSyncException.const(HandshakeInit(packet))
    } yield handshakeInit
  }

  private[this] def makePlainHandshakeResponse(
    handshakeInit: HandshakeInit
  ): Future[HandshakeResponse] = {
    for {
      _ <- LostSyncException.const(isCompatibleVersion(handshakeInit))
      _ <- LostSyncException.const(isCompatibleCharset(handshakeInit))
    } yield
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

  /**
   * Performs the connection phase. The phase should only be performed
   * once before any other exchange between the client/server. A failure
   * to handshake renders a service unusable.
   * [[https://dev.mysql.com/doc/internals/en/connection-phase.html]]
   */
  def connectionPhase(): Future[Result] = {
    val futResult = for {
      handshakeInit <- readHandshakeInit()
      handshakeResponse <- makePlainHandshakeResponse(handshakeInit)
      result <- simpleDispatch(handshakeResponse)
    } yield result
    futResult.onFailure(_ => transport.close())
  }

}
