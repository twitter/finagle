package com.twitter.finagle.mysql

import com.twitter.finagle.mysql.transport.{MysqlBuf, Packet}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.Stack
import com.twitter.finagle.param.Stats
import com.twitter.finagle.stats.{Counter, Verbosity}
import com.twitter.util.{Future, Return, Throw, Try}

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
 * @param params The collection `Stack` params necessary to create the
 * desired MySQL session.
 *
 * @param transport A `Transport` connected to a MySQL server which
 * understands the reading and writing of MySQL packets.
 */
private[mysql] abstract class Handshake(
  params: Stack.Params,
  transport: Transport[Packet, Packet]) {

  protected final val settings = HandshakeSettings(params)

  // To test that fastAuthSuccess has been achieved when
  // connecting to mysql with a user that has a password
  protected val fastAuthSuccessCounter: Counter =
    params[Stats].statsReceiver.counter(Verbosity.Debug, "fast_auth_success")

  private[this] def isCompatibleVersion(init: HandshakeInit): Try[Boolean] =
    if (init.serverCapabilities.has(Capability.Protocol41)) Return.True
    else Throw(IncompatibleVersion)

  private[this] def isCompatibleCharset(init: HandshakeInit): Try[Boolean] =
    if (MysqlCharset.isCompatible(init.charset)) Return.True
    else Throw(IncompatibleCharset)

  protected final def verifyCompatibility(handshakeInit: HandshakeInit): Future[HandshakeInit] =
    LostSyncException.const(isCompatibleVersion(handshakeInit)).flatMap { _ =>
      LostSyncException.const(isCompatibleCharset(handshakeInit)).map(_ => handshakeInit)
    }

  protected final def decodeSimpleResult(packet: Packet): Future[Result] =
    MysqlBuf.peek(packet.body) match {
      case Some(Packet.OkByte) => LostSyncException.const(OK(packet))
      case Some(Packet.ErrorByte) =>
        LostSyncException.const(Error(packet)).flatMap { err =>
          Future.exception(ServerError(err.code, err.sqlState, err.message))
        }
      // During the handshake this is the AuthSwitchRequest
      case Some(Packet.EofByte) =>
        LostSyncException.const(AuthSwitchRequest(packet))
      case Some(Packet.AuthMoreDataByte) =>
        LostSyncException.const(AuthMoreDataFromServer(packet))
      case _ => LostSyncException.AsFuture
    }

  protected final def readHandshakeInit(): Future[HandshakeInit] =
    transport
      .read()
      .flatMap(packet => LostSyncException.const(HandshakeInit(packet)))
      .flatMap(verifyCompatibility)

  /**
   * Performs the connection phase. The phase should only be performed
   * once before any other exchange between the client/server. A failure
   * to handshake renders a service unusable.
   * [[https://dev.mysql.com/doc/internals/en/connection-phase.html]]
   */
  def connectionPhase(): Future[Result]

}

private[mysql] object Handshake {

  /**
   * Creates a `Handshake` based on the specific `Stack` params and `Transport` passed in.
   * If the `Transport.ClientSsl` param is set, then a `SecureHandshake` will be returned.
   * Otherwise a `PlainHandshake` is returned.
   */
  def apply(params: Stack.Params, transport: Transport[Packet, Packet]): Handshake =
    if (params[Transport.ClientSsl].sslClientConfiguration.isDefined) {
      new SecureHandshake(params, transport)
    } else {
      new PlainHandshake(params, transport)
    }

}
