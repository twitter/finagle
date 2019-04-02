package com.twitter.finagle.mysql

import com.twitter.util.{Return, Throw, Try}

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
 * Bridges a server handshake (HandshakeInit) with a
 * client handshake (HandshakeResponse) using the given
 * parameters. This facilitates the connection phase of
 * the mysql protocol.
 *
 * @param settings A [[HandshakeSettings]] collection of MySQL
 * specific settings.
 *
 * @return A Try[HandshakeResponse] that encodes incompatibility
 * with the server.
 */
private[mysql] final class Handshake(settings: HandshakeSettings)
    extends (HandshakeInit => Try[HandshakeResponse]) {

  private[this] def isCompatibleVersion(init: HandshakeInit) =
    if (init.serverCap.has(Capability.Protocol41)) Return.True
    else Throw(IncompatibleVersion)

  private[this] def isCompatibleCharset(init: HandshakeInit) =
    if (MysqlCharset.isCompatible(init.charset)) Return.True
    else Throw(IncompatibleCharset)

  def apply(init: HandshakeInit): Try[HandshakeResponse] = {
    for {
      _ <- isCompatibleVersion(init)
      _ <- isCompatibleCharset(init)
    } yield
      HandshakeResponse(
        settings.username,
        settings.password,
        settings.database,
        settings.calculatedClientCap,
        init.salt,
        init.serverCap,
        settings.charset,
        settings.maxPacketSize.inBytes.toInt
      )
  }
}
