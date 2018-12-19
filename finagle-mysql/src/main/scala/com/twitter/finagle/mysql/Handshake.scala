package com.twitter.finagle.mysql

import com.twitter.conversions.StorageUnitOps._
import com.twitter.finagle.Stack
import com.twitter.finagle.mysql.MysqlCharset.Utf8_general_ci
import com.twitter.finagle.mysql.param.{Charset, Credentials, Database, FoundRows}
import com.twitter.util.{Return, StorageUnit, Throw, Try}

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

private[mysql] object Handshake {

  /**
   * Creates a Handshake from a collection of [[com.twitter.finagle.Stack.Params]].
   */
  def apply(prms: Stack.Params): Handshake = {
    val credentials = prms[Credentials]
    Handshake(
      username = credentials.username,
      password = credentials.password,
      database = prms[Database].db,
      charset = prms[Charset].charset,
      enableFoundRows = prms[FoundRows].enabled
    )
  }
}

/**
 * Bridges a server handshake (HandshakeInit) with a
 * client handshake (HandshakeResponse) using the given
 * parameters. This facilitates the connection phase of
 * the mysql protocol.
 *
 * @param username MySQL username used to login.
 *
 * @param password MySQL password used to login.
 *
 * @param database initial database to use for the session.
 *
 * @param clientCap The capability this client has.
 *
 * @param charset default character established with the server.
 *
 * @param enableFoundRows if the server should return the number
 * of found (matched) rows, not the number of changed rows for
 * UPDATE and INSERT ... ON DUPLICATE KEY UPDATE statements.
 *
 * @param maxPacketSize max size of a command packet that the
 * client intends to send to the server. The largest possible
 * packet that can be transmitted to or from a MySQL 5.5 server or
 * client is 1GB.
 *
 * @return A Try[HandshakeResponse] that encodes incompatibility
 * with the server.
 */
private[mysql] case class Handshake(
  username: Option[String] = None,
  password: Option[String] = None,
  database: Option[String] = None,
  clientCap: Capability = Capability.baseCap,
  charset: Short = Utf8_general_ci,
  enableFoundRows: Boolean = true,
  maxPacketSize: StorageUnit = 1.gigabyte)
    extends (HandshakeInit => Try[HandshakeResponse]) {
  require(maxPacketSize <= 1.gigabyte, "max packet size can't exceed 1 gigabyte")

  private[this] val newClientCap = {
    val capDb = if (database.isDefined) {
      clientCap + Capability.ConnectWithDB
    } else {
      clientCap - Capability.ConnectWithDB
    }

    if (enableFoundRows) capDb + Capability.FoundRows
    else capDb - Capability.FoundRows
  }

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
        username,
        password,
        database,
        newClientCap,
        init.salt,
        init.serverCap,
        charset,
        maxPacketSize.inBytes.toInt
      )
  }
}
