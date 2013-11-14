package com.twitter.finagle.exp.mysql

import com.twitter.util.{Return, StorageUnit, Throw, Try}
import com.twitter.conversions.storage._

class IncompatibleServerError(msg: String)
  extends Exception(msg)

case object IncompatibleVersion
  extends IncompatibleServerError(
    "This client is only compatible with MySQL version 4.1 and later"
  )

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
 * @param maxPacketSize max size of a command packet that the
 * client intends to send to the server. The largest possible
 * packet that can be transmitted to or from a MySQL 5.5 server or
 * client is 1GB.
 *
 * @return A Try[HandshakeResponse] that encodes incompatibility
 * with the server.
 */
case class Handshake(
  username: Option[String] = None,
  password: Option[String] = None,
  database: Option[String] = None,
  clientCap: Capability = Capability.baseCap,
  charset: Short = Charset.Utf8_general_ci,
  maxPacketSize: StorageUnit = 1.gigabyte
) extends (HandshakeInit => Try[HandshakeResponse]) {
  import Capability._
  require(maxPacketSize <= 1.gigabyte, "max packet size can't exceed 1 gigabyte")

  private[this] val newClientCap =
    if (database.isDefined) clientCap + ConnectWithDB
    else clientCap - ConnectWithDB

  private[this] def isCompatibleVersion(init: HandshakeInit) =
    if (init.serverCap.has(Capability.Protocol41)) Return(true)
    else Throw(IncompatibleVersion)

  private[this] def isCompatibleCharset(init: HandshakeInit) =
    if (Charset.isCompatible(init.charset)) Return(true)
    else Throw(IncompatibleCharset)

  def apply(init: HandshakeInit) = {
    for {
      _ <- isCompatibleVersion(init)
      _ <- isCompatibleCharset(init)
    } yield HandshakeResponse(
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