package com.twitter.finagle.mysql

import com.twitter.conversions.storage._
import com.twitter.finagle.Stack
import com.twitter.finagle.mysql.Charset.Utf8_general_ci
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

object Handshake {

  /**
   * A class eligible for configuring a mysql client's credentials during
   * the Handshake phase.
   */
  case class Credentials(username: Option[String], password: Option[String])
  implicit object Credentials extends Stack.Param[Credentials] {
    val default: Credentials = Credentials(None, None)

    override def show(p: Credentials): Seq[(String, () => String)] = {
      // do not show the password for security reasons
      Seq(("username", () => p.username.getOrElse("")))
    }
  }

  /**
   * A class eligible for configuring a mysql client's database during
   * the Handshake phase.
   */
  case class Database(db: Option[String])
  implicit object Database extends Stack.Param[Database] {
    val default: Database = Database(None)
  }

  /**
   * A class eligible for configuring a mysql client's charset during
   * the Handshake phase.
   */
  case class Charset(charset: Short)
  implicit object Charset extends Stack.Param[Charset] {
    val default: Charset = Charset(Utf8_general_ci)
  }

  /**
    * A class eligible for configuring a mysql client's CLIENT_FOUND_ROWS flag
    * during the Handshake phase.
    */
  case class FoundRows(enabled: Boolean)
  implicit object FoundRows extends Stack.Param[FoundRows] {
    val default: FoundRows = FoundRows(true)
  }

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
case class Handshake(
  username: Option[String] = None,
  password: Option[String] = None,
  database: Option[String] = None,
  clientCap: Capability = Capability.baseCap,
  charset: Short = Utf8_general_ci,
  enableFoundRows: Boolean = true,
  maxPacketSize: StorageUnit = 1.gigabyte
) extends (HandshakeInit => Try[HandshakeResponse]) {
  import Capability._
  require(maxPacketSize <= 1.gigabyte, "max packet size can't exceed 1 gigabyte")

  private[this] val newClientCap = {
    val capDb = if (database.isDefined) {
      clientCap + ConnectWithDB
    } else {
      clientCap - ConnectWithDB
    }

    if (enableFoundRows) capDb + FoundRows
    else capDb - FoundRows
  }

  private[this] def isCompatibleVersion(init: HandshakeInit) =
    if (init.serverCap.has(Capability.Protocol41)) Return.True
    else Throw(IncompatibleVersion)

  private[this] def isCompatibleCharset(init: HandshakeInit) =
    if (Charset.isCompatible(init.charset)) Return.True
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
