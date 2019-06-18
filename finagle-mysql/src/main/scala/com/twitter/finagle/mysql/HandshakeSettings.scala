package com.twitter.finagle.mysql

import com.twitter.conversions.StorageUnitOps._
import com.twitter.finagle.Stack
import com.twitter.finagle.mysql.MysqlCharset.Utf8_general_ci
import com.twitter.finagle.mysql.param.{Charset, Credentials, Database, FoundRows}
import com.twitter.util.StorageUnit

/**
 * A collection of MySQL specific settings used for
 * establishing a MySQL session.
 *
 * @param username MySQL username used to login.
 *
 * @param password MySQL password used to login.
 *
 * @param database initial database to use for the session.
 *
 * @param clientCapabilities The capabilities that this client has.
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
 */
private[mysql] final case class HandshakeSettings(
  username: Option[String] = None,
  password: Option[String] = None,
  database: Option[String] = None,
  clientCapabilities: Capability = Capability.baseCapabilities,
  charset: Short = Utf8_general_ci,
  enableFoundRows: Boolean = true,
  maxPacketSize: StorageUnit = 1.gigabyte) {

  require(maxPacketSize <= 1.gigabyte, s"Max packet size ($maxPacketSize) cannot exceed 1 gigabyte")

  /**
   * Optionally adds either or both the `ConnectWithDB` and
   * `FoundRows` capabilities to the initial `clientCapabilities`
   * based on the `database` and `enableFoundRows` values
   * passed in to the constructor.
   *
   * @note This method does not include the `SSL` capability
   * by default. For one that does, see `sslCalculatedClientCapabilities`.
   */
  def calculatedClientCapabilities: Capability =
    clientCapabilities
      .set(database.isDefined, Capability.ConnectWithDB)
      .set(enableFoundRows, Capability.FoundRows)

  /**
   * Adds the `SSL` capability to the `calculatedClientCapabilities`.
   */
  def sslCalculatedClientCapabilities: Capability =
    calculatedClientCapabilities + Capability.SSL
}

private[mysql] object HandshakeSettings {

  /**
   * Creates a HandshakeSettings object from a collection of [[com.twitter.finagle.Stack.Params]].
   */
  def apply(prms: Stack.Params): HandshakeSettings = {
    val credentials = prms[Credentials]
    HandshakeSettings(
      username = credentials.username,
      password = credentials.password,
      database = prms[Database].db,
      charset = prms[Charset].charset,
      enableFoundRows = prms[FoundRows].enabled
    )
  }
}
