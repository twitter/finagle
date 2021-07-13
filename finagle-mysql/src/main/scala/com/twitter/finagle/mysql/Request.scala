package com.twitter.finagle.mysql

import com.twitter.finagle.mysql.transport.{MysqlBuf, MysqlBufWriter, Packet}
import com.twitter.io.Buf
import java.util.logging.Logger

object Command {
  val COM_POISON_CONN: Byte = (-2).toByte // used internally to close an underlying connection
  val COM_SLEEP: Byte = 0x00.toByte // internal thread state
  val COM_QUIT: Byte = 0x01.toByte // mysql_close
  val COM_INIT_DB: Byte = 0x02.toByte // mysql_select_db
  val COM_QUERY: Byte = 0x03.toByte // mysql_real_query
  val COM_FIELD_LIST: Byte = 0x04.toByte // mysql_list_fields
  val COM_CREATE_DB: Byte = 0x05.toByte // mysql_create_db (deprecated)
  val COM_DROP_DB: Byte = 0x06.toByte // mysql_drop_db (deprecated)
  val COM_REFRESH: Byte = 0x07.toByte // mysql_refresh
  val COM_SHUTDOWN: Byte =
    0x08.toByte // mysql_shutdown (deprecated as of 5.7, removal planned for 8 at some point; use SHUTDOWN in com_query instead).
  val COM_STATISTICS: Byte = 0x09.toByte // mysql_stat
  val COM_PROCESS_INFO: Byte = 0x0a.toByte // mysql_list_processes
  val COM_CONNECT: Byte = 0x0b.toByte // internal thread state
  val COM_PROCESS_KILL: Byte = 0x0c.toByte // mysql_kill
  val COM_DEBUG: Byte = 0x0d.toByte // mysql_dump_debug_info
  val COM_PING: Byte = 0x0e.toByte // mysql_ping
  val COM_TIME: Byte = 0x0f.toByte // internal thread state
  val COM_DELAYED_INSERT: Byte = 0x10.toByte // internal thread state
  val COM_CHANGE_USER: Byte = 0x11.toByte // mysql_change_user
  val COM_BINLOG_DUMP: Byte = 0x12.toByte // sent by replica IO thread to req a binlog
  val COM_TABLE_DUMP: Byte = 0x13.toByte // deprecated
  val COM_CONNECT_OUT: Byte = 0x14.toByte // internal thread state
  val COM_REGISTER_SLAVE: Byte =
    0x15.toByte // sent by the replica to register with the master (optional)
  val COM_STMT_PREPARE: Byte = 0x16.toByte // mysql_stmt_prepare
  val COM_STMT_EXECUTE: Byte = 0x17.toByte // mysql_stmt_execute
  val COM_STMT_SEND_LONG_DATA: Byte = 0x18.toByte // mysql_stmt_send_long_data
  val COM_STMT_CLOSE: Byte = 0x19.toByte // mysql_stmt_close
  val COM_STMT_RESET: Byte = 0x1a.toByte // mysql_stmt_reset
  val COM_SET_OPTION: Byte = 0x1b.toByte // mysql_set_server_option
  val COM_STMT_FETCH: Byte = 0x1c.toByte // mysql_stmt_fetch
}

/**
 * A `ProtocolMessage` is an outgoing message sent by the
 * client as part of the MySQL protocol. It contains a
 * sequence number and is able to be converted to a MySQL
 * packet to be sent across the wire.
 */
private[mysql] trait ProtocolMessage {
  def seq: Short
  def toPacket: Packet
}

/**
 * A `Request` is an outgoing message sent by the client
 * as part of the MySQL protocol. It is packet-based and
 * based around a MySQL command.
 */
sealed trait Request extends ProtocolMessage {
  def cmd: Byte
}

/**
 * Contains the SQL for a [[Request]].
 */
sealed trait WithSql {
  def sql: String
}

private[finagle] object PoisonConnectionRequest extends Request {
  def seq: Short = 0
  def cmd: Byte = Command.COM_POISON_CONN
  def toPacket: Packet = ???
}

/**
 * A command request is a request initiated by the client
 * and has a cmd byte associated with it.
 */
abstract class CommandRequest(val cmd: Byte) extends Request {
  def seq: Short = 0
}

/**
 * Defines a request that encodes the command byte and
 * associated data into a packet.
 */
class SimpleCommandRequest(command: Byte, data: Array[Byte]) extends CommandRequest(command) {
  val buf: Buf = Buf.ByteArray.Owned(Array(command)).concat(Buf.ByteArray.Owned(data))
  val toPacket: Packet = Packet(seq, buf)
}

/**
 * A request to check if the server is alive.
 * [[https://dev.mysql.com/doc/internals/en/com-ping.html]]
 */
case object PingRequest extends SimpleCommandRequest(Command.COM_PING, Array.emptyByteArray)

/**
 * Tells the server that the client wants to close the connection.
 * [[https://dev.mysql.com/doc/internals/en/com-quit.html]]
 */
case object QuitRequest extends SimpleCommandRequest(Command.COM_QUIT, Array.emptyByteArray)

/**
 * A UseRequest is used to change the default schema of the connection.
 * [[https://dev.mysql.com/doc/internals/en/com-init-db.html]]
 */
case class UseRequest(dbName: String)
    extends SimpleCommandRequest(Command.COM_INIT_DB, dbName.getBytes)

/**
 * A QueryRequest is used to send the server a text-based query that
 * is executed immediately.
 * [[https://dev.mysql.com/doc/internals/en/com-query.html]]
 */
case class QueryRequest(sqlStatement: String)
    extends SimpleCommandRequest(Command.COM_QUERY, sqlStatement.getBytes)
    with WithSql {
  def sql: String = sqlStatement
}

/**
 * Allocates a prepared statement on the server from the
 * passed in query string.
 * [[https://dev.mysql.com/doc/internals/en/com-stmt-prepare.html]]
 */
case class PrepareRequest(sqlStatement: String)
    extends SimpleCommandRequest(Command.COM_STMT_PREPARE, sqlStatement.getBytes)
    with WithSql {
  def sql: String = sqlStatement
}

/**
 * Client response sent during connection phase.
 * It is similar to a [[HandshakeResponse]], but stops
 * before `username`. If the server supports the `CLIENT_SSL`
 * capability, and the client wants to use SSL/TLS, this is
 * the packet which is sent as a response by the client to
 * indicate that SSL/TLS should then be negotiated.
 * [[https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::SSLRequest]]
 */
private[mysql] case class SslConnectionRequest(
  clientCapabilities: Capability,
  charset: Short,
  maxPacketSize: Int)
    extends ProtocolMessage {
  require(
    clientCapabilities.has(Capability.SSL),
    "Using SslConnectionRequest requires having the SSL capability")

  def seq: Short = 1

  def toPacket: Packet = {
    val packetBodySize = 32
    val bw = MysqlBuf.writer(new Array[Byte](packetBodySize))
    bw.writeIntLE(clientCapabilities.mask)
    bw.writeIntLE(maxPacketSize)
    bw.writeByte(charset)
    bw.fill(23, 0.toByte) // 23 reserved bytes - zeroed out

    Packet(seq, bw.owned())
  }
}

/**
 * Sent to the server in response to the [[AuthSwitchRequest]] during the
 * connection phase. The AuthSwitchResponse wraps the user's password hashed
 * with either SHA-256 or SHA-1, depending on the authentication method.
 *
 * @param seqNum the sequence number of the packet to keep track of
 *               the packets as we move through the authentication phases.
 *
 * @see [[https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchResponse]]
 */
private[mysql] case class AuthSwitchResponse(
  seqNum: Short,
  password: Option[String],
  salt: Array[Byte],
  charset: Short,
  withSha256: Boolean)
    extends ProtocolMessage {

  val hashPassword: Array[Byte] = password match {
    case Some(p) =>
      if (withSha256) PasswordUtils.encryptPasswordWithSha256(p, salt, charset)
      else PasswordUtils.encryptPasswordWithSha1(p, salt, charset)
    case None => Array.emptyByteArray
  }

  // If the password is a null password, then we only send a null
  // byte as the password in the AuthSwitchResponse
  private[this] val bodySize: Int = password match {
    case Some(_) => 32
    case None => 1
  }

  def seq: Short = seqNum

  def toPacket: Packet = {
    val bw = MysqlBuf.writer(new Array[Byte](bodySize))
    bw.writeBytes(hashPassword)
    bw.fill(bodySize - hashPassword.length, 0.toByte)

    Packet(seq, bw.owned())
  }
}

/**
 * Used during `caching_sha2_password` authentication to send:
 * 1. A request to the server for the RSA public key.
 * 2. Password information back to server.
 * Certain bytes are used to communicate what phase of authentication
 * we are in, this is represented by [[AuthMoreDataType]].
 *
 * AuthMoreData packets sent from the client are not wrapped with a
 * 0x01 status flag like the ones sent from the server.
 *
 * @param seqNum the sequence number of the packet to keep track of
 *               the packets as we move through the authentication phases.
 *
 * @see [[https://dev.mysql.com/doc/dev/mysql-server/8.0.22/page_protocol_connection_phase_packets_protocol_auth_more_data.html]]
 */
private[mysql] sealed abstract class AuthMoreDataToServer(
  seqNum: Short,
  authMoreDataType: AuthMoreDataType)
    extends ProtocolMessage {
  def seq: Short = seqNum

  def toPacket: Packet
}

/**
 * Used to communicate successful fast authentication, or that full
 * authentication must be performed.
 */
private[mysql] case class PlainAuthMoreDataToServer(
  seqNum: Short,
  authMoreDataType: AuthMoreDataType)
    extends AuthMoreDataToServer(seqNum = seqNum, authMoreDataType = authMoreDataType) {

  def toPacket: Packet = {
    val bw = MysqlBuf.writer(new Array[Byte](2))
    bw.writeByte(authMoreDataType.moreDataByte)

    Packet(seq, bw.owned())
  }
}

/**
 * Used when the client sends password information to the server.
 */
private[mysql] case class PasswordAuthMoreDataToServer(
  seqNum: Short,
  authMoreDataType: AuthMoreDataType,
  authData: Array[Byte])
    extends AuthMoreDataToServer(seqNum = seqNum, authMoreDataType = authMoreDataType) {

  def toPacket: Packet = {
    // AuthMoreData packet sent from clients are unwrapped (do not have leading 0x01 status tag)
    // See: https://dev.mysql.com/doc/dev/mysql-server/8.0.22/page_protocol_connection_phase_packets_protocol_auth_more_data.html
    val packetBodySize = authData.length
    val bw = MysqlBuf.writer(new Array[Byte](packetBodySize))
    bw.writeBytes(authData)

    Packet(seq, bw.owned())
  }
}

/**
 * Abstract client response sent during connection phase.
 * Responsible for encoding credentials used to
 * authenticate a session.
 */
private[mysql] sealed abstract class HandshakeResponse(
  username: Option[String],
  password: Option[String],
  database: Option[String],
  clientCapabilities: Capability,
  salt: Array[Byte],
  serverCapabilities: Capability,
  charset: Short,
  maxPacketSize: Int,
  enableCachingSha2PasswordAuth: Boolean)
    extends ProtocolMessage {

  lazy val hashPassword: Array[Byte] = password match {
    case Some(pword) =>
      if (enableCachingSha2PasswordAuth) {
        PasswordUtils.encryptPasswordWithSha256(pword, salt, charset)
      } else {
        PasswordUtils.encryptPasswordWithSha1(pword, salt, charset)
      }
    case None => Array.emptyByteArray
  }

  def toPacket: Packet = {
    val fixedBodySize = 34
    val dbStrSize = database.map { _.length + 1 }.getOrElse(0)
    val packetBodySize =
      username.getOrElse("").length + hashPassword.length + dbStrSize + fixedBodySize
    val bw = MysqlBuf.writer(new Array[Byte](packetBodySize))
    bw.writeIntLE(clientCapabilities.mask)
    bw.writeIntLE(maxPacketSize)
    bw.writeByte(charset)
    bw.fill(23, 0.toByte) // 23 reserved bytes - zeroed out
    bw.writeNullTerminatedString(username.getOrElse(""))
    bw.writeLengthCodedBytes(hashPassword)
    if (clientCapabilities.has(Capability.ConnectWithDB) &&
      serverCapabilities.has(Capability.ConnectWithDB))
      bw.writeNullTerminatedString(database.get)

    Packet(seq, bw.owned())
  }
}

/**
 * Client response sent during connection phase.
 * Responsible for encoding credentials used to
 * authenticate a session. Use of this class indicates
 * that the session should not use SSL/TLS.
 * [[https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse41]]
 */
private[mysql] case class PlainHandshakeResponse(
  username: Option[String],
  password: Option[String],
  database: Option[String],
  clientCap: Capability,
  salt: Array[Byte],
  serverCap: Capability,
  charset: Short,
  maxPacketSize: Int,
  enableCachingSha2PasswordAuth: Boolean)
    extends HandshakeResponse(
      username,
      password,
      database,
      clientCap,
      salt,
      serverCap,
      charset,
      maxPacketSize,
      enableCachingSha2PasswordAuth) {

  def seq: Short = 1
}

/**
 * Client response sent during connection phase.
 * Responsible for encoding credentials used to
 * authenticate a session. Use of this class indicates
 * that the session should use SSL/TLS.
 * [[https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse41]]
 */
private[mysql] case class SecureHandshakeResponse(
  username: Option[String],
  password: Option[String],
  database: Option[String],
  clientCap: Capability,
  salt: Array[Byte],
  serverCap: Capability,
  charset: Short,
  maxPacketSize: Int,
  enableCachingSha2PasswordAuth: Boolean)
    extends HandshakeResponse(
      username,
      password,
      database,
      clientCap,
      salt,
      serverCap,
      charset,
      maxPacketSize,
      enableCachingSha2PasswordAuth) {

  require(
    serverCap.has(Capability.SSL),
    "serverCap must contain Capability.SSL to send a SecureHandshakeResponse")
  require(
    clientCap.has(Capability.SSL),
    "clientCap must contain Capability.SSL to send a SecureHandshakeResponse")

  def seq: Short = 2
}

class FetchRequest(val prepareOK: PrepareOK, val numRows: Int)
    extends CommandRequest(Command.COM_STMT_FETCH) {

  val stmtId: Int = prepareOK.id

  def toPacket: Packet = {
    val bw = MysqlBuf.writer(new Array[Byte](9))
    bw.writeByte(cmd)
    bw.writeIntLE(stmtId)
    bw.writeIntLE(numRows)

    Packet(seq, bw.owned())
  }
}

/**
 * Uses the binary protocol to build an execute request for
 * a prepared statement.
 * [[https://dev.mysql.com/doc/internals/en/com-stmt-execute.html]]
 */
class ExecuteRequest(
  val stmtId: Int,
  val params: IndexedSeq[Parameter],
  val hasNewParams: Boolean,
  val flags: Byte)
    extends CommandRequest(Command.COM_STMT_EXECUTE) {

  private[this] val log = Logger.getLogger("finagle-mysql")

  private[this] def makeNullBitmap(parameters: IndexedSeq[Parameter]): Array[Byte] = {
    val bitmap = new Array[Byte]((parameters.size + 7) / 8)
    val ps = parameters.zipWithIndex
    ps foreach {
      case (Parameter.NullParameter, idx) =>
        val bytePos = idx / 8
        val bitPos = idx % 8
        val byte = bitmap(bytePos)
        bitmap(bytePos) = (byte | (1 << bitPos)).toByte
      case _ =>
        ()
    }
    bitmap
  }

  private[this] def writeTypeCode(param: Parameter, writer: MysqlBufWriter): Unit = {
    val typeCode = param.typeCode
    if (typeCode != -1)
      writer.writeShortLE(typeCode)
    else {
      // Unsupported type. Write the error to log, and write the type as null.
      // This allows us to safely skip writing the parameter without corrupting the buffer.
      log.warning(
        "Unknown parameter %s will be treated as SQL NULL.".format(param.getClass.getName)
      )
      writer.writeShortLE(Type.Null)
    }
  }

  /**
   * Returns sizeof all the parameters according to
   * mysql binary encoding.
   */
  private[this] def sizeOfParameters(parameters: IndexedSeq[Parameter]): Int =
    parameters.foldLeft(0)(_ + _.size)

  /**
   * Writes the parameter into its MySQL binary representation.
   */
  private[this] def writeParam(param: Parameter, writer: MysqlBufWriter): MysqlBufWriter = {
    param.writeTo(writer)
    writer
  }

  def toPacket: Packet = {
    val bw = MysqlBuf.writer(new Array[Byte](10))
    bw.writeByte(cmd)
    bw.writeIntLE(stmtId)
    bw.writeByte(flags)
    bw.writeIntLE(1) // iteration count - always 1

    val newParamsBound: Byte = if (hasNewParams) 1 else 0
    val newParamsBoundBuf = Buf.ByteArray.Owned(Array(newParamsBound))

    // convert parameters to binary representation.
    val sizeOfParams = sizeOfParameters(params)
    val values = MysqlBuf.writer(new Array[Byte](sizeOfParams))
    params foreach { writeParam(_, values) }

    // encode null values in bitmap
    val nullBitmap = Buf.ByteArray.Owned(makeNullBitmap(params))

    // parameters are appended to the end of the packet
    // only if the statement has new parameters.
    val composite = if (hasNewParams) {
      val types = MysqlBuf.writer(new Array[Byte](params.size * 2))
      params foreach { writeTypeCode(_, types) }
      Buf(Seq(bw.owned(), nullBitmap, newParamsBoundBuf, types.owned(), values.owned()))
    } else {
      Buf(Seq(bw.owned(), nullBitmap, newParamsBoundBuf, values.owned()))
    }
    Packet(seq, composite)
  }
}

object ExecuteRequest {
  val FLAG_CURSOR_READ_ONLY: Byte = 0x01.toByte // CURSOR_TYPE_READ_ONLY

  def apply(
    stmtId: Int,
    params: IndexedSeq[Parameter] = IndexedSeq.empty,
    hasNewParams: Boolean = true,
    flags: Byte = 0
  ): ExecuteRequest = {
    val sanitizedParams = params.map {
      case null => Parameter.NullParameter
      case other => other
    }
    new ExecuteRequest(stmtId, sanitizedParams, hasNewParams, flags)
  }

  def unapply(
    executeRequest: ExecuteRequest
  ): Option[(Int, IndexedSeq[Parameter], Boolean, Byte)] = {
    Some(
      (
        executeRequest.stmtId,
        executeRequest.params,
        executeRequest.hasNewParams,
        executeRequest.flags
      )
    )
  }
}

/**
 * A CloseRequest deallocates a prepared statement on the server.
 * No response is sent back to the client.
 * [[https://dev.mysql.com/doc/internals/en/com-stmt-close.html]]
 */
case class CloseRequest(stmtId: Int) extends CommandRequest(Command.COM_STMT_CLOSE) {
  val toPacket: Packet = {
    val bw = MysqlBuf.writer(new Array[Byte](5))
    bw.writeByte(cmd).writeIntLE(stmtId)
    Packet(seq, bw.owned())
  }
}
