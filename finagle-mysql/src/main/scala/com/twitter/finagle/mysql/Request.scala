package com.twitter.finagle.exp.mysql

import com.twitter.finagle.exp.mysql.transport.{Buffer, BufferWriter, Packet}
import java.security.MessageDigest
import java.util.logging.Logger

object Command {
  val COM_NO_OP               = -1.toByte   // used internall by this client
  val COM_SLEEP               = 0x00.toByte // internal thread state
  val COM_QUIT                = 0x01.toByte // mysql_close
  val COM_INIT_DB             = 0x02.toByte // mysql_select_db
  val COM_QUERY               = 0x03.toByte // mysql_real_query
  val COM_FIELD_LIST          = 0x04.toByte // mysql_list_fields
  val COM_CREATE_DB           = 0x05.toByte // mysql_create_db (deperacted)
  val COM_DROP_DB             = 0x06.toByte // mysql_drop_db (deprecated)
  val COM_REFRESH             = 0x07.toByte // mysql_refresh
  val COM_SHUTDOWN            = 0x08.toByte // mysql_shutdown
  val COM_STATISTICS          = 0x09.toByte // mysql_stat
  val COM_PROCESS_INFO        = 0x0A.toByte // mysql_list_processes
  val COM_CONNECT             = 0x0B.toByte // internal thread state
  val COM_PROCESS_KILL        = 0x0C.toByte // mysql_kill
  val COM_DEBUG               = 0x0D.toByte // mysql_dump_debug_info
  val COM_PING                = 0x0E.toByte // mysql_ping
  val COM_TIME                = 0x0F.toByte // internal thread state
  val COM_DELAYED_INSERT      = 0x10.toByte // internal thread state
  val COM_CHANGE_USER         = 0x11.toByte // mysql_change_user
  val COM_BINLOG_DUMP         = 0x12.toByte // sent by slave IO thread to req a binlog
  val COM_TABLE_DUMP          = 0x13.toByte // deprecated
  val COM_CONNECT_OUT         = 0x14.toByte // internal thread state
  val COM_REGISTER_SLAVE      = 0x15.toByte // sent by the slave to register with the master (optional)
  val COM_STMT_PREPARE        = 0x16.toByte // mysql_stmt_prepare
  val COM_STMT_EXECUTE        = 0x17.toByte // mysql_stmt_execute
  val COM_STMT_SEND_LONG_DATA = 0x18.toByte // mysql_stmt_send_long_data
  val COM_STMT_CLOSE          = 0x19.toByte // mysql_stmt_close
  val COM_STMT_RESET          = 0x1A.toByte // mysql_stmt_reset
  val COM_SET_OPTION          = 0x1B.toByte // mysql_set_server_option
  val COM_STMT_FETCH          = 0x1C.toByte // mysql_stmt_fetch
}

sealed trait Request {
  val seq: Short
  val cmd: Byte = Command.COM_NO_OP
  def toPacket: Packet
}

/**
 * A command request is a request initiated by the client
 * and has a cmd byte associated with it.
 */
abstract class CommandRequest(override val cmd: Byte) extends Request {
  val seq: Short = 0
}

/**
 * Defines a request that encodes the command byte and
 * associated data into a packet.
 */
class SimpleCommandRequest(command: Byte, data: Array[Byte])
  extends CommandRequest(command) {
    val buf = Buffer(Buffer(Array(command)), Buffer(data))
    val toPacket = Packet(seq, buf)
}

/**
 * A request to check if the server is alive.
 * [[http://dev.mysql.com/doc/internals/en/com-ping.html]]
 */
case object PingRequest
  extends SimpleCommandRequest(Command.COM_PING, Buffer.EmptyByteArray)

/**
 * A UseRequest is used to change the default schema of the connection.
 * [[http://dev.mysql.com/doc/internals/en/com-init-db.html]]
 */
case class UseRequest(dbName: String)
  extends SimpleCommandRequest(Command.COM_INIT_DB, dbName.getBytes)

/**
 * A QueryRequest is used to send the server a text-based query that
 * is executed immediately.
 * [[http://dev.mysql.com/doc/internals/en/com-query.html]]
 */
case class QueryRequest(sqlStatement: String)
  extends SimpleCommandRequest(Command.COM_QUERY, sqlStatement.getBytes)

/**
 * Allocates a prepared statement on the server from the
 * passed in query string.
 * [[http://dev.mysql.com/doc/internals/en/com-stmt-prepare.html]]
 */
case class PrepareRequest(sqlStatement: String)
  extends SimpleCommandRequest(Command.COM_STMT_PREPARE, sqlStatement.getBytes)

/**
 * Client response sent during connection phase.
 * Responsible for encoding credentials used to
 * authenticate a session.
 * [[http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse41]]
 */
case class HandshakeResponse(
  username: Option[String],
  password: Option[String],
  database: Option[String],
  clientCap: Capability,
  salt: Array[Byte],
  serverCap: Capability,
  charset: Short,
  maxPacketSize: Int
) extends Request {
  import Capability._
  override val seq: Short = 1

  lazy val hashPassword = password match {
    case Some(p) => encryptPassword(p, salt)
    case None => Array[Byte]()
  }

  def toPacket = {
    val fixedBodySize = 34
    val dbStrSize = database.map { _.length + 1 }.getOrElse(0)
    val packetBodySize =
      username.getOrElse("").length + hashPassword.length + dbStrSize + fixedBodySize
    val bw = BufferWriter(new Array[Byte](packetBodySize))
    bw.writeInt(clientCap.mask)
    bw.writeInt(maxPacketSize)
    bw.writeByte(charset)
    bw.fill(23, 0.toByte) // 23 reserved bytes - zeroed out
    bw.writeNullTerminatedString(username.getOrElse(""), Charset(charset))
    bw.writeLengthCodedBytes(hashPassword)
    if (clientCap.has(ConnectWithDB) && serverCap.has(ConnectWithDB))
      bw.writeNullTerminatedString(database.get, Charset(charset))

    Packet(seq, bw)
  }

  private[this] def encryptPassword(password: String, salt: Array[Byte]) = {
    val md = MessageDigest.getInstance("SHA-1")
    val hash1 = md.digest(password.getBytes(Charset(charset).displayName))
    md.reset()
    val hash2 = md.digest(hash1)
    md.reset()
    md.update(salt)
    md.update(hash2)

    val digest = md.digest()
    (0 until digest.length) foreach { i =>
      digest(i) = (digest(i) ^ hash1(i)).toByte
    }
    digest
  }
}

/**
 * Uses the binary protocol to build an execute request for
 * a prepared statement.
 * [[http://dev.mysql.com/doc/internals/en/com-stmt-execute.html]]
 */
class ExecuteRequest(
  val stmtId: Int,
  val params: IndexedSeq[Parameter],
  val hasNewParams: Boolean,
  val flags: Byte
) extends CommandRequest(Command.COM_STMT_EXECUTE) {
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

    private[this] def writeTypeCode(param: Parameter, writer: BufferWriter): Unit = {
      val typeCode = param.typeCode
      if (typeCode != -1)
        writer.writeShort(typeCode)
      else {
        // Unsupported type. Write the error to log, and write the type as null.
        // This allows us to safely skip writing the parameter without corrupting the buffer.
        log.warning("Unknown parameter %s will be treated as SQL NULL.".format(param.getClass.getName))
        writer.writeShort(Type.Null)
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
    private[this] def writeParam(param: Parameter, writer: BufferWriter): BufferWriter = {
      param.writeTo(writer)
      writer
    }

    def toPacket = {
      val bw = BufferWriter(new Array[Byte](10))
      bw.writeByte(cmd)
      bw.writeInt(stmtId)
      bw.writeByte(flags)
      bw.writeInt(1) // iteration count - always 1

      val newParamsBound: Byte = if (hasNewParams) 1 else 0

      // convert parameters to binary representation.
      val sizeOfParams = sizeOfParameters(params)
      val values = BufferWriter(new Array[Byte](sizeOfParams))
      params foreach { writeParam(_, values) }

      // encode null values in bitmap
      val nullBitmap = makeNullBitmap(params)

      // parameters are appended to the end of the packet
      // only if the statement has new parameters.
      val composite = if (hasNewParams) {
        val types = BufferWriter(new Array[Byte](params.size * 2))
        params foreach { writeTypeCode(_, types) }
        Buffer(bw, Buffer(nullBitmap), Buffer(Array(newParamsBound)), types, values)
      } else {
        Buffer(bw, Buffer(nullBitmap), Buffer(Array(newParamsBound)), values)
      }
      Packet(seq, composite)
    }
}

object ExecuteRequest {
  def apply(
    stmtId: Int,
    params: IndexedSeq[Parameter] = IndexedSeq.empty,
    hasNewParams: Boolean = true,
    flags: Byte = 0
  ) = {
    val sanitizedParams = params.map {
      case null  => Parameter.NullParameter
      case other => other
    }
    new ExecuteRequest(stmtId, sanitizedParams, hasNewParams, flags)
  }

  def unapply(executeRequest: ExecuteRequest): Option[(Int, IndexedSeq[Parameter], Boolean, Byte)] = {
    Some((executeRequest.stmtId, executeRequest.params, executeRequest.hasNewParams, executeRequest.flags))
  }
}

/**
 * A CloseRequest deallocates a prepared statement on the server.
 * No response is sent back to the client.
 * [[http://dev.mysql.com/doc/internals/en/com-stmt-close.html]]
 */
case class CloseRequest(stmtId: Int) extends CommandRequest(Command.COM_STMT_CLOSE) {
  override val toPacket = {
    val bw = BufferWriter(new Array[Byte](5))
    bw.writeByte(cmd).writeInt(stmtId)
    Packet(seq, bw)
  }
}
