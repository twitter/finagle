package com.twitter.finagle.mysql.protocol

import com.twitter.logging.Logger
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers._

object Command {
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

abstract class Request(seq: Short) {
  /**
   * Request data translates to the body of the MySQL
   * Packet sent to the server. This field becomes
   * part of a compisition of ChannelBuffers. To ensure
   * that it has the correct byte order use Buffer.toChannelBuffer(...)
   * to create the ChannelBuffer.
   */
  val data: ChannelBuffer

  def toChannelBuffer: ChannelBuffer =
    Packet.toChannelBuffer(data.capacity, seq, data)
}

abstract class CommandRequest(val cmd: Byte) extends Request(0)

class SimpleCommandRequest(command: Byte, buffer: Array[Byte])
  extends CommandRequest(command) {
    override val data = Buffer.toChannelBuffer(Array(cmd), buffer)
}

/**
 * NOOP Request used internally by this client.
 */
case object ClientInternalGreet extends Request(0) {
  override val data = EMPTY_BUFFER
  override def toChannelBuffer = EMPTY_BUFFER
}

case object PingRequest
  extends SimpleCommandRequest(Command.COM_PING, Buffer.EMPTY_BYTE_ARRAY)

case class UseRequest(dbName: String)
  extends SimpleCommandRequest(Command.COM_INIT_DB, dbName.getBytes)

case class QueryRequest(sqlStatement: String)
  extends SimpleCommandRequest(Command.COM_QUERY, sqlStatement.getBytes)

case class PrepareRequest(sqlStatement: String)
  extends SimpleCommandRequest(Command.COM_STMT_PREPARE, sqlStatement.getBytes)

/**
 * An Execute Request.
 * Uses the binary protocol to build an execute request for
 * a prepared statement.
 */
case class ExecuteRequest(ps: PreparedStatement, flags: Byte = 0, iterationCount: Int = 1)
  extends CommandRequest(Command.COM_STMT_EXECUTE) {
    private[this] val log = Logger("finagle-mysql")

    private[this] def isNull(param: Any): Boolean = param match {
      case null => true
      case _ => false
    }

    private[this] def makeNullBitmap(parameters: List[Any], bit: Int = 0, result: BigInt = BigInt(0)): Array[Byte] =
      parameters match {
        case Nil => result.toByteArray.reverse // As little-endian byte array
        case param :: rest =>
          val bits = if (isNull(param)) result.setBit(bit) else result
          makeNullBitmap(rest, bit+1, bits)
      }

    private[this] def writeTypeCode(param: Any, writer: BufferWriter): Unit = {
      val typeCode = Type.getCode(param)

      if (typeCode != -1)
        writer.writeShort(typeCode)
      else {
        // Unsupported type. Write the error to log, and write the type as null.
        // This allows us to safely skip writing the parameter without corrupting the buffer.
        log.error("ExecuteRequest: Unknown parameter %s will be treated as SQL NULL.".format(param.getClass.getName))
        writer.writeShort(Type.NULL)
      }
    }

    /**
     * Returns sizeof all the parameters in the List.
     */
    private[this] def sizeOfParameters(parameters: List[Any], size: Int = 0): Int = parameters match {
      case Nil => size
      case p :: rest =>
        val typeSize = Type.sizeOf(p)
        // We can safely convert unknown sizes to 0 because
        // any unknown type is being sent as NULL.
        val sizeOfParam = if (typeSize == -1) 0 else typeSize
        sizeOfParameters(rest, size + sizeOfParam)
    }

    /**
     * Writes the parameter into its MySQL binary representation.
     */
    private[this] def writeParam(param: Any, writer: BufferWriter) = param match {
      case s: String      => writer.writeLengthCodedString(s)
      case b: Boolean     => writer.writeBoolean(b)
      case b: Byte        => writer.writeByte(b)
      case s: Short       => writer.writeShort(s)
      case i: Int         => writer.writeInt(i)
      case l: Long        => writer.writeLong(l)
      case f: Float       => writer.writeFloat(f)
      case d: Double      => writer.writeDouble(d)
      case b: Array[Byte] => writer.writeLengthCodedBytes(b)
      // Dates
      case t: java.sql.Timestamp    => TimestampValue.write(t, writer)
      case d: java.sql.Date         => DateValue.write(d, writer)
      case d: java.util.Date        => TimestampValue.write(new java.sql.Timestamp(d.getTime), writer)
      case _  => writer // skip null and uknown values
    }

    override val data = {
      val bw = BufferWriter(new Array[Byte](10))
      bw.writeByte(cmd)
      bw.writeInt(ps.statementId)
      bw.writeByte(flags)
      bw.writeInt(iterationCount)

      val paramsList = ps.parameters.toList
      val nullBytes = makeNullBitmap(paramsList)
      val newParamsBound: Byte = if (ps.hasNewParameters) 1 else 0

      val initialBuffer = Buffer.toChannelBuffer(bw.array, nullBytes, Array(newParamsBound))

      // convert parameters to binary representation.
      val sizeOfParams = sizeOfParameters(paramsList)
      val values = BufferWriter(new Array[Byte](sizeOfParams))
      paramsList foreach { writeParam(_, values) }

      // parameters are tagged on to the end of the buffer
      // after types or initialBuffer depending if the prepared statement
      // has new parameters.
      if (ps.hasNewParameters) {
        // only add type data if the prepared statement has new parameters.
        val types = BufferWriter(new Array[Byte](ps.numberOfParams * 2))
        paramsList foreach { writeTypeCode(_, types) }
        wrappedBuffer(initialBuffer, types.toChannelBuffer, values.toChannelBuffer)
      } else
          wrappedBuffer(initialBuffer, values.toChannelBuffer)
    }
}

case class CloseRequest(ps: PreparedStatement) extends CommandRequest(Command.COM_STMT_CLOSE) {
  override val data = {
    val bw = BufferWriter(new Array[Byte](5))
    bw.writeByte(cmd).writeInt(ps.statementId)
    bw.toChannelBuffer
  }
}