package com.twitter.finagle.mysql.protocol

import java.sql.{Timestamp, Date => SQLDate}
import java.util.Date
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers._
import scala.math.BigInt

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

abstract class Request(seq: Byte) {
  val data: ChannelBuffer

  def toChannelBuffer: ChannelBuffer = {
    val headerBuffer = PacketHeader(data.capacity, seq).toChannelBuffer
    wrappedBuffer(headerBuffer, data)
  }
}

abstract class CommandRequest(val cmd: Byte) extends Request(0)

class SimpleCommandRequest(command: Byte, buffer: Array[Byte]) 
  extends CommandRequest(command) {
    override val data = wrappedBuffer(Array(cmd), buffer)
}

/** 
 * NOOP Request used internally by this client. 
 */
case object ClientInternalGreet extends Request(0) {
  override val data = EMPTY_BUFFER
  override def toChannelBuffer = EMPTY_BUFFER
}

case class UseRequest(dbName: String)
  extends SimpleCommandRequest(Command.COM_INIT_DB, dbName.getBytes)

case class CreateRequest(dbName: String) 
  extends SimpleCommandRequest(Command.COM_CREATE_DB, dbName.getBytes)

case class DropRequest(dbName: String) 
  extends SimpleCommandRequest(Command.COM_DROP_DB, dbName.getBytes)

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
    def writeType(code: Int) = writer.writeUnsignedShort(code)
    param match {
      case s: String          => writeType(Types.VARCHAR)
      case b: Boolean         => writeType(Types.TINY)
      case t: Timestamp       => writeType(Types.TIMESTAMP)
      case d: SQLDate         => writeType(Types.DATE)
      case d: Date            => writeType(Types.DATE)
      case b: Byte            => writeType(Types.TINY)
      case s: Short           => writeType(Types.SHORT)
      case i: Int             => writeType(Types.LONG)
      case l: Long            => writeType(Types.LONGLONG)
      case f: Float           => writeType(Types.FLOAT)
      case d: Double          => writeType(Types.DOUBLE)
      case null               => writeType(Types.NULL)
      case _ => throw new IllegalArgumentException("Unhandled query parameter type for " +
            param + " type " + param.asInstanceOf[Object].getClass.getName)
    }
  }

  /** 
   * Calculates the size needed to write each parameter
   * in its binary encoding according to the MySQL protocol.
   */
  private[this] def sizeOfParameters(parameters: List[Any], size: Int = 0): Int = parameters match {
    case Nil => size
    case p :: rest =>
      val sizeOfParam = p match {
        case s: String    => 
        // calculate the space needed to store the length
        val sizeOfLen = if (s.size < 251) 1 else if (s.size < 65536) 2 else if (s.size < 16777216) 3 else 8
        sizeOfLen + s.size
        case b: Boolean   => 1
        case b: Byte      => 1
        case s: Short     => 2
        case i: Int       => 4
        case l: Long      => 8
        case f: Float     => 4
        case d: Double    => 8
        case t: Timestamp => 11
        case d: SQLDate   => 11
        case d: Date      => 11
        case null         => 0
        case _            => 0
      }

    sizeOfParameters(rest, size + sizeOfParam)
  }

  private[this] def writeParam(param: Any, writer: BufferWriter) = param match {
    case s: String    => writer.writeLengthCodedString(s)
    case b: Boolean   => writer.writeBoolean(b)
    case t: Timestamp => writer.writeTimestamp(t)
    case d: SQLDate   => writer.writeDate(d)
    case d: Date      => writer.writeDate(d)
    case b: Byte      => writer.writeByte(b)
    case s: Short     => writer.writeShort(s)
    case i: Int       => writer.writeInt(i)
    case l: Long      => writer.writeLong(l)
    case f: Float     => writer.writeFloat(f)
    case d: Double    => writer.writeDouble(d)
    case null         => writer
    case _ => throw new IllegalArgumentException("Unhandled query parameter type for " +
            param + " type " + param.asInstanceOf[Object].getClass.getName)
  }

  override val data = {
    val bw = new BufferWriter(new Array[Byte](10))
    bw.writeByte(cmd)
    bw.writeInt(ps.statementId)
    bw.writeByte(flags)
    bw.writeInt(iterationCount)
    
    val paramsList = ps.parameters.toList
    val nullBytes = makeNullBitmap(paramsList)
    val newParamsBound: Byte = if (ps.hasNewParameters) 1 else 0

    val result = wrappedBuffer(bw.buffer, nullBytes, Array(newParamsBound))

    // Only write the parameter data if the prepared statement
    // has new parameters.
    if (ps.hasNewParameters) {
      val types = new BufferWriter(new Array[Byte](ps.numberOfParams * 2))
      paramsList.map { writeTypeCode(_, types) }

      val sizeOfParams = sizeOfParameters(paramsList)
      val values = new BufferWriter(new Array[Byte](sizeOfParams))
      paramsList.map { writeParam(_, values) }

      wrappedBuffer(result, types.toChannelBuffer, values.toChannelBuffer)
    }
    else 
      result
  }
}

case class CloseRequest(ps: PreparedStatement) extends CommandRequest(Command.COM_STMT_CLOSE) {
  override val data = {
    val bw = new BufferWriter(new Array[Byte](5))
    bw.writeByte(cmd).writeInt(ps.statementId)
    bw.toChannelBuffer
  }
}