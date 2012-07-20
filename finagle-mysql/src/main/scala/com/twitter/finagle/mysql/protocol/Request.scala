package com.twitter.finagle.mysql.protocol

import scala.math.BigInt
import java.sql.Timestamp
import java.util.Date

object Command {
  val COM_SLEEP               = 0x00.toByte /* internal thread state */
  val COM_QUIT                = 0x01.toByte /* mysql_close */
  val COM_INIT_DB             = 0x02.toByte /* mysql_select_db */
  val COM_QUERY               = 0x03.toByte /* mysql_real_query */
  val COM_FIELD_LIST          = 0x04.toByte /* mysql_list_fields */
  val COM_CREATE_DB           = 0x05.toByte /* mysql_create_db (deperacted) */
  val COM_DROP_DB             = 0x06.toByte /* mysql_drop_db (deprecated) */
  val COM_REFRESH             = 0x07.toByte /* mysql_refresh */
  val COM_SHUTDOWN            = 0x08.toByte /* mysql_shutdown */
  val COM_STATISTICS          = 0x09.toByte /* mysql_stat */
  val COM_PROCESS_INFO        = 0x0A.toByte /* mysql_list_processes */
  val COM_CONNECT             = 0x0B.toByte /* internal thread state */
  val COM_PROCESS_KILL        = 0x0C.toByte /* mysql_kill */
  val COM_DEBUG               = 0x0D.toByte /* mysql_dump_debug_info */
  val COM_PING                = 0x0E.toByte /* mysql_ping */
  val COM_TIME                = 0x0F.toByte /* internal thread state */
  val COM_DELAYED_INSERT      = 0x10.toByte /* internal thread state */
  val COM_CHANGE_USER         = 0x11.toByte /* mysql_change_user */
  val COM_BINLOG_DUMP         = 0x12.toByte /* sent by slave IO thread to req a binlog */
  val COM_TABLE_DUMP          = 0x13.toByte /* deprecated */
  val COM_CONNECT_OUT         = 0x14.toByte /* internal thread state */
  val COM_REGISTER_SLAVE      = 0x15.toByte /* sent by the slave to register with the master (optional) */
  val COM_STMT_PREPARE        = 0x16.toByte /* mysql_stmt_prepare */
  val COM_STMT_EXECUTE        = 0x17.toByte /* mysql_stmt_execute */
  val COM_STMT_SEND_LONG_DATA = 0x18.toByte /* mysql_stmt_send_long_data */
  val COM_STMT_CLOSE          = 0x19.toByte /* mysql_stmt_close */
  val COM_STMT_RESET          = 0x1A.toByte /* mysql_stmt_reset */
  val COM_SET_OPTION          = 0x1B.toByte /* mysql_set_server_option */
  val COM_STMT_FETCH          = 0x1C.toByte /* mysql_stmt_fetch */
  val COM_NOOP_GREET          = 0xFF.toByte /* used internally by this client. */
}

abstract class Request(seq: Byte = 0) {
  val data: Array[Byte]
  def toPacket = Packet(data.size, seq, data)
  def toByteArray: Array[Byte] = {
    val p = this.toPacket
    Array.concat(p.header, p.body)
  }
}

class SimpleRequest(val cmd: Byte, _data: Array[Byte] = Array[Byte](), seq:Byte = 0) 
  extends Request(seq) {
    override val data: Array[Byte] = Array.concat(Array(cmd), _data)
}

case class UseRequest(dbName: String)
  extends SimpleRequest(Command.COM_INIT_DB, dbName.getBytes)

case class CreateRequest(dbName: String) 
  extends SimpleRequest(Command.COM_CREATE_DB, dbName.getBytes) 

case class DropRequest(dbName: String) 
  extends SimpleRequest(Command.COM_DROP_DB, dbName.getBytes)

case class QueryRequest(sqlStatement: String) 
  extends SimpleRequest(Command.COM_QUERY, sqlStatement.getBytes)

case class PrepareRequest(sqlStatement: String)
  extends SimpleRequest(Command.COM_STMT_PREPARE, sqlStatement.getBytes)

/**
 * A COM_EXECUTE Request. 
 * Uses the binary protocol to build execute requests.
 */ 
case class ExecuteRequest(ps: PreparedStatement, flags: Byte = 0, iterationCount: Int = 1) extends Request {

  private def isNull(param: Any): Boolean = param match {
    case null => true
    case NullValue(_) => true
    case _ => false
  }

  private def makeNullBitmap(parameters: List[Any], bit: Int, result: BigInt): Array[Byte] = parameters match {
    case Nil => result.toByteArray.reverse //as little-endian byte array
    case param :: rest => 
      val bits = if(isNull(param)) result.setBit(bit) else result
      makeNullBitmap(rest, bit+1, bits)
  }

  private def setTypeCode(param: Any, writer: BufferWriter): Unit = {
    def setType(code: Int) = writer.writeUnsignedShort(code)
    param match {
      case s: String          => setType(Types.VARCHAR)
      case b: Boolean         => setType(Types.TINY)
      case t: Timestamp       => setType(Types.TIMESTAMP)
      case d: Date            => setType(Types.DATE)
      case b: Byte            => setType(Types.TINY)
      case s: Short           => setType(Types.SHORT)
      case i: Int             => setType(Types.LONG)
      case l: Long            => setType(Types.LONGLONG)
      case f: Float           => setType(Types.FLOAT)
      case d: Double          => setType(Types.DOUBLE)
      case NullValue(code)    => setType(code)
      case null               => setType(Types.NULL)
      case _ => throw new IllegalArgumentException("Unhandled query parameter type for " +
            param + " type " + param.asInstanceOf[Object].getClass.getName)
    }
  }

  private def convertToBinary(parameters: List[Any]): Array[Byte] = {
    def mkBuffer(len: Int) = new BufferWriter(new Array[Byte](len))

    def convert(params: List[Any], result: Array[Byte]): Array[Byte] = params match {
      case Nil => result
      case param :: rest => 
        val writer = param match {
          case s: String    => mkBuffer(s.length+1).writeLengthCodedString(s)
          case b: Boolean   => mkBuffer(1).writeBoolean(b)
          //case t: Timestamp => buffer.writeTimestamp(t)
          //case d: Date      => buffer.writeDate(d)
          case b: Byte      => mkBuffer(1).writeByte(b)
          case s: Short     => mkBuffer(2).writeShort(s)
          case i: Int       => mkBuffer(4).writeInt(i)
          case l: Long      => mkBuffer(8).writeLong(l)
          case f: Float     => mkBuffer(4).writeFloat(f)
          case d: Double    => mkBuffer(8).writeDouble(d)
          case NullValue(_) => mkBuffer(0)
          case null         => mkBuffer(0)
          case _ => throw new IllegalArgumentException("Unhandled query parameter type for " +
                  param + " type " + param.asInstanceOf[Object].getClass.getName)
        }

        convert(rest, Array.concat(result, writer.buffer))
    }

    convert(parameters, Array[Byte]())
  }

  override val data: Array[Byte] = {
    val cmdByte = Array(Command.COM_STMT_EXECUTE)
    val bw = new BufferWriter(new Array[Byte](9))
    bw.writeInt(ps.statementId)
    bw.writeByte(flags)
    bw.writeInt(iterationCount)
    
    val paramsList = ps.parameters.toList
    val nullBytes = makeNullBitmap(paramsList, 0, BigInt(0))
    val newParamsBound: Byte = if(ps.hasNewParameters) 1 else 0

    val result = Array.concat(cmdByte, bw.buffer, nullBytes, Array(newParamsBound))

    if(ps.hasNewParameters) {
      val types = new BufferWriter(new Array[Byte](ps.numberOfParams * 2))
      paramsList.map { setTypeCode(_, types) }
      println("types: " + types.buffer.mkString(", "))

      val values = convertToBinary(paramsList)
      println("values: " + values.mkString(", "))

      Array.concat(result, types.buffer, values)
    }
    else 
      result
  }
}

case class CloseRequest(ps: PreparedStatement) extends Request {
  override val data: Array[Byte] = {
    val bw = new BufferWriter(new Array[Byte](4)).writeInt(ps.statementId)
    bw.writeInt(ps.statementId)
    Array.concat(Array(Command.COM_STMT_CLOSE), bw.buffer)
  }
}