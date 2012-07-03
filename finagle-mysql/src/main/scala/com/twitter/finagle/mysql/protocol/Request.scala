package com.twitter.finagle.mysql.protocol

import com.twitter.finagle.mysql.util.BufferUtil

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
  def packet = Packet(data.size, seq, data)
  def encoded: Array[Byte] = Array.concat(packet.header, packet.body)
}

class CommandRequest(val cmd: Byte, _data: Array[Byte] = Array[Byte](), seq:Byte = 0) 
  extends Request(seq) {
    override val data: Array[Byte] = Array.concat(Array(cmd), _data)
}

case class Use(dbName: String)
  extends CommandRequest(Command.COM_INIT_DB, dbName.getBytes)

case class CreateDb(dbName: String) 
  extends CommandRequest(Command.COM_CREATE_DB, dbName.getBytes) 

case class DropDb(dbName: String) 
  extends CommandRequest(Command.COM_DROP_DB, dbName.getBytes)

case class Query(sqlStatement: String) 
  extends CommandRequest(Command.COM_QUERY, sqlStatement.getBytes)

case class PrepareStatement(sqlStatement: String)
  extends CommandRequest(Command.COM_STMT_PREPARE, sqlStatement.getBytes)

case class ExecuteStatement(statementId: Int, flags: Byte, iterationCount: Int) extends Request {
  override val data: Array[Byte] = {
    val bw = new BufferWriter(new Array[Byte](9))
    bw.writeInt(statementId)
    bw.writeByte(flags)
    bw.writeInt(iterationCount)
    Array.concat(Array(Command.COM_STMT_EXECUTE), bw.buffer)
  }
}

case class CloseStatement(statementId: Int) extends Request {
  override val data: Array[Byte] = {
    val bw = new BufferWriter(new Array[Byte](4))
    bw.writeInt(statementId)
    bw.buffer
  }
}

