package com.twitter.finagle.mysql.protocol

import com.twitter.finagle.mysql.util.BufferUtil

object Command {
  val COM_SLEEP               = 0x00.toByte
  val COM_QUIT                = 0x01.toByte
  val COM_INIT_DB             = 0x02.toByte
  val COM_QUERY               = 0x03.toByte
  val COM_FIELD_LIST          = 0x04.toByte
  val COM_CREATE_DB           = 0x05.toByte
  val COM_DROP_DB             = 0x06.toByte
  val COM_REFRESH             = 0x07.toByte
  val COM_SHUTDOWN            = 0x08.toByte
  val COM_STATISTICS          = 0x09.toByte
  val COM_PROCESS_INFO        = 0x0A.toByte
  val COM_CONNECT             = 0x0B.toByte
  val COM_PROCESS_KILL        = 0x0C.toByte
  val COM_DEBUG               = 0x0D.toByte
  val COM_PING                = 0x0E.toByte
  val COM_TIME                = 0x0F.toByte
  val COM_DELAYED_INSERT      = 0x10.toByte
  val COM_CHANGE_USER         = 0x11.toByte
  val COM_BINLOG_DUMP         = 0x12.toByte
  val COM_TABLE_DUMP          = 0x13.toByte
  val COM_CONNECT_OUT         = 0x14.toByte
  val COM_REGISTER_SLAVE      = 0x15.toByte
  val COM_STMT_PREPARE        = 0x16.toByte
  val COM_STMT_EXECUTE        = 0x17.toByte
  val COM_STMT_SEND_LONG_DATA = 0x18.toByte
  val COM_STMT_CLOSE          = 0x19.toByte
  val COM_STMT_RESET          = 0x1A.toByte
  val COM_SET_OPTION          = 0x1B.toByte
  val COM_STMT_FETCH          = 0x1C.toByte
  val COM_NOOP_GREET          = 0xFF.toByte //used internally, never sent to the server.
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


