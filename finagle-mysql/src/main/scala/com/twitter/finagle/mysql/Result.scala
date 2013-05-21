package com.twitter.finagle.exp.mysql

import com.twitter.finagle.exp.mysql.protocol.{BufferReader, Packet}

trait Result

/**
 * Represents the OK Packet received from the server. It is sent
 * to indicate that a command has completed succesfully. The following
 * commands receive OK packets:
 * - COM_PING
 * - COM_QUERY (INSERT, UPDATE, or ALTER TABLE)
 * - COM_REFRESH
 * - COM_REGISTER_SLAVE
 */
case class OK(affectedRows: Long,
              insertId: Long,
              serverStatus: Int,
              warningCount: Int,
              message: String) extends Result

object OK {
  def decode(packet: Packet) = {
    // start reading after flag byte
    val br = BufferReader(packet.body, 1)
    OK(
      br.readLengthCodedBinary(),
      br.readLengthCodedBinary(),
      br.readUnsignedShort(),
      br.readUnsignedShort(),
      new String(br.takeRest())
    )
  }
}

/**
 * Used internally to synthesize a response from
 * the server when sending a prepared statement
 * CloseRequest
 */
object CloseStatementOK extends OK(0,0,0,0, "Internal Close OK")

/**
 * Represents the Error Packet received from the server
 * and the data sent along with it.
 */
case class Error(code: Short, sqlState: String, message: String) extends Result

object Error {
  def decode(packet: Packet) = {
    // start reading after flag byte
    val br = BufferReader(packet.body, 1)
    val code = br.readShort()
    val state = new String(br.take(6))
    val msg = new String(br.takeRest())
    Error(code, state, msg)
  }
}

/**
 * Represents and EOF result received from the server which
 * contains any warnings and the server status.
 */
case class EOF(warnings: Short, serverStatus: Short) extends Result

object EOF {
  def decode(packet: Packet) = {
    val br = BufferReader(packet.body, 1)
    EOF(br.readShort(), br.readShort())
  }
}