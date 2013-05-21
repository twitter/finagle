package com.twitter.finagle.exp.mysql

import com.twitter.util.Promise
import com.twitter.finagle.exp.mysql.protocol.{BufferReader, Packet}

case class PreparedStatement(statementId: Int, numberOfParams: Int) extends Result {
  val statement: Promise[String] = new Promise[String]()
  private[this] var params: Array[Any] = new Array[Any](numberOfParams)
  private[this] var hasNewParams: Boolean = false

  def parameters: Array[Any] = params
  def hasNewParameters: Boolean = hasNewParams

  def bindParameters() { hasNewParams = false }

  def parameters_=(arr: Array[Any]) = {
    require(arr.size == numberOfParams, "Invalid number of parameters.")
    hasNewParams = true
    params = arr
  }

  def updateParameter(index: Int, value: Any): Unit = {
    hasNewParams = true
    params(index) = value
  }
}

object PreparedStatement {
  /**
   * A prepared statement is made up of the following packets:
   * - A PrepareOK packet with meta data about the prepared statement.
   * - A set of field packets with parameter data if number of parameters > 0
   * - A set of field packets with column data if number of columns > 0
   *
   * The pipeline does not guarantee that the two sets are in order, so
   * we need to verify which set contains what data based on the PrepareOK packet.
   */
  def decode(header: Packet, seqOne: Seq[Packet], seqTwo: Seq[Packet]): PreparedStatement = {
    val ok = PreparedOK.decode(header)
    // The current PreparedStatement implementation does not make use of this
    // data. However, it can be used to perform parameter type verification before
    // sending an ExecuteRequest to the server.
    val (paramPackets, columnPackets) = (ok.numOfParams, ok.numOfColumns) match {
      case (0, 0)          => (Nil, Nil)
      case (0, n) if n > 0 => (Nil, seqOne)
      case (n, 0) if n > 0 => (seqOne, Nil)
      case (_, _)          => (seqOne, seqTwo)
    }

    PreparedStatement(ok.statementId, ok.numOfParams)
  }
}

/**
 * Prepared statement header returned from the server
 * in response to a prepared statement initialization request
 * COM_STMT_PREPARE.
 */
case class PreparedOK(statementId: Int,
                     numOfColumns: Int,
                     numOfParams: Int,
                     warningCount: Int)

object PreparedOK {
  def decode(packet: Packet) = {
    val br = BufferReader(packet.body, 1)
    val stmtId = br.readInt()
    val col = br.readUnsignedShort()
    val params = br.readUnsignedShort()
    br.skip(1)
    val warningCount = br.readUnsignedShort()
    PreparedOK(stmtId, col, params, warningCount)
  }
}