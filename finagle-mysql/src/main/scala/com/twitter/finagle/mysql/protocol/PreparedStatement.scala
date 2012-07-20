package com.twitter.finagle.mysql.protocol

import com.twitter.util.Promise

trait PreparedStatement extends Result {
  val statementId: Int
  val numberOfParams: Int
  val statement: Promise[String] = new Promise[String]()

  protected var params: Array[Any] = new Array[Any](numberOfParams)
  protected var hasNewParams: Boolean = false

  def parameters: Array[Any] = params
  def hasNewParameters: Boolean = hasNewParams
  
  def parameters_=(arr: Array[Any]) = {
    require(arr.size == numberOfParams)
    hasNewParams = true
    params = arr
  }

  def updateParameter(index: Int, value: Any): Unit = {
    hasNewParams = true
    params(index) = value
  }
}

object PreparedStatement {
  def decode(header: Packet, paramData: Seq[Packet], fieldData: Seq[Packet]): PreparedStatement = {
    val ok = PreparedOK.decode(header)
    new PreparedStatement {
      val statementId = ok.statementId
      val numberOfParams = paramData.size
    }
  }
}

/**
 * Prepared statement header returned from the server
 * in response to a prepared statement initialization request 
 * COM_STMT_PREPARE.
 */
case class PreparedOK(statementId: Int, 
                     numColumns: Int, 
                     numParams: Int, 
                     warningCount: Int)

object PreparedOK {
  def decode(packet: Packet) = {
    val br = new BufferReader(packet.body, 1)
    val stmtId = br.readInt
    val col = br.readUnsignedShort
    val params = br.readUnsignedShort
    br.skip(1)
    val warningCount = br.readUnsignedShort
    PreparedOK(stmtId, col, params, warningCount)
  }
}

