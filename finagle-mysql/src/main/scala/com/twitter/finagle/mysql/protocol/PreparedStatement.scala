package com.twitter.finagle.mysql.protocol

/**
 * Returned from the server in response to a prepared statement
 * initialization request.
 */
case class PrepareOK(statementId: Int, 
                     numColumns: Int, 
                     numParams: Int, 
                     warningCount: Int) extends Result

object PrepareOK {
  def decode(packet: Packet) = {
    val br = new BufferReader(packet.body, 1)
    val stmtId = br.readInt
    val col = br.readUnsignedShort
    val params = br.readUnsignedShort
    br.skip(1)
    val warningCount = br.readUnsignedShort
    PrepareOK(stmtId, col, params, warningCount)
  }
}

case class PreparedStatement(metaData: PrepareOK,
														 	params: List[Field], 
														 	columns: List[Field]) extends Result

object PreparedStatement {
  def decode(header: Packet, paramPackets: List[Packet], colPackets: List[Packet]) = {
    PreparedStatement(
    	PrepareOK.decode(header),
      paramPackets map { Field.decode(_) },
      colPackets map { Field.decode(_) }
    )
  }
}