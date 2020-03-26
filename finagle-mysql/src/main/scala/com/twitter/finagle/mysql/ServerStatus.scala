package com.twitter.finagle.mysql

object ServerStatus {
  val InTrans: Int = 0x0001
  val Autocommit: Int = 0x0002
  val MoreResultsExists: Int = 0x0008
  val NoGoodIndexUsed: Int = 0x0010
  val NoIndexUsed: Int = 0x0020
  val CursorExists: Int = 0x0040
  val LastRowSent: Int = 0x0080
  val DbDropped: Int = 0x0100
  val NoBackslashEscapes: Int = 0x0200
  val MetadataChanged: Int = 0x0400
  val QueryWasSlow: Int = 0x0800
  val PsOutParams: Int = 0x1000
  val InTransReadonly: Int = 0x2000
  val SessionStateChanged: Int = 0x4000

  val ServerStatusMap = Map(
    "InTrans" -> InTrans,
    "Autocommit" -> Autocommit,
    "MoreResultsExists" -> MoreResultsExists,
    "NoGoodIndexUsed" -> NoGoodIndexUsed,
    "NoIndexUsed" -> NoIndexUsed,
    "CursorExists" -> CursorExists,
    "LastRowSent" -> LastRowSent,
    "DbDropped" -> DbDropped,
    "NoBackslashEscapes" -> NoBackslashEscapes,
    "MetadataChanged" -> MetadataChanged,
    "QueryWasSlow" -> QueryWasSlow,
    "PsOutParams" -> PsOutParams,
    "InTransReadonly" -> InTransReadonly,
    "SessionStateChanged" -> SessionStateChanged
  )

  def apply(flags: Int*): ServerStatus = {
    val m = flags.foldLeft(0)(_ | _)
    ServerStatus(m)
  }
}

/**
 * Represents the ServerStatus as represented in EOF packets
 *
 * @param mask the raw bit mask in the EOF packet
 */
case class ServerStatus(mask: Int) {
  def has(flag: Int) = (flag & mask) > 0

  override def toString = {
    val cs = (ServerStatus.ServerStatusMap filter { t => has(t._2) }).keys mkString (", ")
    "ServerStatus(" + mask + ": " + cs + ")"
  }
}
