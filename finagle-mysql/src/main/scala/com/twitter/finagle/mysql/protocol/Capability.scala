package com.twitter.finagle.mysql.protocol

object Capability {
  val longPassword     = 0x0001
  val foundRows        = 0x0002
  val longFlag         = 0x0004
  val connectWithDB    = 0x0008
  val noSchema         = 0x0010
  val compress         = 0x0020
  val ODBC             = 0x0040
  val localFiles       = 0x0080
  val ignoreSpace      = 0x0100
  val protocol41       = 0x0200
  val interactive      = 0x0400
  val ssl              = 0x0800
  val ignoreSigPipe    = 0x1000
  val transactions     = 0x2000
  val secureConnection = 0x8000
  val multiStatements  = 0x10000
  val multiResults     = 0x20000

  val CapabilityMap = Map(
    "CLIENT_LONG_PASSWORD"     -> Capability.longPassword,
    "CLIENT_FOUND_ROWS"        -> Capability.foundRows,
    "CLIENT_LONG_FLAG"         -> Capability.longFlag,
    "CLIENT_CONNECT_WITH_DB"   -> Capability.connectWithDB,
    "CLIENT_NO_SCHEMA"         -> Capability.noSchema,
    "CLIENT_COMPRESS"          -> Capability.compress,
    "CLIENT_ODBC"              -> Capability.ODBC,
    "CLIENT_LOCAL_FILES"       -> Capability.localFiles,
    "CLIENT_IGNORE_SPACE"      -> Capability.ignoreSpace,
    "CLIENT_PROTOCOL_41"       -> Capability.protocol41,
    "CLIENT_INTERACTIVE"       -> Capability.interactive,
    "CLIENT_SSL"               -> Capability.ssl,
    "CLIENT_IGNORE_SIGPIPE"    -> Capability.ignoreSigPipe,
    "CLIENT_TRANSACTIONS"      -> Capability.transactions,
    "CLIENT_SECURE_CONNECTION" -> Capability.secureConnection,
    "CLIENT_MULTI_STATEMENTS"  -> Capability.multiStatements,
    "CLIENT_MULTI_RESULTS"     -> Capability.multiResults
    )

  def apply(flags: Int*): Capability = {
    val m = flags.foldLeft(0)(_|_)
    Capability(m)
  }
}

case class Capability(mask: Int) {
  def has(flag: Int) = hasAll(flag)
  def hasAll(flags: Int*) = flags map {f: Int => (f & mask) > 0} reduceLeft {_ && _}
  /*override def toString() = {
    val cs = Capability.CapabilityMap filter {t => has(t._2)} map {_._1} mkString(", ")
    "Capability(" + mask + ": " + cs + ")"
  }*/
}