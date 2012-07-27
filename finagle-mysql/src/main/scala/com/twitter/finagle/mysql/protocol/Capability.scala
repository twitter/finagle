package com.twitter.finagle.mysql.protocol

object Capability {
  val LongPassword     = 0x0001 // new more secure passwords
  val FoundRows        = 0x0002 // Found instead of affected rows
  val LongFlag         = 0x0004 // Get all column flags
  val ConnectWithDB    = 0x0008 // One can specify db on connect
  val NoSchema         = 0x0010 // Don't allow database.table.column
  val Compress         = 0x0020 // Can use compression protocol
  val ODBC             = 0x0040 // Odbc client
  val LocalFiles       = 0x0080 // Can use LOAD DATA LOCAL
  val IgnoreSpace      = 0x0100 // Ignore spaces before '('
  val Protocol41       = 0x0200 // New 4.1 protocol
  val Interactive      = 0x0400 // This is an interactive client
  val SSL              = 0x0800 // Switch to SSL after handshake
  val IgnoreSigPipe    = 0x1000 // IGNORE sigpipes
  val Transactions     = 0x2000 // Client knows about transactions
  val SecureConnection = 0x8000 // New 4.1 authentication
  val MultiStatements  = 0x10000 // Enable/disable multi-stmt support
  val MultiResults     = 0x20000 // Enable/disable multi-results */

  val CapabilityMap = Map(
    "CLIENT_LONG_PASSWORD"     -> Capability.LongPassword,
    "CLIENT_FOUND_ROWS"        -> Capability.FoundRows,
    "CLIENT_LONG_FLAG"         -> Capability.LongFlag,
    "CLIENT_CONNECT_WITH_DB"   -> Capability.ConnectWithDB,
    "CLIENT_NO_SCHEMA"         -> Capability.NoSchema,
    "CLIENT_COMPRESS"          -> Capability.Compress,
    "CLIENT_ODBC"              -> Capability.ODBC,
    "CLIENT_LOCAL_FILES"       -> Capability.LocalFiles,
    "CLIENT_IGNORE_SPACE"      -> Capability.IgnoreSpace,
    "CLIENT_PROTOCOL_41"       -> Capability.Protocol41,
    "CLIENT_INTERACTIVE"       -> Capability.Interactive,
    "CLIENT_SSL"               -> Capability.SSL,
    "CLIENT_IGNORE_SIGPIPE"    -> Capability.IgnoreSigPipe,
    "CLIENT_TRANSACTIONS"      -> Capability.Transactions,
    "CLIENT_SECURE_CONNECTION" -> Capability.SecureConnection,
    "CLIENT_MULTI_STATEMENTS"  -> Capability.MultiStatements,
    "CLIENT_MULTI_RESULTS"     -> Capability.MultiResults
    )

  def apply(flags: Int*): Capability = {
    val m = flags.foldLeft(0)(_|_)
    Capability(m)
  }
}

case class Capability(mask: Int) {
  def +(flag: Int) = Capability(mask, flag)
  def -(flag: Int) = Capability(mask & ~flag)
  def has(flag: Int) = hasAll(flag)
  def hasAll(flags: Int*) = flags map {f: Int => (f & mask) > 0} reduceLeft {_ && _}
  override def toString() = {
    val cs = Capability.CapabilityMap filter {t => has(t._2)} map {_._1} mkString(", ")
    "Capability(" + mask + ": " + cs + ")"
  }
}