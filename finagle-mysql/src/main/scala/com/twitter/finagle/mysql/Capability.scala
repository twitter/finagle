package com.twitter.finagle.exp.mysql

object Capability {
  val LongPassword     = 0x1      // new more secure passwords
  val FoundRows        = 0x2      // Found instead of affected rows
  val LongFlag         = 0x4      // Get all column flags
  val ConnectWithDB    = 0x8      // One can specify db on connect
  val NoSchema         = 0x10     // Don't allow database.table.column
  val Compress         = 0x20     // Can use compression protocol
  val ODBC             = 0x40     // Odbc client
  val LocalFiles       = 0x80     // Can use LOAD DATA LOCAL
  val IgnoreSpace      = 0x100    // Ignore spaces before '('
  val Protocol41       = 0x200    // New 4.1 protocol
  val Interactive      = 0x400    // This is an interactive client
  val SSL              = 0x800    // Switch to SSL after handshake
  val IgnoreSigPipe    = 0x1000   // IGNORE sigpipes
  val Transactions     = 0x2000   // Client knows about transactions
  val SecureConnection = 0x8000   // New 4.1 authentication
  val MultiStatements  = 0x10000  // Enable/disable multi-stmt support
  val MultiResults     = 0x20000  // Enable/disable multi-results
  val PluginAuth       = 0x80000  // supports auth plugins

  val CapabilityMap = Map(
    "CLIENT_LONG_PASSWORD"     -> LongPassword,
    "CLIENT_FOUND_ROWS"        -> FoundRows,
    "CLIENT_LONG_FLAG"         -> LongFlag,
    "CLIENT_CONNECT_WITH_DB"   -> ConnectWithDB,
    "CLIENT_NO_SCHEMA"         -> NoSchema,
    "CLIENT_COMPRESS"          -> Compress,
    "CLIENT_ODBC"              -> ODBC,
    "CLIENT_LOCAL_FILES"       -> LocalFiles,
    "CLIENT_IGNORE_SPACE"      -> IgnoreSpace,
    "CLIENT_PROTOCOL_41"       -> Protocol41,
    "CLIENT_INTERACTIVE"       -> Interactive,
    "CLIENT_SSL"               -> SSL,
    "CLIENT_IGNORE_SIGPIPE"    -> IgnoreSigPipe,
    "CLIENT_TRANSACTIONS"      -> Transactions,
    "CLIENT_SECURE_CONNECTION" -> SecureConnection,
    "CLIENT_MULTI_STATEMENTS"  -> MultiStatements,
    "CLIENT_MULTI_RESULTS"     -> MultiResults
  )

  /**
   * Encapsulates this client's base
   * capability.
   */
  val baseCap = Capability(
    Capability.LongFlag,
    Capability.Transactions,
    Capability.Protocol41,
    Capability.FoundRows,
    Capability.Interactive,
    Capability.LongPassword,
    Capability.ConnectWithDB,
    Capability.SecureConnection,
    Capability.LocalFiles
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
