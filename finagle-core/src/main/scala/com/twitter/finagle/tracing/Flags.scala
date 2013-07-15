package com.twitter.finagle.tracing

object Flags {
  /*
   * The debug flag is used to ensure this the current trace passes all of the sampling stages.
   */
  val Debug = 1L << 0 // 1

  /**
   * @return a flags instance with no flags set.
   */
  def apply(): Flags = Flags(0L)
}

/**
 * Represents flags that can be passed along in request headers.
 * @param flags Initial flag state. May be 0.
 */
case class Flags(flags: Long) {

  /**
   * @param field Is this flag set or not? Pass a field identifier from the Flags object.
   */
  def isFlagSet(field: Long): Boolean = { (flags & field) == field }

  /**
   * @param field Field to set. Pass a field identifier from the Flags object.
   * @return a new Flags object with the flag set
   */
  def setFlag(field: Long): Flags = Flags(flags | field)

  /**
   * @param fields Fields to set. Pass field identifiers from the Flags object.
   * @return a new Flags object with the flags set
   */
  def setFlags(fields: Seq[Long]): Flags = {
    Flags(fields.reduceLeft((a, b) => a | b))
  }

  /**
   * Convenience method to check if the debug flag is set.
   */
  def isDebug = isFlagSet(Flags.Debug)

  /**
   * Convenience method to set the debug flag.
   */
  def setDebug: Flags = setFlag(Flags.Debug)

  /**
   * @return a long that we can use to pass in the tracing header
   */
  def toLong = flags
}