package com.twitter.finagle.exp.mysql

import java.nio.charset.{Charset => JCharset}

object Charset {
  /**
   * Default Charset used by this client.
   */
  val defaultCharset = JCharset.forName("UTF-8")

  /**
   * Converts from mysql charset to java charset.
   */
  def apply(charset: Short): JCharset =
    if (isUtf8(charset))
      JCharset.forName("UTF-8")
    else if (isLatin1(charset))
      JCharset.forName("ISO-8859-1")
    else if (isBinary(charset))
      JCharset.forName("US-ASCII")
    else
      throw new IllegalArgumentException("Charset %d is not supported.".format(charset))

  /**
   * SELECT id,collation_name FROM information_schema.collations
   * WHERE `collation_name` LIKE 'latin1%' ORDER BY id;
   */
  private[this] val Latin1Set = Set(5, 8, 15, 31, 47, 48, 49, 94)

  /**
   * "SELECT id,collation_name FROM information_schema.collations
   * WHERE collation_name LIKE '%utf8' ORDER BY id"
   */
  private[this] val Utf8Set = Set(192 to 254:_*) + 33 + 45 + 46 + 83

  /**
   * Some useful charset constants
   */
  val Utf8_general_ci = 33.toShort
  val Binary = 63.toShort

  private[this] val CompatibleSet = Latin1Set ++ Utf8Set + Binary
  def isCompatible(code: Short): Boolean = CompatibleSet(code)
  def isUtf8(code: Short): Boolean = Utf8Set(code)
  def isLatin1(code: Short): Boolean = Latin1Set(code)
  def isBinary(code: Short): Boolean = code == Binary
}
