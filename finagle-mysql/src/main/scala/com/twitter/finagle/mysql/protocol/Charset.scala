package com.twitter.finagle.exp.mysql.protocol

import java.nio.charset.{Charset => JCharset}

object Charset {

  /**
    * Converts from mysql charset to java charset.
    */
  def apply(charset: Short): JCharset = if (isUtf8(charset) || isBinary(charset))
    JCharset.forName("UTF-8")
  else if (isLatin1(charset))
    JCharset.forName("ISO-8859-1")
  else
    throw new IllegalArgumentException("Charset %d is not supported.".format(charset))

  /**
   * Default Charset to use when decoding strings.
   */
  val defaultCharset = JCharset.forName("UTF-8")

  /**
   * MySQL UTF-8 Collations.
   */
  val Latin1_bin               = 47.toShort
  val Latin1_danish_ci         = 15.toShort
  val Latin1_general_ci        = 48.toShort
  val Latin1_general_cs        = 49.toShort
  val Latin1_german1_c1        = 5.toShort
  val Latin1_german2_ci        = 31.toShort
  val Latin1_spanish_ci        = 94.toShort
  val Latin1_swedish_c1        = 8.toShort
  val Utf8_bin                 = 83.toShort
  val Utf8_czech_ci            = 202.toShort
  val Utf8_danish_ci           = 203.toShort
  val Utf8_esperanto_ci        = 209.toShort
  val Utf8_estonian_ci         = 198.toShort
  val Utf8_general_ci          = 33.toShort
  val Utf8_general_mysql500_ci = 223.toShort
  val Utf8_hungarian_ci        = 210.toShort
  val Utf8_icelandic_ci        = 193.toShort
  val Utf8_latvian_ci          = 194.toShort
  val Utf8_lithuanian_ci       = 204.toShort
  val Utf8_persian_ci          = 208.toShort
  val Utf8_polish_ci           = 197.toShort
  val Utf8_romanian_ci         = 195.toShort
  val Utf8_roman_ci            = 207.toShort
  val Utf8_sinhala_ci          = 211.toShort
  val Utf8_slovak_ci           = 205.toShort
  val Utf8_spanish2_ci         = 206.toShort
  val Utf8_spanish_ci          = 199.toShort
  val Utf8_swedish_ci          = 200.toShort
  val Utf8_turkish_ci          = 201.toShort
  val Utf8_unicode_ci          = 192.toShort

  private[this] val Latin1Set = Set(
    Latin1_bin,
    Latin1_danish_ci,
    Latin1_general_ci,
    Latin1_general_cs,
    Latin1_german1_c1,
    Latin1_german2_ci,
    Latin1_spanish_ci,
    Latin1_swedish_c1
  )

  private[this] val Utf8Set = Set(
    Utf8_bin,
    Utf8_czech_ci,
    Utf8_danish_ci,
    Utf8_esperanto_ci,
    Utf8_estonian_ci,
    Utf8_general_ci,
    Utf8_general_mysql500_ci,
    Utf8_hungarian_ci,
    Utf8_icelandic_ci,
    Utf8_latvian_ci,
    Utf8_lithuanian_ci,
    Utf8_persian_ci,
    Utf8_polish_ci,
    Utf8_romanian_ci,
    Utf8_roman_ci,
    Utf8_sinhala_ci,
    Utf8_slovak_ci,
    Utf8_spanish2_ci,
    Utf8_spanish_ci,
    Utf8_swedish_ci,
    Utf8_turkish_ci,
    Utf8_unicode_ci
  )

  private[this] val Binary = 63.toShort

  private[this] val CompatibleSet = Latin1Set ++ Utf8Set

  def isCompatible(code: Short): Boolean = CompatibleSet(code)
  def isUtf8(code: Short): Boolean = Utf8Set(code)
  def isLatin1(code: Short): Boolean = Latin1Set(code)
  def isBinary(code: Short): Boolean = code == Binary
}
