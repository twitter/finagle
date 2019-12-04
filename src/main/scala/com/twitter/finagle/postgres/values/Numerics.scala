package com.twitter.finagle.postgres.values

import java.math.BigInteger

import io.netty.buffer.{ByteBuf, Unpooled}

private object Numerics {
  private val NUMERIC_POS = 0x0000
  private val NUMERIC_NEG = 0x4000
  private val NUMERIC_NAN = 0xC000
  private val NUMERIC_NULL = 0xF000
  private val NumericDigitBaseExponent = 4
  val biBase = BigInteger.valueOf(10000)

  private def getUnsignedShort(buf: ByteBuf) = {
    val high = buf.readByte().toInt
    val low = buf.readByte()
    (high << 8) | low
  }

  private def base10exponent(in: java.math.BigDecimal) = {
     in.round(new java.math.MathContext(1)).scale() * -1
  }

  def readNumeric(buf: ByteBuf) = {
    val len = getUnsignedShort(buf)
    val weight = buf.readShort()
    val sign = getUnsignedShort(buf)
    val displayScale = getUnsignedShort(buf)

    //digits are actually unsigned base-10000 numbers (not straight up bytes)
    val digits = Array.fill(len)(buf.readShort())
    val bdDigits = digits.map(BigDecimal(_))

    if(bdDigits.length > 0) {
      val unscaled = bdDigits.foldLeft(BigDecimal(0)) {
        case (acc, n) => acc * 10000 + n
      }

      val scaleFactor = (weight  + 1 - len) * NumericDigitBaseExponent
      val unsigned = unscaled.bigDecimal.movePointRight(scaleFactor).setScale(displayScale)

      sign match {
        case NUMERIC_POS => BigDecimal(unsigned)
        case NUMERIC_NEG => BigDecimal(unsigned.negate())
        case NUMERIC_NAN => throw new NumberFormatException("Decimal is NaN")
        case NUMERIC_NULL => throw new NumberFormatException("Decimal is NUMERIC_NULL")
      }
    } else {
      BigDecimal(0)
    }
  }

  def writeNumeric(in: BigDecimal) = {
    val minimized = BigDecimal(in.bigDecimal.stripTrailingZeros())
    val unscaled = minimized.bigDecimal.unscaledValue().abs()
    val sign = minimized.signum

    def findDigits(i: BigInteger, current: List[Short] = Nil): List[Short] = if(i.signum() != 0) {
      val Array(q, r) = i.divideAndRemainder(biBase)
      findDigits(q, r.shortValue() :: current)
    } else current

    //the decimal point must align on a base-10000 digit
    val padZeroes = 4 - (minimized.scale % 4)
    val paddedUnscaled = Option(padZeroes)
      .filterNot(_ == 0)
      .map(a => BigInteger.valueOf(10).pow(a))
      .map(unscaled.multiply)
      .getOrElse(unscaled)

    val digits = findDigits(paddedUnscaled, Nil)

    val weight = {
      val powers10 = base10exponent(in.bigDecimal) - base10exponent(new java.math.BigDecimal(paddedUnscaled))
      val mod4 = if (powers10 % 4 >= 0) powers10 % 4 else 4 + powers10 % 4

      digits.length + (powers10 - mod4) / 4 - 1
    }
    val bufSize =
      2 + //digit length
      2 + //weight
      2 + //sign
      2 + //scale
      digits.length * 2 //a short for each digit

    val buf = Unpooled.wrappedBuffer(new Array[Byte](bufSize))
    val scale = if(in.scale < 0) 0 else in.scale
    buf.resetWriterIndex()
    buf.writeShort(digits.length)
    buf.writeShort(weight)
    buf.writeShort(if(sign < 0) NUMERIC_NEG else NUMERIC_POS)
    buf.writeShort(scale)
    digits foreach (d => buf.writeShort(d))
    buf
  }
}
