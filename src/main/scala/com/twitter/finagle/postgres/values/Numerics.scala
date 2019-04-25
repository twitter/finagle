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

  def readNumeric(buf: ByteBuf) = {
    val len = getUnsignedShort(buf)
    val weight = buf.readShort()
    val sign = getUnsignedShort(buf)
    val displayScale = getUnsignedShort(buf)

    //digits are actually unsigned base-10000 numbers (not straight up bytes)
    val digits = Array.fill(len)(buf.readShort())
    val bdDigits = digits.map(BigDecimal(_))

    if(bdDigits.length > 0) {
      val unscaled = bdDigits.tail.foldLeft(bdDigits.head) {
        case (accum, digit) => BigDecimal(accum.bigDecimal.scaleByPowerOfTen(NumericDigitBaseExponent)) + digit
      }

      val firstDigitSize =
        if (digits.head < 10) 1
        else if (digits.head < 100) 2
        else if (digits.head < 1000) 3
        else 4

      val scaleFactor = weight * NumericDigitBaseExponent + firstDigitSize
      val unsigned = unscaled.bigDecimal.movePointLeft(unscaled.precision).movePointRight(scaleFactor).setScale(displayScale)

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
    val unscaled = minimized.bigDecimal.unscaledValue()
    val sign = minimized.signum

    def findDigits(i: BigInteger, current: List[Short] = Nil): List[Short] = if(i.signum() != 0) {
      val Array(q, r) = i.divideAndRemainder(biBase)
      findDigits(q, r.shortValue() :: current)
    } else current

    val beforeDecimal = minimized.precision - minimized.scale
    //the decimal point must align on a base-10000 digit
    val padZeroes = 4 - (minimized.scale % 4)
    val paddedUnscaled = Option(padZeroes)
      .filterNot(_ == 0)
      .map(a => BigInteger.valueOf(10).pow(a))
      .map(unscaled.multiply)
      .getOrElse(unscaled)

    val digits = findDigits(paddedUnscaled, Nil)

    val weight = if(digits.nonEmpty) {
      val firstDigitSize =
        if (digits.head < 10) 1
        else if (digits.head < 100) 2
        else if (digits.head < 1000) 3
        else 4
      (beforeDecimal - firstDigitSize) / 4
    }
    else
      0
    val bufSize =
      2 + //digit length
      2 + //weight
      2 + //sign
      2 + //scale
      digits.length * 2 //a short for each digit

    val buf = Unpooled.wrappedBuffer(new Array[Byte](bufSize))

    buf.resetWriterIndex()
    buf.writeShort(digits.length)
    buf.writeShort(weight)
    buf.writeShort(if(sign < 0) NUMERIC_NEG else NUMERIC_POS)
    buf.writeShort(in.scale)
    digits foreach (d => buf.writeShort(d))

    buf
  }
}
