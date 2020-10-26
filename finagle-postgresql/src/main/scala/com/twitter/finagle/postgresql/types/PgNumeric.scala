package com.twitter.finagle.postgresql.types

import com.twitter.finagle.postgresql.PgSqlClientError
import com.twitter.finagle.postgresql.Types.Numeric
import com.twitter.finagle.postgresql.Types.NumericSign

/**
 * Some utilities to deal with Postgres' internal numeric representation.
 */
object PgNumeric {

  // Numeric digits are base-10000
  // So every "numeric digit" represents 4 decimal places.
  private[this] val NumericBase = 4
  private[this] val Base10000 = java.math.BigInteger.valueOf(10000)
  private[postgresql] val NumericZero =
    Numeric(weight = 0, sign = NumericSign.Positive, displayScale = 0, digits = Seq.empty)

  val Zero = BigDecimal(0)

  private[this] def numericToUnsignedBigDecimal(n: Numeric): java.math.BigDecimal =
    if (n.digits.isEmpty) java.math.BigDecimal.ZERO
    else {
      val unscaled = n.digits.foldLeft(java.math.BigDecimal.ZERO) { case (acc, d) =>
        acc.movePointRight(NumericBase).add(java.math.BigDecimal.valueOf(d.toLong))
      }

      // https://github.com/postgres/postgres/blob/7538708394e7a70105a4e601e253adf80f47cca8/src/backend/utils/adt/numeric.c#L263-L265
      // "There are weight+1 digits before the decimal point."
      // In this case, "digit" means "numeric digits", not decimal digits.
      //
      // So if there are a total of 5 numeric digits and weight is 2
      // Then there are 3 numeric digits before the decimal point or 12 (3 * 4) decimal digits
      // So we could move the decimal point 12 places to the left,
      // but because we've lost the leading zeroes, that wouldn't work.
      // So instead we leverage the trailing zeroes and move the decimal point
      // from "the right" using a negative value.
      // In our example, we want to move 2 (5 - 3) numeric digits from the right, or 8 decimal digits.
      val scale = (n.weight + 1 - n.digits.length) * NumericBase
      unscaled.movePointRight(scale).setScale(n.displayScale)
    }

  def numericToBigDecimal(n: Numeric): BigDecimal =
    n.sign match {
      case NumericSign.Positive => numericToUnsignedBigDecimal(n)
      case NumericSign.Negative => numericToUnsignedBigDecimal(n).negate()
      case NumericSign.NaN => throw new PgSqlClientError("NaN cannot be converted to BigDecimal")
      case NumericSign.Infinity => throw new PgSqlClientError("+Inf cannot be converted to BigDecimal")
      case NumericSign.NegInfinity => throw new PgSqlClientError("-Inf cannot be converted to BigDecimal")
    }

  def bigDecimalToNumeric(bd: BigDecimal): Numeric =
    if (bd.signum == 0) NumericZero
    else {

      val striped = bd.underlying.stripTrailingZeros()
      val unscaled = BigInt(striped.unscaledValue().abs())

      // Align decimal digits to numeric digits
      // pad is the number of zeros that we must add to the last digit
      // scale tells us how many digits are after the decimal point (or how many times we must multiple by 10)
      // Taking % 4 of that number tells us how many non-zero digits there are
      // So subtracting from 4 tells us how many zeroes to add (% 4 again since adding 4 is the same as adding none)
      val pad = (4 - striped.scale() % 4) % 4
      var padded =
        if (pad == 0) unscaled.underlying
        else {
          (unscaled * BigInt(math.pow(10, pad.toDouble).toInt)).underlying
        }

      var digits = List.empty[Short]
      // We iteratively divide by 10000 to get the individual "numeric digits"
      while (padded.signum() != 0) {
        val Array(p, digit) = padded.divideAndRemainder(Base10000)
        digits = digit.shortValueExact() :: digits
        padded = p
      }

      // Weight:
      // When scale is positive, all decimal digits are to the left of the decimal point.
      // This means that all values in "digits" must at least be included in weight.
      // Furthermore, scale tells us how many additional zeros to add, so we divide that by 4
      // to get the number of zeros in "numeric digits" to add.
      //
      // When scale is negative, a certain number of decimal digits are to the right of the point.
      // We compute the number digits right of the point by taking scale and adding the number of zeroes we added when padding.
      // We divide that by 4 to get the number of "numeric digits" right of the point.
      // Then we subtract that from the number of total digits (this also means weight can be null).
      val weight =
        if (striped.scale <= 0) {
          striped.scale.abs / 4 + digits.length
        } else {
          val decimalDigits = striped.scale + pad
          digits.length - decimalDigits / 4
        }

      Numeric(
        sign = if (bd.signum > 0) NumericSign.Positive else NumericSign.Negative,
        // we have to subtract 1 since the definition is "weight + 1 digits before the point"
        weight = (weight - 1).toShort,
        displayScale = if (bd.scale < 0) 0 else bd.scale,
        digits = digits
      )
    }

}
