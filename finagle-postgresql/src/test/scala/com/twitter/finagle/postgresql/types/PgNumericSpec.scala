package com.twitter.finagle.postgresql.types

import com.twitter.finagle.postgresql.PgSqlClientError
import com.twitter.finagle.postgresql.PgSqlSpec
import com.twitter.finagle.postgresql.PropertiesSpec
import com.twitter.finagle.postgresql.Types.Numeric
import com.twitter.finagle.postgresql.Types.NumericSign

class PgNumericSpec extends PgSqlSpec with PropertiesSpec {

  "PgNumeric" should {

    def num(w: Short, s: Short, digits: Seq[Short], sign: NumericSign) =
      Numeric(weight = w, sign = sign, displayScale = s.toInt, digits = digits)

    "bigDecimalToNumeric" should {
      import PgNumeric.bigDecimalToNumeric

      def check(bd: String, w: Short, s: Short, digits: Seq[Short]) = {
        bigDecimalToNumeric(BigDecimal(bd)) must_== num(w, s, digits, NumericSign.Positive)
        bigDecimalToNumeric(BigDecimal(bd) * -1) must_== num(w, s, digits, NumericSign.Negative)
      }

      "convert 0 to NumericZero" in {
        bigDecimalToNumeric(BigDecimal(0)) must_== PgNumeric.NumericZero
      }

      "convert decimals" in {
        check("0.1", -1, 1, Seq(1000))
        check("0.1", -1, 1, Seq(1000))
        check("0.01", -1, 2, Seq(100))
        check("0.001", -1, 3, Seq(10))
        check("0.0001", -1, 4, Seq(1))
        check("0.00001", -2, 5, Seq(1000))
        check("0.000001", -2, 6, Seq(100))
        check("0.0000001", -2, 7, Seq(10))
        check("0.00000001", -2, 8, Seq(1))
      }

      "convert integers" in {
        check("1", 0, 0, Seq(1))
        check("10", 0, 0, Seq(10))
        check("100", 0, 0, Seq(100))
        check("1000", 0, 0, Seq(1000))
        check("10000", 1, 0, Seq(1))
        check("100000", 1, 0, Seq(10))
        check("1000000", 1, 0, Seq(100))
        check("10000000", 1, 0, Seq(1000))
      }

      "convert bigdecimal" in {
        check("1.1", 0, 1, Seq(1, 1000))
        check("10.01", 0, 2, Seq(10, 100))
        check("100.001", 0, 3, Seq(100, 10))
        check("1000.0001", 0, 4, Seq(1000, 1))
        check("10000.00001", 1, 5, Seq(1, 0, 0, 1000))
        check("100000.000001", 1, 6, Seq(10, 0, 0, 100))
        check("1000000.0000001", 1, 7, Seq(100, 0, 0, 10))
        check("10000000.00000001", 1, 8, Seq(1000, 0, 0, 1))

        check("12345678.87654321", 1, 8, Seq(1234, 5678, 8765, 4321))
      }
    }

    "numericToBigDecimal" should {
      import PgNumeric.numericToBigDecimal

      def check(bd: String, w: Short, s: Short, digits: Seq[Short]) = {
        numericToBigDecimal(num(w, s, digits, NumericSign.Positive)) must_== BigDecimal(bd)
        numericToBigDecimal(num(w, s, digits, NumericSign.Negative)) must_== BigDecimal(bd) * -1
      }

      "convert NumericZero to 0" in {
        numericToBigDecimal(PgNumeric.NumericZero) must_== BigDecimal(0)
      }

      def failFor(s: NumericSign) =
        s"fail for $s" in {
          numericToBigDecimal(Numeric(0, NumericSign.NaN, 0, Nil)) must throwAn[PgSqlClientError]
        }
      failFor(NumericSign.NaN)
      failFor(NumericSign.Infinity)
      failFor(NumericSign.NegInfinity)

      "convert decimals" in {
        check("0.1", -1, 1, Seq(1000))
        check("0.1", -1, 1, Seq(1000))
        check("0.01", -1, 2, Seq(100))
        check("0.001", -1, 3, Seq(10))
        check("0.0001", -1, 4, Seq(1))
        check("0.00001", -2, 5, Seq(1000))
        check("0.000001", -2, 6, Seq(100))
        check("0.0000001", -2, 7, Seq(10))
        check("0.00000001", -2, 8, Seq(1))
      }

      "convert integers" in {
        check("1", 0, 0, Seq(1))
        check("10", 0, 0, Seq(10))
        check("100", 0, 0, Seq(100))
        check("1000", 0, 0, Seq(1000))
        check("10000", 1, 0, Seq(1))
        check("100000", 1, 0, Seq(10))
        check("1000000", 1, 0, Seq(100))
        check("10000000", 1, 0, Seq(1000))
      }

      "convert bigdecimal" in {
        check("1.1", 0, 1, Seq(1, 1000))
        check("10.01", 0, 2, Seq(10, 100))
        check("100.001", 0, 3, Seq(100, 10))
        check("1000.0001", 0, 4, Seq(1000, 1))
        check("10000.00001", 1, 5, Seq(1, 0, 0, 1000))
        check("100000.000001", 1, 6, Seq(10, 0, 0, 100))
        check("1000000.0000001", 1, 7, Seq(100, 0, 0, 10))
        check("10000000.00000001", 1, 8, Seq(1000, 0, 0, 1))

        check("12345678.87654321", 1, 8, Seq(1234, 5678, 8765, 4321))
      }
    }

    "round trip" in prop { bd: BigDecimal =>
      PgNumeric.numericToBigDecimal(PgNumeric.bigDecimalToNumeric(bd)) must_== bd
    }
  }
}
