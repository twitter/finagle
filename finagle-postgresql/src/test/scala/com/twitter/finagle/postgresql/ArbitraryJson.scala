package com.twitter.finagle.postgresql

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.typelevel.jawn.ast.DoubleNum
import org.typelevel.jawn.ast.JArray
import org.typelevel.jawn.ast.JAtom
import org.typelevel.jawn.ast.JFalse
import org.typelevel.jawn.ast.JNull
import org.typelevel.jawn.ast.JObject
import org.typelevel.jawn.ast.JString
import org.typelevel.jawn.ast.JTrue
import org.typelevel.jawn.ast.JValue
import org.typelevel.jawn.ast.LongNum

/**
 * This is copy-pasted from
 * https://github.com/typelevel/jawn/blob/f1b95ecfd51372104a729a7338c2bcd99a478090/ast/src/test/scala/jawn/ArbitraryUtil.scala
 *
 * With the addition of isValidPgString
 */
object ArbitraryJson {

  // JSON doesn't allow NaN, PositiveInfinity, or NegativeInfinity
  def isFinite(n: Double): Boolean =
    !java.lang.Double.isNaN(n) && !java.lang.Double.isInfinite(n)

  // Postgres JSON strings are not allowed to contain the 0 byte.
  def isValidPgString(s: String): Boolean =
    !s.exists(_ == 0)

  val jnull = Gen.const(JNull)
  val jboolean = Gen.oneOf(JTrue, JFalse)
  val jlong = arbitrary[Long].map(LongNum(_))
  val jdouble = arbitrary[Double].filter(isFinite).map(DoubleNum(_))
  val jstring = arbitrary[String].filter(isValidPgString).map(JString(_))

  // Totally unscientific atom frequencies.
  val jatom: Gen[JAtom] =
    Gen.frequency((1, jnull), (8, jboolean), (8, jlong), (8, jdouble), (16, jstring))

  // Use lvl to limit the depth of our jvalues.
  // Otherwise we will end up with SOE real fast.

  val MaxLevel: Int = 3

  def jarray(lvl: Int): Gen[JArray] =
    Gen.containerOf[Array, JValue](jvalue(lvl + 1)).map(JArray(_))

  def jitem(lvl: Int): Gen[(String, JValue)] =
    for { s <- arbitrary[String] if isValidPgString(s); j <- jvalue(lvl) } yield (s, j)

  def jobject(lvl: Int): Gen[JObject] =
    Gen.containerOf[Vector, (String, JValue)](jitem(lvl + 1)).map(JObject.fromSeq)

  def jvalue(lvl: Int = 0): Gen[JValue] =
    if (lvl >= MaxLevel) jatom
    else Gen.frequency((16, jatom), (1, jarray(lvl)), (2, jobject(lvl)))

  implicit lazy val arbitraryJValue: Arbitrary[JValue] =
    Arbitrary(jvalue())
}
