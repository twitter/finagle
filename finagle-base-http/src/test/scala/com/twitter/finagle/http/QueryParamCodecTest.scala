package com.twitter.finagle.http

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import org.scalacheck.Gen
import org.scalatest.FunSuite
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import scala.collection.JavaConverters._

class QueryParamCodecTest extends FunSuite with GeneratorDrivenPropertyChecks {

  private def encode(s: String): String = URLEncoder.encode(s, StandardCharsets.UTF_8.name)

  private def genNonEmptyToken: Gen[String] =
    Gen.nonEmptyListOf(Gen.choose('a', 'z')).map(s => new String(s.toArray))

  private def genToken: Gen[String] =
    Gen.listOf(Gen.choose('a', 'z')).map(s => new String(s.toArray))

  private def genKeyValuePair: Gen[(String, String)] = for {
    k <- genNonEmptyToken
    v <- genToken
  } yield k -> v

  private def genParams: Gen[Seq[(String, String)]] = Gen.listOf(genKeyValuePair).map(_.sorted)

  private def roundTrip(params: Seq[(String, String)]): Unit = {
    val queryString = QueryParamEncoder.encode(params)
    val result = QueryParamDecoder.decode(queryString)
    val flattened = result.asScala.toSeq.flatMap { case (key, values) =>
      values.asScala.map(key -> _)
    }.sorted

    assert(flattened == params)
  }

  private val samples = Seq(
    "a" -> "b",
    "a" -> "b", // nobody says you can't repeat a value
    "empty value" -> "", // empty value
    "empty value" -> "", // nobody says you can't repeat an empty value
    "$$&&%;" -> "LOL!!&&;Z",
    encode("$$&&%;") -> encode("LOL!!&&;Z") // should be able to encode encoded samples
  ).sorted

  test("Will round trip explicit samples") {
    roundTrip(samples)
  }

  test("arbitrary keys and values") {
    forAll(genParams) { params =>
      roundTrip(params)
    }
  }

  test("Decode a uri without a query string") {
    assert(QueryParamDecoder.decode("foo.com").isEmpty)
    assert(QueryParamDecoder.decode("foo.com/bar").isEmpty)
    assert(QueryParamDecoder.decode("foo.com/bar?").isEmpty)
  }

  test("Encode an empty query string") {
    assert(QueryParamEncoder.encode(Map.empty) == "")
  }

  test("Limits decoding to 1024 params with different keys") {
    val tooLongQueryString = (0 until 1025).map { i =>
      s"key$i=value$i"
    }.mkString("&")

    val results = QueryParamDecoder.decode("?" + tooLongQueryString)
    assert(results.size == 1024)

    (0 until 1024).foreach { i =>
      val values = results.get(s"key$i")
      assert(values.size == 1)
      assert(values.get(0) == s"value$i")
    }
  }

  test("Limits decoding to 1024 params for the same key") {
    val tooLongQueryString = (0 until 1025).map { i =>
      s"key=value$i"
    }.mkString("&")

    val results = QueryParamDecoder.decode("?" + tooLongQueryString)
    assert(results.size == 1)
    val values = results.get("key")
    assert(values.size == 1024)
    (0 until 1024).foreach { i =>
      assert(values.get(i) == s"value$i")
    }
  }

  test("Decodes both '%20' and '+' as `space`") {
    Seq(
      "?foo%20=bar",
      "?foo+=bar"
    ).foreach { s =>
      val result = QueryParamDecoder.decode(s)
      assert(result.size == 1)
      val params = result.get("foo ")
      assert(params.size == 1)
      assert(params.get(0) == "bar")
    }
  }

  test("Illegal query params") {
    Seq(
      "?f%tf=bar", // Illegal hex char
      "?foo%=bar", // missing hex chars
      "?foo%d=bar" // missing hex char
    ).foreach { uri =>
      intercept[IllegalArgumentException] {
        QueryParamDecoder.decode(uri)
      }
    }
  }
}
