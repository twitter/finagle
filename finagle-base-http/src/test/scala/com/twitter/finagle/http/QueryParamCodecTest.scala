package com.twitter.finagle.http

import com.twitter.conversions.DurationOps._
import com.twitter.util.Stopwatch
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import org.scalacheck.Gen
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import scala.jdk.CollectionConverters._
import org.scalatest.funsuite.AnyFunSuite

class QueryParamCodecTest
    extends AnyFunSuite
    with ScalaCheckDrivenPropertyChecks
    with Eventually
    with IntegrationPatience {

  private def encode(s: String): String = URLEncoder.encode(s, StandardCharsets.UTF_8.name)

  private def genNonEmptyToken: Gen[String] =
    Gen.nonEmptyListOf(Gen.choose('a', 'z')).map(s => new String(s.toArray))

  private def genToken: Gen[String] =
    Gen.listOf(Gen.choose('a', 'z')).map(s => new String(s.toArray))

  private def genKeyValuePair: Gen[(String, String)] =
    for {
      k <- genNonEmptyToken
      v <- genToken
    } yield k -> v

  private def genParams: Gen[Seq[(String, String)]] = Gen.listOf(genKeyValuePair).map(_.sorted)

  private def roundTrip(params: Seq[(String, String)]): Unit = {
    val queryString = QueryParamEncoder.encode(params)
    val result = QueryParamDecoder.decode(queryString)
    val flattened = result.asScala.toSeq.flatMap {
      case (key, values) =>
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
    forAll(genParams) { params => roundTrip(params) }
  }

  test("Decode a uri without a query string") {
    assert(QueryParamDecoder.decode("foo.com").isEmpty)
    assert(QueryParamDecoder.decode("foo.com/bar").isEmpty)
    assert(QueryParamDecoder.decode("foo.com/bar?").isEmpty)
  }

  test("Encode an empty query string") {
    assert(QueryParamEncoder.encode(Map.empty) == "")
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

  private def collisions(num: Int, length: Int): Iterator[String] = {
    val equiv = Array("Aa", "BB")

    // returns a length 2n string, indexed by x
    // 0 <= x < 2^n
    // for a fixed n, all strings have the same hashCode
    def f(x: Int, n: Int): String = n match {
      case 0 => ""
      case _ => equiv(x % 2) + f(x / 2, n - 1)
    }

    (0 until num).iterator.map(f(_, length))
  }

  // On Travis CI, we've seen this test take over 5.seconds.
  if (!sys.props.contains("SKIP_FLAKY_TRAVIS"))
    test("massive number of collisions isn't super slow") {
      // Using a quad core laptop i7 (single threaded) this many params took 399771 ms
      // for scala HashMap and 277 ms using the Java LinkedHashMap on Java 8.
      val num = 100 * 1000

      val cs = collisions(num, 22)
      val queryString = cs.map(_ + "=a").mkString("?", "&", "")

      eventually {
        val stopwatch = Stopwatch.start()
        val result = QueryParamDecoder.decode(queryString)
        assert(result.size == num)
        // we give a generous 2 seconds to complete, 10x what was observed in local
        // testing because CI can be slow at times. We'd expect quadratic behavior
        // to take two orders of magnitude longer, so just making sure it's below
        // 2 seconds should be enough to confirm we're not vulnerable to DoS attack.
        assert(stopwatch() < 2.seconds)
      }
    }
}
