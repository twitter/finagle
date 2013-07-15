package com.twitter.finagle.memcached.unit

import com.twitter.finagle.memcached._
import com.twitter.finagle.memcached.protocol.Value
import org.specs.mock.Mockito
import org.specs.SpecificationWithJUnit
import scala.collection.immutable

class GetResultSpec extends SpecificationWithJUnit with Mockito {

  "GetResult" should {
    "add together hits/misses/failures with ++" in {
      val value1 = mock[Value]
      val value2 = mock[Value]
      val ex1 = mock[Exception]
      val ex2 = mock[Exception]
      val left = GetResult(
        hits = Map("h1" -> value1),
        misses = immutable.Set("m1"),
        failures = Map("f1" -> ex1))
      val right = GetResult(
        hits = Map("h2" -> value2),
        misses = immutable.Set("m2"),
        failures = Map("f2" -> ex2))
      val empty = GetResult()

      "both empty" in {
        empty ++ empty mustEqual empty
      }

      "non-empty left, empty right" in {
        left ++ empty mustEqual left
      }

      "empty left, non-empty right" in {
        empty ++ right mustEqual right
      }

      "non-empty left, non-empty right" in {
        left ++ right mustEqual GetResult(
          hits = Map("h1" -> value1, "h2" -> value2),
          misses = immutable.Set("m1", "m2"),
          failures = Map("f1" -> ex1, "f2" -> ex2)
        )
      }
    }
  }

  "object GetResult" should {
    "merged of empty seq produces empty GetResult" in {
      GetResult.merged(Seq[GetResult]()) mustEqual GetResult()
    }

    "merged of single item produces that item" in {
      val getResult = GetResult()
      GetResult.merged(Seq(getResult)) mustBe getResult
    }

    "merge is the same as ++" in {
      val subResults = (1 to 10) map { i =>
        GetResult(
          hits = Map("h" + i -> mock[Value]),
          misses = immutable.Set("m" + i),
          failures = Map("f" + i -> mock[Exception]))
      }

      GetResult.merged(subResults) mustEqual (subResults.reduceLeft { _ ++ _ })
    }
  }
}
