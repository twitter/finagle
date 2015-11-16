package com.twitter.finagle.memcached.unit

import com.twitter.finagle.memcached._
import com.twitter.finagle.memcached.protocol.Value
import scala.collection.immutable
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class GetResultTest extends FunSuite with MockitoSugar {

  class Context {
    val value1 = mock[Value]
    val value2 = mock[Value]
    val ex1 = mock[Exception]
    val ex2 = mock[Exception]
    val empty = GetResult()
    val left = GetResult(
      hits = Map("h1" -> value1),
      misses = immutable.Set("m1"),
      failures = Map("f1" -> ex1))
    val right = GetResult(
      hits = Map("h2" -> value2),
      misses = immutable.Set("m2"),
      failures = Map("f2" -> ex2))
  }

  test("add together hits/misses/failures with ++") {
    val context = new Context
    import context._

    info("both empty")
    assert(empty ++ empty == empty)

    info("non-empty left, empty right")
    assert(left ++ empty == left)

    info("Empty left, non-empty right")
    assert(empty ++ right == right)

    info("non-empty left, non-empty right")
    assert(left ++ right == GetResult(
      hits = Map("h1" -> value1, "h2" -> value2),
      misses = immutable.Set("m1", "m2"),
      failures = Map("f1" -> ex1, "f2" -> ex2)
    ))    
  }

  test("merged of empty seq produces empty GetResult") {
    val context = new Context
    import context._

    assert(GetResult.merged(Seq[GetResult]()) == GetResult())
  }

  test("merged of single item produces that item") {
    val context = new Context
    import context._

    val getResult = GetResult()
    assert(GetResult.merged(Seq(getResult)) == getResult)
  }

  test("merge is the same as ++") {
    val context = new Context
    import context._

    val subResults = (1 to 10) map { i =>
      GetResult(
        hits = Map("h" + i -> mock[Value]),
        misses = immutable.Set("m" + i),
        failures = Map("f" + i -> mock[Exception]))
    }
    
    assert(GetResult.merged(subResults) == (subResults.reduceLeft { _ ++ _ }))
  }
}
