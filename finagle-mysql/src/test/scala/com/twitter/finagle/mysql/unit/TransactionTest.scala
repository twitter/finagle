package com.twitter.finagle.exp.mysql

import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{MustMatchers, FunSuite}
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.mockito.Matchers._
import com.twitter.util.{Await, Future, Time}

/**
 * Tests the transaction functionality of the MySQL client.
 */
@RunWith(classOf[JUnitRunner])
class TransactionTest extends FunSuite with MockitoSugar with MustMatchers {
  private val sqlQuery = "SELECT * FROM FOO"

  test("transaction test uses a single service repeatedly and closes it upon completion") {
    val service = new MockService()
    val factory = spy(new MockServiceFactory(service))
    val client = Client(factory)

    val result = client.transaction[String] { c =>
      for {
        r1 <- c.query(sqlQuery)
        r2 <- c.query(sqlQuery)
      } yield "success"
    }

    Await.result(result) must equal ("success")
    service.requests must equal (List(
      "START TRANSACTION", sqlQuery, sqlQuery, "COMMIT"
    ).map(QueryRequest(_)))

    verify(factory, times(1)).apply()
    verify(factory, times(0)).close(any[Time])
  }

  test("transaction test rollback") {
    val service = new MockService()
    val factory = spy(new MockServiceFactory(service))
    val client = Client(factory)

    try {
      client.transaction[String] { c =>
        c.query(sqlQuery).map { r1 =>
          throw new RuntimeException("Fake exception to trigger ROLLBACK")
          "first response object"
        }.flatMap { r2 =>
          c.query(sqlQuery).map { r3 =>
            "final response object"
          }
        }
      }
    } catch {
      case e: Exception =>
    }

    service.requests must equal (List(
      "START TRANSACTION", sqlQuery, "ROLLBACK"
    ).map(QueryRequest(_)))

    verify(factory, times(1)).apply()
    verify(factory, times(0)).close(any[Time])
  }
}

