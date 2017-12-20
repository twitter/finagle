package com.twitter.finagle.mysql

import com.twitter.conversions.time._
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.util.{Await, Time}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FunSuite, MustMatchers}

/**
 * Tests the transaction functionality of the MySQL client.
 */
class TransactionTest extends FunSuite with MockitoSugar with MustMatchers {
  private val sqlQuery = "SELECT * FROM FOO"

  test("transaction test uses a single service repeatedly and closes it upon completion") {
    val service = new MockService()
    val factory = spy(new MockServiceFactory(service))
    val client = Client(factory, NullStatsReceiver, supportUnsigned = false)

    val result = client.transactionWithIsolation[String](IsolationLevel.ReadCommitted) { c =>
      for {
        _ <- c.query(sqlQuery)
        _ <- c.query(sqlQuery)
      } yield "success"
    }

    Await.result(result) must equal("success")
    service.requests must equal(
      List(
        "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
        "START TRANSACTION",
        sqlQuery,
        sqlQuery,
        "COMMIT"
      ).map(QueryRequest(_))
    )

    verify(factory, times(1)).apply()
    verify(factory, times(0)).close(any[Time])
  }

  test("transaction test rollback") {
    val service = new MockService()
    val factory = spy(new MockServiceFactory(service))
    val client = Client(factory, NullStatsReceiver, supportUnsigned = false)

    val res = client.transaction[String] { c =>
      c.query(sqlQuery)
        .map { r1 =>
          throw new RuntimeException("Fake exception to trigger ROLLBACK")
          "first response object"
        }
        .flatMap { r2 =>
          c.query(sqlQuery).map { r3 =>
            "final response object"
          }
        }
    }

    service.requests must equal(
      List(
        "START TRANSACTION",
        sqlQuery,
        "ROLLBACK"
      ).map(QueryRequest(_))
    )

    intercept[RuntimeException] {
      Await.result(res, 5.seconds)
    }

    verify(factory, times(1)).apply()
    verify(factory, times(0)).close(any[Time])
  }
}
