package com.twitter.finagle.mysql

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.util.Await
import com.twitter.util.Awaitable
import com.twitter.util.Time
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

/**
 * Tests the transaction functionality of the MySQL client.
 */
class TransactionTest extends AnyFunSuite with MockitoSugar with Matchers {
  private val sqlQuery = "SELECT * FROM FOO"

  private[this] def await[T](t: Awaitable[T]): T = Await.result(t, 1.second)

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

    await(result) must equal("success")
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
        .flatMap { r2 => c.query(sqlQuery).map { r3 => "final response object" } }
    }

    service.requests must equal(
      List(
        "START TRANSACTION",
        sqlQuery,
        "ROLLBACK"
      ).map(QueryRequest(_))
    )

    intercept[RuntimeException] {
      await(res)
    }

    verify(factory, times(1)).apply()
    verify(factory, times(0)).close(any[Time])
  }
}
