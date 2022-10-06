package com.twitter.finagle.mysql.unit

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.mysql._
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.util.Await
import com.twitter.util.Awaitable
import com.twitter.util.Future
import com.twitter.util.Time
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.spy
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class SessionsTest extends AnyFunSuite with MockitoSugar {
  private[this] val sqlQuery = "SELECT * FROM FOO"

  private[this] def await[T](t: Awaitable[T]): T = Await.result(t, 5.seconds)

  test("sessions uses a single service") {
    val service = spy(new MockService())
    val factory = spy(new MockServiceFactory(service))
    val client = Client(factory, NullStatsReceiver, supportUnsigned = false)

    val result = client.session { session =>
      for {
        _ <- session.query(sqlQuery)
        _ <- session.query(sqlQuery)
      } yield "success"
    }

    assert(await(result) == "success")
    assert(
      service.requests ==
        List(
          sqlQuery,
          sqlQuery
        ).map(QueryRequest.apply _)
    )

    verify(factory, times(1)).apply()
    verify(factory, times(0)).close(any[Time])
    verify(service, times(1)).close(any[Time])
  }

  test("sessions with nested transactions uses a single service") {
    val service = spy(new MockService())
    val factory = spy(new MockServiceFactory(service))
    val client = Client(factory, NullStatsReceiver, supportUnsigned = false)

    val result = client.session { session =>
      for {
        _ <- session.query("LOCK TABLES FOO WRITE")
        r <- session.transaction { tx =>
          for {
            _ <- tx.query(sqlQuery)
            _ <- tx.query(sqlQuery)
          } yield "success"
        }
        _ <- session.query("UNLOCK TABLES")
      } yield r
    }

    assert(await(result) == "success")
    assert(
      service.requests ==
        List(
          "LOCK TABLES FOO WRITE",
          "START TRANSACTION",
          sqlQuery,
          sqlQuery,
          "COMMIT",
          "UNLOCK TABLES"
        ).map(QueryRequest(_))
    )

    verify(factory, times(1)).apply()
    verify(factory, times(0)).close(any[Time])
    verify(service, times(1)).close(any[Time])
  }

  test("multiple nested transactions are not supported:") {
    val service = spy(new MockService())
    val factory = spy(new MockServiceFactory(service))
    val client = Client(factory, NullStatsReceiver, supportUnsigned = false)
    val sqlQuery2 = "SELECT * FROM BAR"
    val result = client.transaction {
      case tx1: Client with Transactions =>
        tx1.query(sqlQuery).flatMap { _ =>
          tx1.transaction {
            case tx2: Client with Transactions =>
              tx2.query(sqlQuery).flatMap { _ => tx2.transaction { tx3 => tx3.query(sqlQuery2) } }
            case _ => fail("Client is assumed to have transactions")
          }
        }
      case _ => fail("Client is assumed to have transactions")
    }
    intercept[IllegalStateException] { await(result) }

    assert(
      service.requests == List(
        "START TRANSACTION",
        sqlQuery,
        "START TRANSACTION",
        sqlQuery,
        "ROLLBACK",
        "ROLLBACK"
      ).map(QueryRequest))
  }

  test("sessions with nested transaction and discard") {
    val service = spy(new MockService())
    val factory = spy(new MockServiceFactory(service))
    val client = Client(factory, NullStatsReceiver, supportUnsigned = false)

    val result = client.session { session =>
      val inner = for {
        _ <- session.query("LOCK TABLES FOO WRITE")
        r <- session.transaction { tx =>
          for {
            _ <- tx.query(sqlQuery)
            _ <- tx.query(sqlQuery)
            _ <- Future.exception(new Exception())
          } yield "success"
        }
        _ <- session.query("UNLOCK TABLES")
      } yield r

      inner.onFailure(_ => session.discard())
    }

    intercept[Exception] { await(result) }
    assert(
      service.requests ==
        List(
          "LOCK TABLES FOO WRITE",
          "START TRANSACTION",
          sqlQuery,
          sqlQuery,
          "ROLLBACK"
        ).map(QueryRequest(_)) :+
          PoisonConnectionRequest
    )

    verify(factory, times(1)).apply()
    verify(factory, times(0)).close(any[Time])
    verify(service, times(1)).close(any[Time])
  }

  test("discarded sessions poison the connection") {
    val service = spy(new MockService())
    val factory = spy(new MockServiceFactory(service))
    val client = Client(factory, NullStatsReceiver, supportUnsigned = false)

    client.session(_.discard())

    assert(service.requests == List(PoisonConnectionRequest))

    verify(factory, times(1)).apply()
    verify(factory, times(0)).close(any[Time])
    verify(service, times(1)).close()
  }

  test("released sessions are returned to the pool") {
    val service = spy(new MockService())
    val factory = spy(new MockServiceFactory(service))
    val client = Client(factory, NullStatsReceiver, supportUnsigned = false)

    client.session(_ => Future.value("foo"))

    verify(factory, times(1)).apply()
    verify(factory, times(0)).close(any[Time])
    verify(service, times(1)).close(any[Time])
  }
}
