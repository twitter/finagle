package com.twitter.finagle.mysql

import com.twitter.finagle.Mysql
import com.twitter.finagle.filter.NackAdmissionFilter
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.util.Time
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FunSuite, MustMatchers}

/**
 * Tests the functionality of the MySQL client.
 */
class ClientTest extends FunSuite with MockitoSugar with MustMatchers {
  private val sqlQuery = "SELECT * FROM FOO"

  test("client stack excludes NackAdmissionFilter") {
    val client = Mysql.client
    val stack = client.stack
    assert(!stack.contains(NackAdmissionFilter.role))
  }

  test("basic test creates a new service for each query") {
    val service = new MockService()
    val factory = spy(new MockServiceFactory(service))
    val client = spy(Client(factory, NullStatsReceiver, supportUnsigned = false))

    client.query(sqlQuery)
    client.query(sqlQuery)

    service.requests must equal(
      List(
        sqlQuery,
        sqlQuery
      ).map(QueryRequest(_))
    )

    verify(client, times(2)).query(sqlQuery)
    verify(factory, times(2)).apply()
    verify(factory, times(0)).close(any[Time])
  }
}
