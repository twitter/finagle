package com.twitter.finagle.mysql

import com.twitter.finagle.filter.NackAdmissionFilter
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.ChannelClosedException
import com.twitter.finagle.Mysql
import com.twitter.util.Time
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.scalatestplus.mockito.MockitoSugar

/**
 * Tests the functionality of the MySQL client.
 */
class ClientTest extends AnyFunSuite with MockitoSugar with Matchers {
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

  test("WrappedChannelClosedException.unapply returns None for non-CCE nested exceptions") {
    val exceptionInner = new Exception("inner")
    val exceptionMiddle = new Exception("middle", exceptionInner)
    val exceptionOuter = new Exception("outer", exceptionMiddle)

    exceptionOuter match {
      case e @ Client.WrappedChannelClosedException() =>
        fail("exception did not contain CCE but was unwrapped as WrappedChannelClosedException")
      case _ =>
    }
  }

  test("WrappedChannelClosedException.unapply returns Some(exc) for CCEs") {
    val exception = new ChannelClosedException()

    exception match {
      case e @ Client.WrappedChannelClosedException() =>
      case _ => fail("exception DID contain CCE but was not unwrapped")
    }
  }

  test(
    "WrappedChannelClosedException.unapply returns Some(outer exception) for CCEs nested one " +
      "level down"
  ) {
    val exceptionInner = new Exception("inner")
    val exceptionMiddle = new ChannelClosedException(exceptionInner, null)
    val exceptionOuter = new Exception("outer", exceptionMiddle)

    exceptionOuter match {
      case e @ Client.WrappedChannelClosedException() =>
      case _ => fail("exception DID contain CCE but was not unwrapped")
    }
  }

  test(
    "WrappedChannelClosedException.unapply returns Some(outer exception) for CCEs nested two " +
      "levels down"
  ) {
    val exceptionInner = new ChannelClosedException()
    val exceptionMiddle = new Exception("middle", exceptionInner)
    val exceptionOuter = new Exception("outer", exceptionMiddle)

    exceptionOuter match {
      case e @ Client.WrappedChannelClosedException() =>
      case _ => fail("exception DID contain CCE but was not unwrapped")
    }
  }
}
