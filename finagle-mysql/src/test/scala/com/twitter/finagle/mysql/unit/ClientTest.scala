package com.twitter.finagle.mysql

import com.twitter.finagle.mysql.transport.TransportImpl
import com.twitter.finagle.Mysql
import com.twitter.util.Time
import java.net.SocketAddress
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{MustMatchers, FunSuite}

/**
 * Tests the functionality of the MySQL client.
 */
@RunWith(classOf[JUnitRunner])
class ClientTest extends FunSuite with MockitoSugar with MustMatchers {
  private val sqlQuery = "SELECT * FROM FOO"

  test("basic test creates a new service for each query") {
    val service = new MockService()
    val factory = spy(new MockServiceFactory(service))
    val client = spy(Client(factory))

    client.query(sqlQuery)
    client.query(sqlQuery)

    service.requests must equal (List(
     sqlQuery, sqlQuery
    ).map(QueryRequest(_)))

    verify(client, times(2)).query(sqlQuery)
    verify(factory, times(2)).apply()
    verify(factory, times(0)).close(any[Time])
  }

  test("Client uses Netty3 by default, but can be toggled to netty4") {
    val params = Mysql.client.params
    val addr = new SocketAddress { }

    assert(params[TransportImpl].transporter(params)(addr).toString.equals("Netty3Transporter"))
  }
}
