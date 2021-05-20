package com.twitter.finagle.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.mysql.OK
import com.twitter.finagle.mysql.integration.IntegrationClient
import com.twitter.finagle.thriftmux.thriftscala.TestService
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.{Address, Name, RequestTimeoutException, ThriftMux, param}
import com.twitter.util.{Await, Future, Promise}
import java.net.{InetAddress, InetSocketAddress}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class ThriftMuxServerMysqlClientTest
    extends AnyFunSuite
    with IntegrationClient
    with Eventually
    with IntegrationPatience
    with BeforeAndAfter {

  private def await[T](f: Future[T]): T = Await.result(f, 5.seconds)

  before {
    val createTable =
      """CREATE TABLE IF NOT EXISTS txn_test (
        |  id INT(5)
        |) ENGINE=InnoDB
      """.stripMargin

    for (mysqlClient <- client) {
      await(mysqlClient.query(createTable)) match {
        case _: OK => // ok
        case t => fail()
      }
    }
  }

  after {
    val dropTable = "DROP TABLE txn_test"
    for (mysqlClient <- client) {
      await(mysqlClient.query(dropTable)) match {
        case _: OK => // ok
        case t => fail()
      }
    }
  }

  test("interruption during transaction") {
    for (mysqlClient <- client) {
      val timer = DefaultTimer

      val mysqlLatch = new Promise[Unit]()

      // a thrift service that uses a "slow" mysql client
      val thriftService = new TestService.MethodPerEndpoint {
        def query(x: String): Future[String] = {
          mysqlClient.transaction { mc =>
            mc.query("INSERT INTO txn_test (id) VALUES (1)").flatMap { _ =>
                // wait for the client's timeout
                // (can't use MockTimer/TimeControl as this happens on a diff "thread")
                mysqlLatch
              }.flatMap { _ =>
                // run another query that should fail, given the prior timeout.
                mc.query("SELECT id FROM txn_test").flatMap { _ => Future.value(x.toString) }
              }
          }
        }
        def question(y: String): Future[String] = ???
        def inquiry(z: String): Future[String] = ???
      }

      // start a thriftmux server and client to talk to it.
      val server = ThriftMux.server
        .withLabel("thriftmux_server")
        .configured(param.Timer(timer))
        .serveIface(
          new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
          thriftService
        )

      val client = ThriftMux.client
        .withLabel("mysql_db")
        .withRequestTimeout(20.milliseconds)
        .configured(param.Timer(timer))
        .build[TestService.MethodPerEndpoint](
          dest = Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
          label = "mysql_db"
        )

      // issue the thriftmux request and mysql query
      val result = client.query("hi")

      // wait until we trigger the client timeout
      intercept[RequestTimeoutException] {
        await(result)
      }

      // trigger the rest of the mysql client txn and rollback
      mysqlLatch.setDone()

      // verify that the transaction was rolled back by selecting
      // the number of rows in the table. which should be 0, as the
      // insert should get rolled back.
      val ids: Seq[Int] = await(
        mysqlClient.select("SELECT id FROM txn_test") { row => row.intOrZero("id") }
      )
      assert(Seq.empty == ids)
    }
  }

}
