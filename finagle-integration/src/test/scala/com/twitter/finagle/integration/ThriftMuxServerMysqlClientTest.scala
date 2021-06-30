package com.twitter.finagle.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.mysql.harness.EmbeddedSuite
import com.twitter.finagle.mysql.harness.config.{DatabaseConfig, InstanceConfig, MySqlVersion}
import com.twitter.finagle.thriftmux.thriftscala.TestService
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Future, Promise}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}

import java.net.{InetAddress, InetSocketAddress}

class ThriftMuxServerMysqlClientTest
    extends EmbeddedSuite
    with Eventually
    with IntegrationPatience {

  // This configuration is a direct copy of what we use
  // in the finagle-mysql integration tests. We should
  // consolidate them in the future.
  val v5_7_28 = MySqlVersion(
    5,
    7,
    28,
    Map(
      "--innodb_use_native_aio" -> "0",
      "--innodb_stats_persistent" -> "0",
      "--innodb_strict_mode" -> "1",
      "--explicit_defaults_for_timestamp" -> "0",
      "--sql_mode" -> "NO_ENGINE_SUBSTITUTION",
      "--character_set_server" -> "utf8",
      "--default_time_zone" -> "+0:00",
      "--innodb_file_format" -> "Barracuda",
      "--enforce_gtid_consistency" -> "ON",
      "--log-bin" -> "binfile",
      "--server-id" -> "1"
    )
  )

  val createTableQuery: String =
    """CREATE TABLE IF NOT EXISTS txn_test (
      |  id INT(5)
      |) ENGINE=InnoDB
      """.stripMargin

  val instanceConfig: InstanceConfig = InstanceConfig(v5_7_28)
  val databaseConfig: DatabaseConfig =
    DatabaseConfig(
      databaseName = "a_database",
      users = Seq.empty,
      setupQueries = Seq(createTableQuery))

  test("interruption during transaction") { fixture =>
    val mysqlClient = fixture.newRichClient()

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
