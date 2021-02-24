package com.twitter.finagle.mysql.integration

import com.twitter.finagle.mysql.harness.EmbeddedMySqlSuite
import com.twitter.finagle.mysql.harness.config._
import com.twitter.util.{Await, Duration}

class PingTest extends EmbeddedMySqlSuite {
  test("ping default") { fixture =>
    val roClient = fixture.mySqlDatabase
      .createROClient().newRichClient(fixture.mySqlInstance.dest)
    Await.result(roClient.ping(), Duration.fromSeconds(1))
  }

  val mySqlInstanceConfig: MySqlInstanceConfig = defaultInstanceConfig

  val mySqlDatabaseConfig: MySqlDatabaseConfig = defaultDatabaseConfig
}
