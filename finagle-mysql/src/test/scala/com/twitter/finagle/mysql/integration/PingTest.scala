package com.twitter.finagle.mysql.integration

import com.twitter.finagle.mysql.harness.EmbeddedSuite
import com.twitter.finagle.mysql.harness.config.{DatabaseConfig, InstanceConfig}

class PingTest extends EmbeddedSuite {
  override val instanceConfig: InstanceConfig = defaultInstanceConfig
  override val databaseConfig: DatabaseConfig = defaultDatabaseConfig

  test("ping default") { fixture =>
    val clnt = fixture.newRichClient()
    await(clnt.ping())
    await(clnt.close())
  }
}
