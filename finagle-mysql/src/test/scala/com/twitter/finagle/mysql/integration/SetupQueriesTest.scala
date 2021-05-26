package com.twitter.finagle.mysql.integration

import com.twitter.finagle.mysql.harness.EmbeddedSuite
import com.twitter.finagle.mysql.harness.config.{DatabaseConfig, InstanceConfig}
import java.lang

class SetupQueriesTest extends EmbeddedSuite {
  val instanceConfig: InstanceConfig = defaultInstanceConfig
  val databaseConfig: DatabaseConfig = defaultDatabaseConfig.copy(
    databaseName = "setupQueriesTest",
    setupQueries = Seq(
      "CREATE TABLE for_testing(data Int);",
      "INSERT INTO for_testing VALUE(100);"
    )
  )

  test("execute setup queries") { fixture =>
    val clnt = fixture.newRichClient()
    val result: Seq[Option[lang.Long]] =
      await(clnt.select("SELECT * FROM for_testing")(row => row.getLong("data")))
    assert(result.forall(_.contains(100L)))
    await(clnt.ping())
    await(clnt.close())
  }

}
