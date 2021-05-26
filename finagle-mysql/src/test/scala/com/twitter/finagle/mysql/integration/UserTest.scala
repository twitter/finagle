package com.twitter.finagle.mysql.integration

import com.twitter.finagle.mysql.ServerError
import com.twitter.finagle.mysql.harness.EmbeddedSuite
import com.twitter.finagle.mysql.harness.config.{DatabaseConfig, InstanceConfig, User}

class UserTest extends EmbeddedSuite {
  val aUser: User = User("aUser", Some("aPassword"), User.Permission.All)
  val noPwUser: User = User("noPwUser", None, User.Permission.Select)
  val nonDbUser: User = User("willFail", None, User.Permission.Select)

  val instanceConfig: InstanceConfig = defaultInstanceConfig
  val databaseConfig: DatabaseConfig = defaultDatabaseConfig.copy(
    databaseName = "usersTest",
    users = Seq(
      aUser,
      noPwUser
    )
  )

  test("create user") { fixture =>
    val aUserClient = fixture.newRichClient(user = aUser)
    val noPwUserClient = fixture.newRichClient(user = noPwUser)
    await(aUserClient.ping())
    await(aUserClient.close())
    await(noPwUserClient.ping())
    await(noPwUserClient.close())

    val nonUser = fixture.newRichClient(user = nonDbUser)
    val error = intercept[ServerError] {
      await(nonUser.ping())
    }
    assert(error.code == 1045)
  }
}
