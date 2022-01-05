package com.twitter.finagle.mysql.unit
package harness

import com.twitter.finagle.mysql.harness.EmbeddedDatabase
import com.twitter.finagle.mysql.harness.EmbeddedInstance
import com.twitter.finagle.mysql.harness.config.DatabaseConfig
import com.twitter.finagle.mysql.harness.config.User
import java.util.UUID
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

class MySqlDatabaseCacheTest
    extends AnyFunSuite
    with MockitoSugar
    with Matchers
    with BeforeAndAfterEach {
  private val embeddedInstance = new EmbeddedInstance(null, Seq.empty[String], 1234, "aServer")
  private val differentEmbeddedInstance =
    new EmbeddedInstance(null, Seq.empty[String], 1234, "aDifferentServer")

  private def uniqueName = UUID.randomUUID().toString

  test("no exception thrown if identical configs are used on an instance") {
    val databaseConfig1 =
      DatabaseConfig(
        uniqueName,
        Seq(User(uniqueName, Some("aPassword"), User.Permission.All)),
        Seq("aSetupQuery"))

    val databaseConfig2 = databaseConfig1.copy()
    noException shouldBe thrownBy(EmbeddedDatabase.getInstance(databaseConfig1, embeddedInstance))
    noException shouldBe thrownBy(EmbeddedDatabase.getInstance(databaseConfig2, embeddedInstance))
  }

  test(
    "exception thrown if user passwords don't match for the same users defined for a database name on an instance") {
    val user = User(uniqueName, Some("aPassword"), User.Permission.All)
    val databaseConfig1 =
      DatabaseConfig(uniqueName, Seq(user), Seq("aSetupQuery"))

    val databaseConfig2 =
      databaseConfig1.copy(users = Seq(user.copy(password = Some("aDifferentPassword"))))
    noException shouldBe thrownBy(EmbeddedDatabase.getInstance(databaseConfig1, embeddedInstance))
    intercept[Exception](EmbeddedDatabase.getInstance(databaseConfig2, embeddedInstance))
  }

  test(
    "exception thrown if user passwords don't match for the same users defined for different database name on an instance") {
    val user = User(uniqueName, Some("aPassword"), User.Permission.All)
    val databaseConfig1 =
      DatabaseConfig(uniqueName, Seq(user), Seq("aSetupQuery"))

    val databaseConfig2 =
      databaseConfig1.copy(
        databaseName = uniqueName,
        users = Seq(user.copy(password = Some("aDifferentPassword"))))
    noException shouldBe thrownBy(EmbeddedDatabase.getInstance(databaseConfig1, embeddedInstance))
    intercept[Exception](EmbeddedDatabase.getInstance(databaseConfig2, embeddedInstance))
  }

  test("exception thrown if setup queries don't match for the same database name on an instance") {
    val databaseConfig1 =
      DatabaseConfig(
        uniqueName,
        Seq(User(uniqueName, Some("aPassword"), User.Permission.All)),
        Seq("aSetupQuery"))

    val databaseConfig2 = databaseConfig1.copy(setupQueries = Seq("aDifferentSetupQuery"))
    noException shouldBe thrownBy(EmbeddedDatabase.getInstance(databaseConfig1, embeddedInstance))
    intercept[Exception](EmbeddedDatabase.getInstance(databaseConfig2, embeddedInstance))
  }

  test("exception not thrown identical users are used for different databases on an instance") {
    val databaseConfig1 =
      DatabaseConfig(
        uniqueName,
        Seq(User(uniqueName, Some("aPassword"), User.Permission.All)),
        Seq("aSetupQuery"))

    val databaseConfig2 = databaseConfig1.copy(databaseName = uniqueName)
    noException shouldBe thrownBy(EmbeddedDatabase.getInstance(databaseConfig1, embeddedInstance))
    noException shouldBe thrownBy(EmbeddedDatabase.getInstance(databaseConfig2, embeddedInstance))
  }

  test("exception not thrown when users with different passwords created for separate instances") {
    val user = User(uniqueName, Some("aPassword"), User.Permission.All)
    val databaseConfig1 =
      DatabaseConfig(uniqueName, Seq(user), Seq("aSetupQuery"))

    val databaseConfig2 =
      databaseConfig1.copy(users = Seq(user.copy(password = Some("aDifferentPassword"))))
    noException shouldBe thrownBy(EmbeddedDatabase.getInstance(databaseConfig1, embeddedInstance))
    noException shouldBe thrownBy(
      EmbeddedDatabase.getInstance(databaseConfig2, differentEmbeddedInstance))
  }

  test(
    "exception not thrown if setup queries don't match for the same database name on different instances") {
    val databaseConfig1 =
      DatabaseConfig(
        uniqueName,
        Seq(User(uniqueName, Some("aPassword"), User.Permission.All)),
        Seq("aSetupQuery"))

    val databaseConfig2 = databaseConfig1.copy(setupQueries = Seq("aDifferentSetupQuery"))
    noException shouldBe thrownBy(EmbeddedDatabase.getInstance(databaseConfig1, embeddedInstance))
    noException shouldBe thrownBy(
      EmbeddedDatabase.getInstance(databaseConfig2, differentEmbeddedInstance))
  }
}
