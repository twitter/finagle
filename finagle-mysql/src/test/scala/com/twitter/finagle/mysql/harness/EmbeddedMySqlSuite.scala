package com.twitter.finagle.mysql.harness

import com.twitter.finagle.mysql.harness.EmbeddedMySqlSuite.MySqlFixture
import com.twitter.finagle.mysql.harness.config.{MySqlDatabaseConfig, MySqlInstanceConfig}
import org.scalatest.exceptions.TestCanceledException
import org.scalatest.funsuite.FixtureAnyFunSuite
import org.scalatest.{BeforeAndAfterAll, Canceled, Outcome}

object EmbeddedMySqlSuite {

  /**
   * Test fixture
   * @param mySqlInstance an instance of MySql.
   * @param mySqlDatabase a database hosted in that instance
   */
  final case class MySqlFixture(
    mySqlInstance: EmbeddedMySqlInstance,
    mySqlDatabase: EmbeddedMySqlDatabase)
}

trait EmbeddedMySqlSuite extends FixtureAnyFunSuite with BeforeAndAfterAll {
  // By default, the database and its users will be dropped after all test cases.
  // For large suites with complex databases, this can be turned off to save on db creation and
  // seeding
  def dropDatabaseAfterAll: Boolean = true

  /**
   * Config object describing the MySql instance to run. Must point to an extracted distribution of
   * MySql
   */
  def mySqlInstanceConfig: MySqlInstanceConfig

  /**
   * Config object describing the database table and users to use for this test.
   */
  def mySqlDatabaseConfig: MySqlDatabaseConfig

  private[this] var mySqlFixture: Option[MySqlFixture] = _

  override protected def beforeAll(): Unit = {
    mySqlFixture = EmbeddedMySqlInstance.getInstance(mySqlInstanceConfig).map { mySqlInstance =>
      val mySqlDatabase: EmbeddedMySqlDatabase =
        EmbeddedMySqlDatabase.getInstance(mySqlInstance, mySqlDatabaseConfig, onDatabaseSetup)
      MySqlFixture(mySqlInstance, mySqlDatabase)
    }
  }

  override protected def afterAll(): Unit = {
    if (dropDatabaseAfterAll)
      mySqlFixture.foreach(fixture =>
        EmbeddedMySqlDatabase.dropDatabase(fixture.mySqlInstance, mySqlDatabaseConfig))
  }

  /**
   * Place all initial database DDL setup here. This function is called once the database and its
   * users have been created.
   */
  def onDatabaseSetup: EmbeddedMySqlDatabase => Unit = _ => {}

  type FixtureParam = MySqlFixture
  override protected def withFixture(
    test: OneArgTest
  ): Outcome = {
    mySqlFixture match {
      case Some(fixture) => withFixture(test.toNoArgTest(fixture))
      case None =>
        new Canceled(
          new TestCanceledException(
            s"Executables not found in ${mySqlInstanceConfig.extractedMySqlPath}",
            1))
    }
  }
}
