package com.twitter.finagle.mysql.harness

import com.twitter.finagle.Mysql
import com.twitter.finagle.mysql.{Client, Transactions}
import com.twitter.finagle.mysql.harness.config.{DatabaseConfig, InstanceConfig, User}
import com.twitter.util.{Await, Duration, Future}
import org.scalatest.exceptions.TestCanceledException
import org.scalatest.funsuite.FixtureAnyFunSuite
import org.scalatest.{BeforeAndAfterAll, Canceled, Outcome}

object EmbeddedSuite {

  /**
   * Test fixture passed in to tests in an [[EmbeddedSuite]].
   *
   * @param instance an instance of MySql.
   * @param database a database hosted in that instance
   */
  final case class Fixture(
    instance: EmbeddedInstance,
    database: EmbeddedDatabase) {

    /**
     * Creates a Mysql.Client from `base` configured with
     * the `user` credentials.
     */
    def newClient(
      base: Mysql.Client = Mysql.client,
      user: User = User.Root
    ): Mysql.Client =
      base
        .withDatabase(database.config.databaseName)
        .withCredentials(user.name, user.password.orNull)

    /**
     * Creates a mysql.Client from `base` configured with
     * the `user` credentials.
     */
    def newRichClient(
      base: Mysql.Client = Mysql.client,
      user: User = User.Root
    ): Client with Transactions = {
      newClient(base, user).newRichClient(instance.dest)
    }
  }
}

/**
 * [[EmbeddedSuite]] is a testing harness for finagle-mysql integration testing
 * that allows a mysql server instance to be bootstrapped programmatically via
 * the provided config. The only necessary environment setup is the path to the
 * relevant mysql executables.
 */
trait EmbeddedSuite extends FixtureAnyFunSuite with BeforeAndAfterAll {
  import EmbeddedSuite._

  // an await with ample grace for embedded tests to run
  protected def await[A](f: Future[A]): A = Await.result(f, Duration.fromSeconds(30))

  /**
   * Config object describing the MySql instance to run. Must point to an extracted distribution of
   * MySql
   */
  def instanceConfig: InstanceConfig

  /**
   * Config object describing the database table and users to use for this test.
   */
  def databaseConfig: DatabaseConfig

  private[this] var fixture: Option[Fixture] = None

  override protected def beforeAll(): Unit = {
    EmbeddedInstance.getInstance(instanceConfig) match {
      case None => // do nothing.
      case Some(instance) =>
        val db = EmbeddedDatabase.getInstance(databaseConfig, instance)
        // Initialize db. Note, this will only once per `db` instance even
        // if it's called multiple times from different suites.
        db.init()
        fixture = Some(Fixture(instance, db))
    }
  }

  type FixtureParam = Fixture
  override protected def withFixture(test: OneArgTest): Outcome = fixture match {
    case Some(fixture) => withFixture(test.toNoArgTest(fixture))
    case None => new Canceled(new TestCanceledException("no available embedded fixture", 1))
  }
}
