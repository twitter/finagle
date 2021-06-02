package com.twitter.finagle.mysql.harness

import com.twitter.finagle.mysql.harness.EmbeddedSuite.Fixture
import com.twitter.finagle.mysql.harness.config.{DatabaseConfig, InstanceConfig}
import com.twitter.util.{Await, Duration, Future}
import org.scalatest.exceptions.TestCanceledException
import org.scalatest.{Canceled, Outcome}
import org.scalatest.funsuite.AnyFunSuite
import scala.util.control.NonFatal

/**
 * A simple trait that only exposes the embedded MySQL fixture so a test can
 * create a client during test suite initialization. Common use cases include:
 * - running queries on the database, the result of which is used in each test
 * - and reducing code duplication while running different queries before and after each test
 */
trait EmbeddedSimpleSuite extends AnyFunSuite {
  // an await with ample grace for embedded tests to run
  protected def await[A](f: Future[A]): A = Await.result(f, Duration.fromSeconds(30))

  /**
   * Config object describing the MySql instance to run. Must point to an extracted distribution of
   * MySQL.
   */
  def instanceConfig: InstanceConfig

  /**
   * Config object describing the database table and users to use for this test.
   */
  def databaseConfig: DatabaseConfig

  /**
   * The fixture that allows a caller to create a client to connect to the desired database config.
   */
  val fixture: Option[Fixture] =
    try {
      EmbeddedInstance.getInstance(instanceConfig) match {
        case None => None
        case Some(instance) =>
          val db = EmbeddedDatabase.getInstance(databaseConfig, instance)
          // Initialize db. Note, this will only once per `db` instance
          db.init()
          Some(Fixture(instance, db))
      }
    } catch {
      case NonFatal(_) => None
    }

  override def withFixture(noArgTest: NoArgTest): Outcome = fixture match {
    case Some(_) => super.withFixture(noArgTest)
    case None => new Canceled(new TestCanceledException("no available embedded fixture", 1))
  }
}
