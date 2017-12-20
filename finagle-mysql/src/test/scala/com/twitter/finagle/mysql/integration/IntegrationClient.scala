package com.twitter.finagle.mysql.integration

import com.twitter.finagle.Mysql
import com.twitter.finagle.mysql._
import java.io.{File, FileInputStream}
import java.net.{BindException, ServerSocket}
import java.util.logging.{Level, Logger}
import java.util.Properties
import org.scalactic.source.Position
import org.scalatest.{FunSuiteLike, Tag}
import scala.util.control.NonFatal

trait IntegrationClient extends FunSuiteLike {
  private val logger = Logger.getLogger("integration-client")

  // Check if default mysql port is available.
  val isPortAvailable: Boolean = try {
    val socket = new ServerSocket(3306)
    socket.close()
    true
  } catch {
    case e: BindException => false
  }

  val propFile: File = new File(
    System.getProperty("user.home") +
      "/.finagle-mysql/integration-test.properties"
  )

  val p: Properties = new Properties
  val propFileExists: Boolean = try {
    val fis = new FileInputStream(propFile)
    p.load(fis)
    fis.close()
    true
  } catch {
    case NonFatal(e) =>
      logger.log(Level.WARNING, "Error loading integration.properties, skipping integration test")
      false
  }

  // It's likely that we can run this test
  // if a mysql instance is running and a valid
  // properties file is found which contains
  // mysql credentials.
  val isAvailable: Boolean = !isPortAvailable && propFileExists

  protected def configureClient(username: String, password: String, db: String): Mysql.Client =
    Mysql.client
      .withCredentials(username, password)
      .withDatabase(db)

  protected def configureClient(): Mysql.Client = {
    val username = p.getProperty("username", "<user>")
    val password = p.getProperty("password", null)
    val db = p.getProperty("db", "test")
    configureClient(username, password, db)
  }

  protected def dest: String = "localhost:3306"

  val client: Option[Client with Transactions] = if (isAvailable) {
    logger.log(Level.INFO, "Attempting to connect to mysqld @ localhost:3306")
    Some(configureClient().newRichClient(dest))
  } else {
    None
  }

  override def test(
    testName: String,
    testTags: Tag*
  )(
    f: => Any
  )(
    implicit pos: Position
  ): Unit = {
    if (isAvailable) {
      super.test(testName, testTags: _*)(f)(pos)
    } else {
      ignore(testName)(f)(pos)
    }
  }

}
