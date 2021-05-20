package com.twitter.finagle.mysql.integration

import com.twitter.finagle.Mysql
import com.twitter.finagle.mysql._
import java.io.{File, FileInputStream}
import java.net.{BindException, InetAddress, ServerSocket}
import java.util.logging.{Level, Logger}
import java.util.Properties
import org.scalactic.source.Position
import org.scalatest.Tag
import scala.util.control.NonFatal
import org.scalatest.funsuite.AnyFunSuiteLike

trait IntegrationClient extends AnyFunSuiteLike {
  protected val maxConcurrentPreparedStatements = 10
  private val logger = Logger.getLogger("integration-client")

  val p: Properties = new Properties
  val propFile: File = new File(
    System.getProperty("user.home") +
      "/.finagle-mysql/integration-test.properties"
  )

  val propFileExists: Boolean =
    try {
      val fis = new FileInputStream(propFile)
      p.load(fis)
      fis.close()
      true
    } catch {
      case NonFatal(_) =>
        logger.log(Level.WARNING, "Error loading integration.properties, skipping integration test")
        false
    }

  // Check if default mysql port is available.
  val isPortAvailable: Boolean =
    try {
      val address = InetAddress.getByName("localhost")
      // 50 is the connection backlog, meaningless here
      // but this is the only constructor overload that
      // allows us to specify an address and a port.
      val socket = new ServerSocket(port, 50, address)
      socket.close()
      logger.log(
        Level.WARNING,
        s"Error mysql is not running on port $dest, skipping integration test")
      true
    } catch {
      case _: BindException => false
    }

  // It's likely that we can run this test
  // if a mysql instance is running and a valid
  // properties file is found which contains
  // mysql credentials.
  val isAvailable: Boolean = !isPortAvailable && propFileExists

  protected def configureClient(username: String, password: String, db: String): Mysql.Client =
    Mysql.client
      .withCredentials(username, password)
      .withMaxConcurrentPrepareStatements(maxConcurrentPreparedStatements)
      .withDatabase(db)

  protected def configureClient(): Mysql.Client = {
    val username = p.getProperty("username", "<user>")
    val password = p.getProperty("password", null)
    val db = p.getProperty("db", "test")
    configureClient(username, password, db)
  }

  protected def dest: String = s"localhost:$port"

  protected def port: Int = p.getProperty("port", "3306").toInt

  val client: Option[Client with Transactions] = if (isAvailable) {
    logger.log(Level.INFO, s"Attempting to connect to mysqld @ $dest")
    Some(configureClient().newRichClient(dest))
  } else {
    None
  }

  override def test(testName: String, testTags: Tag*)(f: => Any)(implicit pos: Position): Unit = {
    if (isAvailable) {
      super.test(testName, testTags: _*)(f)(pos)
    } else {
      ignore(testName)(f)(pos)
    }
  }

}
