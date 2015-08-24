package com.twitter.finagle.exp.mysql.integration

import com.twitter.finagle.exp.Mysql
import com.twitter.finagle.exp.mysql._
import com.twitter.util.NonFatal
import java.io.{File, FileInputStream}
import java.net.{ServerSocket, BindException}
import java.util.logging.{Level, Logger}
import java.util.Properties

trait IntegrationClient {
  private val logger = Logger.getLogger("integration-client")

  // Check if default mysql port is available.
  val isPortAvailable = try {
    val socket = new ServerSocket(3306)
    socket.close()
    true
  } catch {
    case e: BindException => false
  }

  val propFile = new File(System.getProperty("user.home") +
    "/.finagle-mysql/integration-test.properties")

  val p = new Properties
  val propFileExists = try {
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
  val isAvailable = !isPortAvailable && propFileExists

  val client: Option[Client] = if (isAvailable) {
    logger.log(Level.INFO, "Attempting to connect to mysqld @ localhost:3306")
    val username = p.getProperty("username", "<user>")
    val password = p.getProperty("password", null)
    val db = p.getProperty("db", "test")
    Some(Mysql.client
      .withCredentials(username, password)
      .withDatabase(db)
      .newRichClient("localhost:3306"))
  } else {
    None
  }
}
