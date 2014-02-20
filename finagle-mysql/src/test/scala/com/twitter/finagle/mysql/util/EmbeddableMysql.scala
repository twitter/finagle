package com.twitter.finagle.mysql.util

import com.mysql.management.{MysqldResource, MysqldResourceI}
import com.twitter.finagle.exp.Mysql
import com.twitter.finagle.exp.mysql.Client
import com.twitter.util.RandomSocket
import java.io.File
import java.util.UUID
import java.util.logging.Logger
import scala.collection.JavaConversions._
import com.twitter.logging.Level

/**
 * Creates an embeddable instance of MySQL.  By default, the database will be created in a
 * sub-directory of the temporary directory as defined by the Java system property "java.io.tmpdir".
 *
 * An example usage of this class is given below:
 *
 * object Main {
 *   def main(args: Array[String]) {
 *     val db = new EmbeddableMySql(user = "testuser", password = "password", db = "test")
 *     db.start()
 *      try {
 *        // do some work
 *      } finally {
 *        db.shutdown()
 *      }
 *    }
 *  }
 *
 * @param user the database user
 * @param password the database user password
 * @param db the database name
 * @param dbDir the directory to store database files
 * @param dbPort the database port
 */
class EmbeddableMysql(
    user: String,
    password: String,
    db: String,
    dbDir: Option[File] = None,
    dbPort: Option[Int] = None) {

  private[this] val log = Logger.getLogger("finagle-mysql")
  private[this] val mysqldRsrc = new MysqldResource(dbDir.getOrElse(createTempDirectory()))

  def start() {
    val localPort = dbPort.getOrElse(RandomSocket.nextPort())
    val dbOptions = Map[String, String](
      MysqldResourceI.PORT -> Integer.toString(localPort),
      MysqldResourceI.INITIALIZE_USER -> "true",
      MysqldResourceI.INITIALIZE_USER_NAME -> user,
      MysqldResourceI.INITIALIZE_PASSWORD -> password
    )

    mysqldRsrc.start("embeddable-mysqld", dbOptions)

    if (!mysqldRsrc.isRunning) throw new RuntimeException("MySql did not start.")
    log.info("MySql is running on port %d.\n\tBase directory: %s".format(port, baseDir))
  }

  def port = mysqldRsrc.getPort

  def baseDir = mysqldRsrc.getBaseDir

  def dataDir = mysqldRsrc.getDataDir

  def shutdown() {
    log.info("MySql is shutting down...")
    try {
      mysqldRsrc.shutdown()
    } catch {
      case t: Throwable =>
        log.log(Level.WARNING, "while shutting down", t)
    }
    log.info("MySql shutdown")
  }

  def createClient(): Client = {
    Mysql
        .withDatabase(db)
        .withCredentials(user, password)
        .newRichClient("localhost:%d".format(port))
  }

  private def createTempDirectory(): File = {
    val tempDir = new File(System.getProperty("java.io.tmpdir") + "/finagle-mysql/" + UUID.randomUUID.toString)
    if(!tempDir.mkdirs()) {
      throw new IllegalStateException("Failed to create temporary directory: %s".format(tempDir.getAbsolutePath))
    }
    tempDir
  }
}
