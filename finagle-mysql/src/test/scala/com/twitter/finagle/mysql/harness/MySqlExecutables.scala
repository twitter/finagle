package com.twitter.finagle.mysql.harness

import com.twitter.finagle.mysql.harness.MySqlExecutables.mysqldPath
import com.twitter.logging.Logger
import com.twitter.util.Try
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import scala.sys.process.BasicIO
import scala.sys.process.Process

sealed trait MySqlExecutable {
  val absolutePath: String
}
case class Mysqld(absolutePath: String) extends MySqlExecutable
case class MysqlAdmin(absolutePath: String) extends MySqlExecutable

object MySqlExecutables {

  private val mysqldPath: String = "/bin/mysqld"
  private val mysqlAdminPath: String = "/bin/mysqladmin"

  /**
   * Traverse the provided path to find [[mysqldPath]] and [[mysqlAdminPath]]
   * @param root The start path to traverse from
   * @return The option is defined if the executables are found
   */
  def fromPath(root: Path): Option[MySqlExecutables] = {
    fromExtractedFiles(traverse(root.toFile))
  }

  private def traverse(root: File): Seq[Path] = {
    val files = Option(root.listFiles()).getOrElse(Array.empty)
    files
      .filter(_.isFile)
      .map(_.toPath).toSeq ++ files.filter(_.isDirectory).flatMap(traverse).toSeq
  }

  /**
   * Search for [[mysqldPath]] and [[mysqlAdminPath]] in the provided files
   * @param extractedFiles The files to search for
   * @return The option is defined if the executables are found
   */
  def fromExtractedFiles(extractedFiles: Seq[Path]): Option[MySqlExecutables] = {
    (
      extractedFiles.find(_.toString.endsWith(mysqldPath)),
      extractedFiles.find(_.toString.endsWith(mysqlAdminPath))
    ) match {
      case (Some(mysqld), Some(mysqlAdmin)) =>
        mysqld.toFile.setExecutable(true)
        mysqlAdmin.toFile.setExecutable(true)
        Some(
          new MySqlExecutables(
            Mysqld(mysqld.toString),
            MysqlAdmin(mysqlAdmin.toString)
          ))
      case (_, _) => None
    }
  }
}

case class MySqlExecutables(mysqld: Mysqld, mysqlAdmin: MysqlAdmin) {
  private val log: Logger = Logger.get()

  override def toString: String = {
    s"""
       |mysqld: ${mysqld.absolutePath}
       |mysqlAdmin: ${mysqlAdmin.absolutePath}""".stripMargin
  }

  /**
   * Compute the base dir from the mysql distribution. The home directory of /bin/mysqld
   * @return the base dir of the mysql distribution
   */
  def getBaseDir: Path = {
    Paths.get(mysqld.absolutePath.replace(mysqldPath, ""))
  }

  /**
   * Call mysqld to init the MySql --datadir
   * @param arguments the init arguments
   */
  def initDatabase(arguments: Seq[String]): Unit = {
    log.debug(s"Executing: ${mysqld.absolutePath} ${arguments.mkString(" ")}")
    Try {
      Process(
        mysqld.absolutePath,
        arguments
      ).run(BasicIO.standard(false)).exitValue()
    }.map(errorCode).get()
  }

  /**
   * Run mysqd to start the MySql instance
   * @param arguments the parameters to start the instance with
   * @return The scala Process
   */
  def startDatabase(arguments: Seq[String]): Process = {
    log.debug(s"Executing: ${mysqld.absolutePath} ${arguments.mkString(" ")}")
    Process(mysqld.absolutePath, arguments).run(BasicIO.standard(false))
  }

  /**
   * Run mysqladmin to stop the MySql instance
   * @param port the port of the MySql instance
   */
  def stopDatabase(port: Int): Unit = {
    val arguments = Seq(
      "--protocol=tcp",
      "-uroot",
      s"--port=$port",
      "shutdown"
    )
    log.debug(s"Executing: ${mysqlAdmin.absolutePath} ${arguments.mkString(" ")}")
    Try {
      Process(mysqlAdmin.absolutePath, arguments)
        .run(BasicIO.standard(false)).exitValue()
    }.map(errorCode).get()
  }

  private def errorCode: Int => Unit = {
    case 0 => ()
    case code => throw new RuntimeException(s"Exited with exit value $code")
  }
}
