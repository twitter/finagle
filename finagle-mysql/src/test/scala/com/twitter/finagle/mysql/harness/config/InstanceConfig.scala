package com.twitter.finagle.mysql.harness.config

import java.nio.file.Path
import java.nio.file.Paths

object InstanceConfig {

  /**
   * Create a config for the given MySql version.
   *
   * @param mySqlVersion the version for this instance config
   * @return a constructed [[InstanceConfig]]
   */
  def apply(mySqlVersion: MySqlVersion): InstanceConfig = {
    val serverParameters: Seq[String] = getAndCheckServerParameters(mySqlVersion)

    val extractionPath =
      Paths
        .get(s"${System.getProperty("java.io.tmpdir")}")
        .resolve(".embedded_mysql")
        .resolve(mySqlVersion.toString)

    InstanceConfig(
      mySqlVersion,
      extractionPath,
      serverParameters
    )
  }

  /**
   * Create a config for the given MySql version.
   *
   * @param mySqlVersion the version for this instance config
   * @param extractionPath the path to an extracted MySql binary package.  This binary package
   *                       should be downloaded from <a href="https://dev.mysql.com/downloads/mysql/">https://dev.mysql.com/downloads/mysql/</a>
   *                       and extracted to this location.  The library will then traverse the
   *                       extracted directory tree for mysqld and mysqladmin executables
   * @return a constructed [[InstanceConfig]]
   */
  def apply(mySqlVersion: MySqlVersion, extractionPath: Path): InstanceConfig = {
    val serverParameters: Seq[String] = getAndCheckServerParameters(mySqlVersion)

    InstanceConfig(
      mySqlVersion,
      extractionPath,
      serverParameters
    )
  }

  /**
   * Format the start parameters from [[MySqlVersion]] by prepending with --.
   *
   * The following values are derived at runtime:
   * - An open port is found for --port
   * - A directory is created within the extraction directory for --datadir
   * - A temporary file is created for --socket
   *
   * Thus, we throw a [[RuntimeException]] if the parameters include port,
   * datadir, or socket.
   *
   * @param mySqlVersion the version to retrieve parameters from.

   * @return the formatted server parameters
   */
  private def getAndCheckServerParameters(mySqlVersion: MySqlVersion): Seq[String] = {
    val serverParameters: Seq[String] = formatKeys(
      mySqlVersion.serverStartParameters
    ).map {
      case (key, value) => s"$key=$value"
    }.toSeq

    if (serverParameters.exists(_.startsWith("--port"))) {
      throw new RuntimeException("--port is not allowed in startServerParameters")
    } else if (serverParameters.exists(_.startsWith("--datadir"))) {
      throw new RuntimeException("--datadir is not allowed in startServerParameters")
    } else if (serverParameters.exists(_.startsWith("--socket"))) {
      throw new RuntimeException("--socket is not allowed in startServerParameters")
    }
    serverParameters
  }

  private def formatKeys(parameters: Map[String, String]): Map[String, String] = {
    parameters.map {
      case (key, value) if key.startsWith("--") => key -> value
      case (key, value) => s"--$key" -> value
    }
  }
}

/**
 * The configuration for an embedded mysql instance.
 *
 * @param mySqlVersion The MySql version
 * @param extractedMySqlPath the path to an extracted MySql binary package.  This binary package
 *                           should be downloaded from <a href="https://dev.mysql.com/downloads/mysql/">https://dev.mysql.com/downloads/mysql/</a>
 *                           and extracted to this location. The library will then traverse the
 *                           extracted directory tree for mysqld and mysqladmin executables
 * @param startServerParameters The parameters to start this instance with
 */
final case class InstanceConfig(
  mySqlVersion: MySqlVersion,
  extractedMySqlPath: Path,
  startServerParameters: Seq[String])
