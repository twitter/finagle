package com.twitter.finagle.mysql.harness.config

/**
 * Version definition for a MySqlInstance.
 *
 * @param majorVersion The major MySql version
 * @param minorVersion The minor MySql version
 * @param patchVersion The patch MySql version
 * @param serverStartParameters The default server start parameters for this version of mysql.
 * These values can be added to or replaced at the EmbeddedInstance level through its config.
 */
final case class MySqlVersion(
  majorVersion: Int,
  minorVersion: Int,
  patchVersion: Int,
  serverStartParameters: Map[String, String]) {
  override def toString: String = {
    s"$majorVersion.$minorVersion.$patchVersion"
  }
}
