package com.twitter.finagle.server

import com.twitter.util.NetUtil
import com.twitter.util.registry.GlobalRegistry

/**
 * Information about a server.
 */
abstract class ServerInfo {

  /**
   * A representation of the environment a server is running in.
   *
   * Commonly used values include: "production", "test", "development",
   * and "staging".
   *
   * @see [[com.twitter.finagle.toggle.StandardToggleMap]]
   */
  def environment: Option[String]

  /**
   * An identifier for this server.
   *
   * The implementation is generally specific to a user's operating environment.
   */
  def id: String

  /**
   * The instance id of the server, if available
   */
  def instanceId: Option[Long]

}

object ServerInfo {

  /**
   * A [[ServerInfo]] with nothing defined.
   */
  val Empty: ServerInfo = new ServerInfo {
    override def toString: String = "ServerInfo.Empty"
    def environment: Option[String] = None
    val id: String = NetUtil.getLocalHostName()
    val instanceId: Option[Long] = None
  }

  private[this] def registerServerInfo(serverInfo: ServerInfo): Unit =
    GlobalRegistry.get.put("library", "server_info", serverInfo.toString)

  private[this] val global = new WriteOnce[ServerInfo](Empty)
  registerServerInfo(global())

  /**
   * Initialize the global [[ServerInfo]] returned by [[ServerInfo$.apply]].
   *
   * May only be called once.
   */
  def initialize(serverInfo: ServerInfo): Unit = {
    global.write(serverInfo)
    registerServerInfo(serverInfo)
  }

  /**
   * Get the global [[ServerInfo]] if [[initialize initialized]],
   * or [[Empty]] if not yet initialized.
   */
  def apply(): ServerInfo =
    global()

}
