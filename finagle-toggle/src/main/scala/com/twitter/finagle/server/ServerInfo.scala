package com.twitter.finagle.server

import com.twitter.app.GlobalFlag
import com.twitter.finagle.toggle.WriteOnce
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

}

object ServerInfo {

  /**
   * A [[ServerInfo]] with nothing defined.
   */
  val Empty: ServerInfo = new ServerInfo {
    override def toString: String = "ServerInfo.Empty"
    def environment: Option[String] = None
  }

  /**
   * A [[ServerInfo]] that gets its value from the [[environment]] `GlobalFlag`.
   */
  val Flag: ServerInfo = new ServerInfo {
    override def toString: String = s"ServerInfo.Flag($environment)"
    def environment: Option[String] =
      com.twitter.finagle.server.environment.get
  }

  private[this] def registerServerInfo(serverInfo: ServerInfo): Unit =
    GlobalRegistry.get.put("library", "server_info", serverInfo.toString)

  private[this] val global = new WriteOnce[ServerInfo](Empty)
  registerServerInfo(global())

  /**
   * Initialize the global [[ServerInfo]] returned by [[apply()]].
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

/**
 * @see [[ServerInfo.Flag]]
 */
object environment extends GlobalFlag[String](
    "The environment the server is running in.")
