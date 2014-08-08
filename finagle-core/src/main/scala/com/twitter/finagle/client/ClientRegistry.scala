package com.twitter.finagle.client

import scala.collection.mutable


/**
 * Maintains information about clients that register.
 * Call ClientRegistry.register(clientLabel, client) to register a client
 */
private[twitter] case class ClientModuleInfo(
  role: String, 
  description: String, 
  perModuleParams: Map[String, String]
)
 
private[twitter] case class ClientInfo(name: String, modules: List[ClientModuleInfo])

private[twitter] object ClientRegistry {

  private[this] val clients = mutable.Map.empty[String, StackClient[_, _]]

  private[finagle] def register(name: String, client: StackClient[_, _]): Unit = 
    synchronized { clients += name -> client }

  /**
   * Get a list of all registered clients
   */
  def clientList(): List[String] = synchronized { 
    clients.keySet.toList 
  }

  /**
   * Get information about a registered client with a given name
   */
  def clientInfo(name: String): Option[ClientInfo] = synchronized {
    clients.get(name).map({ client =>
      val modules = client.stack.tails.toList map { n => 
        ClientModuleInfo(n.head.role.name, n.head.description.toString, n.head.params.toMap)
      }
      ClientInfo(name, modules)
    })
  }
}