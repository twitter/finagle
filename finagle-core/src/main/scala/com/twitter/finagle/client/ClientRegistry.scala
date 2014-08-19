package com.twitter.finagle.client

import com.twitter.finagle.{Addr, param, Stack}
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.util.{Future, FuturePool, Promise}

import scala.collection.mutable

import java.util.logging.Level

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

  // added for tests
  private[finagle] def clear() {
    synchronized { clients.clear() }
  }

  /**
   * Get a Future which is satisfied when the dest of every currently
   * registered client is no longer pending.
   * @note The destination of clients may later change to pending.
   * @note Clients are only registered after creation (i.e. calling `newClient` or
   *       `build` with ClientBuilder APIs).
   * @note Experimental feature which will eventually be solved by exposing Service
   *       availability as a Var.
   */
  def expAllRegisteredClientsResolved(): Future[Unit] = synchronized {
    val fs = clients map { case (name, stackClient) =>
      val p = new Promise[Unit]
      val LoadBalancerFactory.Dest(va) = stackClient.params[LoadBalancerFactory.Dest]
      val param.Logger(log) = stackClient.params[param.Logger]

      val resolved = va.changes.filter(_ != Addr.Pending).toFuture
      resolved onSuccess { resolution =>
        log.log(Level.INFO, "ClientRegistery: %s resolved to %s".format(name, resolution))
      }
    }

    Future.join(fs.toSeq)
  }

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
