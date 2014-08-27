package com.twitter.finagle.client

import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.{Addr, Name, param, Stack}
import com.twitter.util.{Future, FuturePool, Promise}
import java.util.logging.Level
import scala.collection.mutable

private[twitter] case class ClientModuleInfo(
  role: String,
  description: String,
  perModuleParams: Map[String, String]
)

/**
 * Contains information about a client
 * name: client name
 * dest: addrs of the client
 */
private[twitter] case class ClientInfo(name: String, dest: Seq[String], modules: Seq[ClientModuleInfo])

/**
 * Maintains information about clients that register.
 * Call ClientRegistry.register(clientLabel, client) to register a client
 */
private[twitter] object ClientRegistry {

  private[this] val clients = mutable.Map.empty[String, (Name, StackClient[_, _])]

  private[finagle] def register(name: String, dest: Name, client: StackClient[_, _]): Unit =
    synchronized { clients += name -> (dest, client) }

  private[this] def parseDest(dest: Name): Seq[String] = (dest match {
    case Name.Bound(addr) => addr.sample match {
      case Addr.Bound(addrs) => addrs.toSeq
      case addr => Seq(addr)
    }
    case Name.Path(addr) => Seq(addr)
  }).map(_.toString)

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
  def expAllRegisteredClientsResolved(): Future[Set[String]] = synchronized {
    val fs = clients map { case (name, (_, stackClient)) =>
      val LoadBalancerFactory.Dest(va) = stackClient.params[LoadBalancerFactory.Dest]
      val param.Logger(log) = stackClient.params[param.Logger]

      val resolved = va.changes.filter(_ != Addr.Pending).toFuture
      resolved map { resolution =>
        log.log(Level.INFO, "ClientRegistery: %s resolved to %s".format(name, resolution))
        name
      }
    }

    Future.collect(fs.toSeq).map(_.toSet)
  }

  /**
   * Get a list of all registered clients. No module information is included.
   */
  def clientList(): Seq[ClientInfo] = synchronized {
    clients map { case (name, (dest, _)) => ClientInfo(name, parseDest(dest), Seq.empty) } toSeq
  }

  /**
   * Get information about a registered client with a given name
   */
  def clientInfo(name: String): Option[ClientInfo] = synchronized {
    clients.get(name) map { case(dest, client) =>
      val modules = client.stack.tails.toList map { n =>
        ClientModuleInfo(n.head.role.name, n.head.description.toString, n.head.params.toMap)
      }
      ClientInfo(name, parseDest(dest), modules)
    }
  }
}
