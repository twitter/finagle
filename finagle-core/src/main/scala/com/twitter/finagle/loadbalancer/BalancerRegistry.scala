package com.twitter.finagle.loadbalancer

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

/**
 * A registry of [[Balancer load balancers]] currently in use.
 *
 * This class is thread-safe.
 *
 * @see [[BalancerRegistry$.get()]]
 * @see TwitterServer's "/admin/balancers.json" admin endpoint.
 */
final class BalancerRegistry private[loadbalancer] {

  private[this] val balancers =
    new ConcurrentHashMap[Balancer[_, _], Metadata]()

  private[loadbalancer] def register(label: String, balancer: Balancer[_, _]): Unit =
    balancers.put(balancer, new Metadata(label, balancer))

  private[loadbalancer] def unregister(balancer: Balancer[_, _]): Unit =
    balancers.remove(balancer)

  def allMetadata: Seq[Metadata] =
    balancers.values.asScala.toSeq

}

object BalancerRegistry {

  private[this] val instance: BalancerRegistry =
    new BalancerRegistry()

  /**
   * The global registry used by Finagle.
   */
  def get: BalancerRegistry = instance

}
