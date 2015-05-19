package com.twitter.finagle.loadbalancer

import com.twitter.finagle._
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.service.DelayedFactory
import com.twitter.finagle.stats._
import com.twitter.finagle.util.OnReady
import com.twitter.util.{Activity, Future}
import java.net.SocketAddress
import java.util.logging.Level
import scala.collection.mutable

/**
 * A load balancer module that operates over stacks and the
 * stack parameters, and creates a WeightedServiceFactory for
 * each resolved SocketAddress.
 */
private[loadbalancer] trait BalancerStackModule[Req, Rep]
  extends Stack.Module[ServiceFactory[Req, Rep]] {
  import com.twitter.finagle.loadbalancer.LoadBalancerFactory._

  /**
   * A tuple containing a [[com.twitter.finagle.ServiceFactory]] and its
   * associated weight.
   */
  type WeightedFactory[Req, Rep] = (ServiceFactory[Req, Rep], Double)

  /**
   * Allows implementations to process `addrs` before they are committed
   * to the load balancer's active set.
   *
   * @param params stack parameters which may contain configurations about
   *               how to process the addresses.
   * @param addrs input addresses
   * @return processed addresses
   */
  protected def processAddrs(
    params: Stack.Params,
    addrs: Set[SocketAddress]
  ): Set[SocketAddress] = addrs

  val role = LoadBalancerFactory.role

  /**
   * Update a mutable Map of `WeightedFactory`s according to a set of
   * active SocketAddresses and a factory construction function.
   *
   * `cachedFactories` are keyed by its unweighted address.
   * When an active address has a new weight compared with its
   * counterpart in the cache, the cached `WeightedFactory` is updated
   * with the new weight; and the `ServiceFactory` is reused.
   *
   * A new `ServiceFactory` is created only when an active address
   * does not have a unweighted counterpart in the cache. The newly
   * created `ServiceFactory` along with the weight of the `SocketAddress`
   * (if any) are cached. If no weight is provided from the `SocketAddress`,
   * a default weight is used in `WeightedFactory`.
   *
   * If `activeAddrs` contains duplicated host socket addresses, only one
   * such socket address and its weighted factory is cached. The weight in
   * the last socket address wins.
   *
   * When `probationEnabled` is true, `ServiceFactory` is only removed from
   * the active set when its status has changed to !Open. i.e. Removal may
   * not happen in the current update.
   */
  def updateFactories[Req, Rep](
    activeAddrs: Set[SocketAddress],
    cachedFactories: mutable.Map[SocketAddress, WeightedFactory[Req, Rep]],
    mkFactory: SocketAddress => ServiceFactory[Req, Rep],
    probationEnabled: Boolean
  ): Unit = cachedFactories.synchronized {
    val addrsWithWeight = activeAddrs.map(WeightedSocketAddress.extract)

    addrsWithWeight.foreach { case (unweightedAddr, weight) =>
      cachedFactories.get(unweightedAddr) match {
        case Some((f, oldWeight)) if weight != oldWeight =>
          // update factory with weight
          cachedFactories += unweightedAddr -> (f, weight)
        case None =>
          // add new factory
          cachedFactories += unweightedAddr -> (mkFactory(unweightedAddr), weight)
        case _ =>
          // nothing to do
      }
    }

    // remove a serviceFactory only when its status is !open with probationEnabled enabled
    (cachedFactories.keySet &~ addrsWithWeight.map(_._1)).foreach { sa =>
      cachedFactories.get(sa) match {
        case Some((factory, _)) if !probationEnabled || factory.status != Status.Open =>
          factory.close()
          cachedFactories -= sa

        case _ => // nothing to do
      }
    }
  }

  def make(
    params: Stack.Params,
    next: Stack[ServiceFactory[Req, Rep]]
  ): Stack[ServiceFactory[Req, Rep]] = {
    val ErrorLabel(errorLabel) = params[ErrorLabel]
    val Dest(dest) = params[Dest]
    val Param(loadBalancerFactory) = params[Param]
    val EnableProbation(probationEnabled) = params[EnableProbation]


    /**
     * Determine which stats receiver to use based on `perHostStats`
     * flag and the configured `HostStats` param. Report per-host stats
     * only when the flag is set.
     */
    val hostStatsReceiver =
      if (!perHostStats()) NullStatsReceiver
      else params[LoadBalancerFactory.HostStats].hostStatsReceiver

    val param.Stats(statsReceiver) = params[param.Stats]
    val param.Logger(log) = params[param.Logger]
    val param.Label(label) = params[param.Label]
    val param.Monitor(monitor) = params[param.Monitor]
    val param.Reporter(reporter) = params[param.Reporter]

    val noBrokersException = new NoBrokersAvailableException(errorLabel)

    def mkFactory(sockaddr: SocketAddress): ServiceFactory[Req, Rep] = {
      val stats = if (hostStatsReceiver.isNull) statsReceiver else {
        val scope = sockaddr match {
          case WeightedInetSocketAddress(addr, _) =>
            "%s:%d".format(addr.getHostName, addr.getPort)
          case other => other.toString
        }
        val host = hostStatsReceiver.scope(label).scope(scope)
        BroadcastStatsReceiver(Seq(host, statsReceiver))
      }

      val composite = reporter(label, Some(sockaddr)) andThen monitor

      val underlying = next.make(params +
          Transporter.EndpointAddr(SocketAddresses.unwrap(sockaddr)) +
          param.Stats(stats) +
          param.Monitor(composite))

      new ServiceFactoryProxy(underlying) {
        override def toString = sockaddr.toString
      }
    }

    val cachedFactories = mutable.Map.empty[SocketAddress, WeightedFactory[Req, Rep]]
    val endpoints = Activity(
      dest.map {
        case Addr.Bound(sockaddrs, metadata) =>
          updateFactories(
            processAddrs(params, sockaddrs.toSet), cachedFactories, mkFactory, probationEnabled)
          Activity.Ok(cachedFactories.values.toSet)

        case Addr.Neg =>
          log.info(s"$label: name resolution is negative")
          updateFactories(
            Set.empty, cachedFactories, mkFactory, probationEnabled)
          Activity.Ok(cachedFactories.values.toSet)

        case Addr.Failed(e) =>
          log.log(Level.INFO, s"$label: name resolution failed", e)
          Activity.Failed(e)

        case Addr.Pending =>
          if (log.isLoggable(Level.FINE)) {
            log.fine(s"$label: name resolution is pending")
          }
          Activity.Pending
      }
    )

    val rawStatsReceiver = statsReceiver match {
      case sr: RollupStatsReceiver => sr.self
      case sr => sr
    }

    val lb = loadBalancerFactory.newBalancer(
      endpoints,
      rawStatsReceiver.scope(role.toString),
      noBrokersException)

    val lbReady = lb match {
      case onReady: OnReady =>
        onReady.onReady before Future.value(lb)
      case _ =>
        log.warning("Load balancer cannot signal readiness and may throw "+
            "NoBrokersAvailableExceptions during resolution.")
        Future.value(lb)
    }

    val delayed = DelayedFactory.swapOnComplete(lbReady)
    Stack.Leaf(role, delayed)
  }
}