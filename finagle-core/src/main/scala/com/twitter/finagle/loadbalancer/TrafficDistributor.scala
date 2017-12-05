package com.twitter.finagle.loadbalancer

import com.twitter.finagle._
import com.twitter.finagle.addr.WeightedAddress
import com.twitter.finagle.service.{DelayedFactory, FailingFactory, ServiceFactoryRef}
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver, Verbosity}
import com.twitter.finagle.util.{Drv, Rng}
import com.twitter.util._

private object TrafficDistributor {

  /**
   * A [[ServiceFactory]] and its associated weight.
   */
  case class WeightedFactory[Req, Rep](factory: EndpointFactory[Req, Rep], weight: Double)

  /**
   * An intermediate representation of the endpoints that a load balancer
   * operates over, capable of being updated.
   */
  type BalancerEndpoints[Req, Rep] =
    Var[Activity.State[Set[EndpointFactory[Req, Rep]]]] with Updatable[
      Activity.State[Set[EndpointFactory[Req, Rep]]]
    ]

  /**
   * Represents cache entries for load balancer instances. Stores both
   * the load balancer instance and its backing updatable collection.
   * Size refers to the number of elements in `endpoints`.
   */
  case class CachedBalancer[Req, Rep](
    balancer: ServiceFactory[Req, Rep],
    endpoints: BalancerEndpoints[Req, Rep],
    size: Int
  )

  /**
   * A load balancer and its associated weight. Size refers to the
   * size of the balancers backing collection. The [[Distributor]]
   * operates over these.
   */
  case class WeightClass[Req, Rep](
    balancer: ServiceFactory[Req, Rep],
    endpoints: BalancerEndpoints[Req, Rep],
    weight: Double,
    size: Int
  )

  /**
   * Folds and accumulates over an [[Activity]] based event `stream` while biasing
   * for success by suppressing intermediate failures.
   *
   * Once we have seen data (via Activity.Ok), ignore Activity.Pending/Failed state
   * changes and keep using stale data to prevent discarding possibly valid data
   * if the updating activity is having transient failures.
   */
  private def safelyScanLeft[T, U](
    init: U,
    stream: Event[Activity.State[T]]
  )(f: (U, T) => U): Event[Activity.State[U]] = {
    val initState: Activity.State[U] = Activity.Ok(init)
    stream.foldLeft(initState) {
      case (Activity.Pending, Activity.Ok(update)) => Activity.Ok(f(init, update))
      case (Activity.Failed(_), Activity.Ok(update)) => Activity.Ok(f(init, update))
      case (Activity.Ok(state), Activity.Ok(update)) => Activity.Ok(f(state, update))
      case (stale @ Activity.Ok(state), Activity.Failed(_)) if init != state => stale
      case (stale @ Activity.Ok(state), Activity.Pending) if init != state => stale
      case (_, failed @ Activity.Failed(_)) => failed
      case (_, Activity.Pending) => Activity.Pending
    }
  }

  /**
   * Distributes requests to `classes` according to their weight and size.
   */
  private class Distributor[Req, Rep](
    classes: Iterable[WeightClass[Req, Rep]],
    rng: Rng = Rng.threadLocal
  ) extends ServiceFactory[Req, Rep] {

    private[this] val (balancers, drv): (IndexedSeq[ServiceFactory[Req, Rep]], Drv) = {
      val tupled = classes.map {
        case WeightClass(b, _, weight, size) => (b, weight * size)
      }
      val (bs, ws) = tupled.unzip
      (bs.toIndexedSeq, Drv.fromWeights(ws.toSeq))
    }

    def apply(conn: ClientConnection): Future[Service[Req, Rep]] =
      balancers(drv(rng))(conn)

    private[this] def endpoints: Seq[Closable] =
      classes.toSeq.map { wc =>
        wc.endpoints.sample() match {
          case Activity.Ok(set) => Closable.all(set.toSeq: _*)
          case _ => Closable.nop
        }
      }

    def close(deadline: Time): Future[Unit] =
      Closable
        .all(
          Closable.all(balancers: _*),
          Closable.all(endpoints: _*)
        )
        .close(deadline)

    private[this] val svcFactoryStatus: ServiceFactory[Req, Rep] => Status =
      sf => sf.status

    override def status = Status.bestOf[ServiceFactory[Req, Rep]](balancers, svcFactoryStatus)
    override def toString = s"Distributor($classes)"
  }
}

/**
 * A traffic distributor groups the input `dest` into distinct weight classes and
 * allocates traffic to each class. Classes are encoded in the stream of [[Address]]
 * instances in `dest`. The class operates with the following regime:

 * 1. For every distinct [[Address]] observed in `dest`, it creates a `newEndpoint`.
 * Each resulting `newEndpoint` is paired with a weight extracted from the [[Address]].
 * Calls to `newEndpoint` are assumed to be expensive, so they are cached by input address.
 *
 * 2. The weighted endpoints are partitioned into weight classes and each class is
 * serviced by a distinct `newBalancer` instance. That is, load offered to a weight
 * class is also load balanced across its members. Offered load is distributed according
 * to the classes weight and number of members that belong to the class.
 *
 * @param eagerEviction When set to false, an [[Address]] cache entry is only removed
 * when its associated [[ServiceFactory]] has a status that is not Status.Open. This allows
 * for stale cache entries across updates that are only evicted when a [[ServiceFactory]]
 * is no longer eligible to receive traffic (as indicated by its `status` field).
 */
private class TrafficDistributor[Req, Rep](
  dest: Activity[Set[Address]],
  newEndpoint: Address => ServiceFactory[Req, Rep],
  newBalancer: Activity[Set[EndpointFactory[Req, Rep]]] => ServiceFactory[Req, Rep],
  eagerEviction: Boolean,
  rng: Rng = Rng.threadLocal,
  statsReceiver: StatsReceiver = NullStatsReceiver
) extends ServiceFactory[Req, Rep] {
  import TrafficDistributor._

  /**
   * Creates a `newEndpoint` for each distinct [[Address]] in the `addrs`
   * stream. Calls to `newEndpoint` are cached based on the input address. The cache is
   * privy to [[Address]] with weight metadata and unwraps them. Weights
   * are extracted and coupled with the their respective result from `newEndpoint`. If
   * the [[Address]] does not have a weight, a default weight of 1.0 is used.
   */
  private[this] def weightEndpoints(
    addrs: Event[Activity.State[Set[Address]]]
  ): Event[Activity.State[Set[WeightedFactory[Req, Rep]]]] = {
    val init = Map.empty[Address, WeightedFactory[Req, Rep]]
    safelyScanLeft(init, addrs) {
      case (active, addrs) =>
        // Note, if an update contains multiple `Address` instances
        // with duplicate `weight` metadata, only one of the instances and its associated
        // factory is cached. Last write wins.
        val weightedAddrs: Set[(Address, Double)] = addrs.map(WeightedAddress.extract)
        val merged = weightedAddrs.foldLeft(active) {
          case (cache, (addr, weight)) =>
            cache.get(addr) match {
              // An update with an existing Address that has a new weight
              // results in the the weight being overwritten but the [[ServiceFactory]]
              // instance is maintained.
              case Some(wf @ WeightedFactory(_, w)) if w != weight =>
                cache.updated(addr, wf.copy(weight = weight))
              case None =>
                val endpoint = new LazyEndpointFactory(() => newEndpoint(addr), addr)
                cache.updated(addr, WeightedFactory(endpoint, weight))
              case _ => cache
            }
        }

        // Remove stale cache entries. When `eagerEviction` is false cache
        // entries are only removed in subsequent stream updates.
        val removed = merged.keySet -- weightedAddrs.map(_._1)
        removed.foldLeft(merged) {
          case (cache, addr) =>
            cache.get(addr) match {
              case Some(WeightedFactory(f, _)) if eagerEviction || f.status != Status.Open =>
                f.close()
                cache - addr
              case _ => cache
            }
        }
    }.map {
      case Activity.Ok(cache) => Activity.Ok(cache.values.toSet)
      case Activity.Pending => Activity.Pending
      case failed @ Activity.Failed(_) => failed
    }
  }

  /**
   * Partitions `endpoints` and assigns a `newBalancer` instance to each partition.
   * Because balancer instances are stateful, they need to be cached across updates.
   */
  private[this] def partition(
    endpoints: Event[Activity.State[Set[WeightedFactory[Req, Rep]]]]
  ): Event[Activity.State[Iterable[WeightClass[Req, Rep]]]] = {
    // Cache entries are balancer instances together with their backing collection
    // which is updatable. The entries are keyed by weight class.
    val init = Map.empty[Double, CachedBalancer[Req, Rep]]
    safelyScanLeft(init, endpoints) {
      case (balancers, activeSet) =>
        val weightedGroups: Map[Double, Set[WeightedFactory[Req, Rep]]] =
          activeSet.groupBy(_.weight)

        val merged = weightedGroups.foldLeft(balancers) {
          case (cache, (weight, factories)) =>
            val unweighted = factories.map { case WeightedFactory(f, _) => f }
            val newCacheEntry = if (cache.contains(weight)) {
              // an update that contains an existing weight class updates
              // the balancers backing collection.
              val cached = cache(weight)
              cached.endpoints.update(Activity.Ok(unweighted))
              cached.copy(size = unweighted.size)
            } else {
              val endpoints: BalancerEndpoints[Req, Rep] = Var(Activity.Ok(unweighted))
              val lb = newBalancer(Activity(endpoints))
              CachedBalancer(lb, endpoints, unweighted.size)
            }
            cache + (weight -> newCacheEntry)
        }

        // weight classes that no longer exist in the update are removed from
        // the cache and the associated balancer instances are closed.
        val removed = balancers.keySet -- weightedGroups.keySet
        removed.foldLeft(merged) {
          case (cache, weight) =>
            cache.get(weight) match {
              case Some(CachedBalancer(bal, _, _)) =>
                bal.close()
                cache - weight
              case _ => cache
            }
        }
    }.map {
      case Activity.Ok(cache) =>
        Activity.Ok(cache.map {
          case (weight, CachedBalancer(bal, endpoints, size)) =>
            WeightClass(bal, endpoints, weight, size)
        })
      case Activity.Pending => Activity.Pending
      case failed @ Activity.Failed(_) => failed
    }
  }

  private[this] val weightClasses = partition(weightEndpoints(dest.states))
  private[this] val pending = new Promise[ServiceFactory[Req, Rep]]
  private[this] val init: ServiceFactory[Req, Rep] = new DelayedFactory(pending)

  @volatile private[this] var meanWeight = 0.0F
  @volatile private[this] var numWeightClasses = 0.0F

  private[this] val gauges = Seq(
    statsReceiver.addGauge(Verbosity.Debug, "meanweight") { meanWeight },
    statsReceiver.addGauge("num_weight_classes") { numWeightClasses }
  )

  private[this] def updateGauges(classes: Iterable[WeightClass[Req, Rep]]): Unit = {
    numWeightClasses = classes.size.toFloat
    val numEndpoints = classes.map(_.size).sum
    meanWeight = if (numEndpoints == 0) 0.0F else {
      classes.map { c => c.weight * c.size }.sum.toFloat / numEndpoints
    }
  }

  // Translate the stream of weightClasses into a stream of underlying
  // ServiceFactories that can service requests.
  private[this] val underlying: Event[ServiceFactory[Req, Rep]] =
    weightClasses.foldLeft(init) {
      case (_, Activity.Ok(wcs)) if wcs.isEmpty =>
        // Defer the handling of an empty destination set to `newBalancer`
        val emptyBal = newBalancer(Activity(Var(Activity.Ok(Set.empty[EndpointFactory[Req, Rep]]))))
        updateGauges(wcs)
        pending.updateIfEmpty(Return(emptyBal))
        emptyBal
      case (_, Activity.Ok(wcs)) =>
        val dist = new Distributor(wcs, rng)
        updateGauges(wcs)
        pending.updateIfEmpty(Return(dist))
        dist
      case (_, Activity.Failed(e)) =>
        updateGauges(Iterable.empty)
        val failing = new FailingFactory[Req, Rep](e)
        pending.updateIfEmpty(Return(failing))
        failing
      case (staleState, Activity.Pending) =>
        // This should only happen on initialization and never be seen again
        // due to the logic in safelyScanLeft.
        staleState
    }

  // Note, we only call close on the underlying `ref` when the TrafficDistributor
  // is closed. The backing resources (i.e. balancers and endpoints) are
  // otherwise managed by the caches in `partition` and `weightEndpoints`.
  private[this] val ref = new ServiceFactoryRef(init)
  private[this] val obs = underlying.register(Witness(ref))

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] = ref(conn)

  def close(deadline: Time): Future[Unit] = {
    gauges.foreach(_.remove())
    // Note, we want to sequence here since we want to stop
    // flushing to `ref` before we call close on it.
    Closable.sequence(obs, ref).close(deadline)
  }

  override def status: Status = ref.status
}
