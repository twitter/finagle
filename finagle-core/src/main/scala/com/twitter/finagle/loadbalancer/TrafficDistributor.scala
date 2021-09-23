package com.twitter.finagle.loadbalancer

import com.twitter.finagle._
import com.twitter.finagle.addr.WeightedAddress
import com.twitter.finagle.loadbalancer.distributor.AddrLifecycle._
import com.twitter.finagle.loadbalancer.distributor.AddressedFactory
import com.twitter.finagle.loadbalancer.distributor.BalancerEndpoints
import com.twitter.finagle.loadbalancer.distributor.CachedBalancer
import com.twitter.finagle.loadbalancer.distributor.WeightClass
import com.twitter.finagle.service.DelayedFactory
import com.twitter.finagle.service.FailingFactory
import com.twitter.finagle.service.ServiceFactoryRef
import com.twitter.finagle.stats.Counter
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.stats.Verbosity
import com.twitter.finagle.util.Drv
import com.twitter.finagle.util.Rng
import com.twitter.logging.Logger
import com.twitter.util._
import scala.util.control.NonFatal

private[finagle] object TrafficDistributor {
  val log = Logger.get(classOf[TrafficDistributor[_, _]])

  /**
   * Creates a `newEndpoint` for each distinct [[Address]] in the `addrs`
   * stream. Calls to `newEndpoint` are cached based on the input address. The cache is
   * privy to [[Address]] with weight metadata and unwraps them. Weights
   * are extracted and coupled with the their respective result from `newEndpoint`. If
   * the [[Address]] does not have a weight, a default weight of 1.0 is used.
   *
   * For every distinct [[Address]] observed in `addrs`, it creates a `newEndpoint`.
   * Each resulting `newEndpoint` is paired with a weight extracted from the [[Address]].
   * Calls to `newEndpoint` are assumed to be expensive, so they are cached by input address.
   *
   * @param eagerEviction When set to false, an [[Address]] cache entry is only removed
   * when its associated [[ServiceFactory]] has a status that is not Status.Open. This allows
   * for stale cache entries across updates that are only evicted when a [[ServiceFactory]]
   * is no longer eligible to receive traffic (as indicated by its `status` field).
   */
  private[finagle] def weightEndpoints[Req, Rep](
    addrs: Activity[Set[Address]],
    newEndpoint: Address => ServiceFactory[Req, Rep],
    eagerEviction: Boolean
  ): Event[Activity.State[Set[AddressedFactory[Req, Rep]]]] = {
    val init = Map.empty[Address, AddressedFactory[Req, Rep]]
    safelyScanLeft(init, addrs.run.changes.dedup) {
      case (active, addrs) =>
        // Note, if an update contains multiple `Address` instances that are the same
        // with duplicate `weight` metadata, only one of the instances and its associated
        // factory is cached. Last write wins.
        val merged = addrs.foldLeft(active) {
          case (cache, weightedAddr @ WeightedAddress(addr, weight)) =>
            cache.get(addr) match {
              // An update with an existing Address that has a new weight
              // results in the the weight being overwritten but the [[ServiceFactory]]
              // instance is maintained.
              case Some(af @ AddressedFactory(_, WeightedAddress(_, w))) if w != weight =>
                cache.updated(addr, af.copy(address = weightedAddr))
              case None =>
                val endpoint = new LazyEndpointFactory(() => newEndpoint(addr), addr)
                cache.updated(addr, AddressedFactory(endpoint, weightedAddr))
              case _ => cache
            }
        }

        // Remove stale cache entries. When `eagerEviction` is false cache
        // entries are only removed in subsequent stream updates.
        removeStaleAddresses(merged, addrs, eagerEviction)

    }.map {
      case Activity.Ok(cache) => Activity.Ok(cache.values.toSet)
      case Activity.Pending => Activity.Pending
      case failed @ Activity.Failed(_) => failed
    }
  }

  /**
   * Distributes requests to `classes` according to their weight and size.
   */
  private class Distributor[Req, Rep](
    classes: Iterable[WeightClass[Req, Rep]],
    busyWeightClasses: Counter,
    reuseEndpoints: Boolean,
    rng: Rng = Rng.threadLocal)
      extends ServiceFactory[Req, Rep] {

    private[this] val (balancers, weights, drv): (
      IndexedSeq[ServiceFactory[Req, Rep]],
      IndexedSeq[Double],
      Drv
    ) = {
      val wcs = classes.toIndexedSeq.sortBy(_.weight)
      val tupled = wcs.map {
        case WeightClass(b, _, weight, size) => (b, weight, weight * size)
      }
      val (bs, w, ws) = tupled.unzip3

      (bs, w, Drv.fromWeights(ws))
    }

    // Ignore the weight vector and pick the next available balancer starting at `startingIndex`
    private[this] def firstOpen(startingIndex: Int): ServiceFactory[Req, Rep] = {
      var i = startingIndex + 1

      // walk around the vector of balancers and terminate if we fully wrap around,
      // returning the last balancer picked
      var b = balancers(i % balancers.size)
      while (b.status != Status.Open && i != startingIndex) {
        i = (i + 1) % balancers.size

        b = balancers(i)
      }

      b
    }

    def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
      val index = drv(rng)
      val balancer = balancers(index)

      // If we have only a single balancer, there's no other to pick from. We must use this
      // balancer regardless of the status
      if (balancers.size == 1 || balancer.status == Status.Open) balancer(conn)
      else {
        busyWeightClasses.incr()
        log.debug(
          s"weight class ${weights(index)}'s endpoints all Busy. "
            + "Ignoring the weight vector and picking the next available balancer")

        firstOpen(index)(conn)
      }
    }

    private[this] def endpoints: Seq[Closable] =
      classes.toSeq.map { wc =>
        wc.endpoints.sample() match {
          case Activity.Ok(set) => Closable.all(set.toSeq: _*)
          case _ => Closable.nop
        }
      }

    def close(deadline: Time): Future[Unit] = {
      if (reuseEndpoints) {
        Closable.all(balancers: _*).close(deadline)
      } else {
        Closable
          .all(
            Closable.all(balancers: _*),
            Closable.all(endpoints: _*)
          )
          .close(deadline)
      }
    }

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
 *
 * The weighted endpoints are partitioned into weight classes and each class is
 * serviced by a distinct `newBalancer` instance. That is, load offered to a weight
 * class is also load balanced across its members. Offered load is distributed according
 * to the classes weight and number of members that belong to the class.
 *
 * @param newBalancer A lambda to create a new balancer from a set of endpoints and a boolean
 * indicator, `disableEagerConnection`, if the balancer should have the
 * [[c.t.f.loadbalancer.aperture.EagerConnections]] feature disabled.
 */
private class TrafficDistributor[Req, Rep](
  dest: Event[Activity.State[Set[AddressedFactory[Req, Rep]]]],
  newBalancer: (Activity[Set[EndpointFactory[Req, Rep]]], Boolean) => ServiceFactory[Req, Rep],
  reuseEndpoints: Boolean,
  rng: Rng = Rng.threadLocal,
  statsReceiver: StatsReceiver = NullStatsReceiver)
    extends ServiceFactory[Req, Rep] {
  import TrafficDistributor._

  /**
   * Partitions `endpoints` and assigns a `newBalancer` instance to each partition.
   * Because balancer instances are stateful, they need to be cached across updates.
   */
  private[this] def partition(
    endpoints: Event[Activity.State[Set[AddressedFactory[Req, Rep]]]]
  ): Event[Activity.State[Iterable[WeightClass[Req, Rep]]]] = {
    // Cache entries are balancer instances together with their backing collection
    // which is updatable. The entries are keyed by weight class.
    val init = Map.empty[Double, CachedBalancer[Req, Rep]]

    val balancerDiffOps = new DiffOps[
      AddressedFactory[Req, Rep],
      CachedBalancer[Req, Rep]
    ] {
      // Close balancers that don't correspond to new endpoints.
      def remove(cachedBalancer: CachedBalancer[Req, Rep]): Unit =
        try cachedBalancer.balancer.close()
        catch {
          case NonFatal(t) => log.warning(t, "unable to close balancer")
        }

      // Construct new balancers from new endpoints.
      def add(
        factories: Set[AddressedFactory[Req, Rep]]
      ): CachedBalancer[Req, Rep] = {
        val group = factories.map(_.factory)
        val weight = if (factories.isEmpty) 1D else factories.head.weight
        val endpoints: BalancerEndpoints[Req, Rep] = Var(Activity.Ok(group))

        // we disable eager connections for non 1.0 weight class balancers. We assume the 1.0
        // weight balancer to be the main balancer and because sessions are managed independently
        // by each balancer, we avoid eagerly creating connections for balancers that may not be
        // long-lived.
        val bal = newBalancer(Activity(endpoints), weight != 1.0)
        CachedBalancer(bal, endpoints, group.size)
      }

      // Update existing balancers with new endpoints.
      def update(
        factories: Set[AddressedFactory[Req, Rep]],
        cachedBalancer: CachedBalancer[Req, Rep]
      ): CachedBalancer[Req, Rep] = {
        val group = factories.map(_.factory)
        cachedBalancer.endpoints.update(Activity.Ok(group))
        cachedBalancer.copy(size = group.size)
      }
    }

    safelyScanLeft(init, endpoints) { (balancers, activeSet) =>
      // group the factories and partition them to CachedBalancer by weight
      val result = updatePartitionMap(
        balancers,
        activeSet,
        (addressedFactory: AddressedFactory[Req, Rep]) => Seq(addressedFactory.weight),
        balancerDiffOps)

      // Intercept the empty balancer set and replace it with a single balancer
      // that has an empty set of endpoints.
      if (result.nonEmpty) result
      else {
        // It's important that we construct the balancer within the scanLeft,
        // so a subsequent iteration has a chance to close it. This way,
        // scanLeft is responsible for creating and managing the resource.
        val endpoints: BalancerEndpoints[Req, Rep] = Var(Activity.Ok(Set.empty))
        val bal = newBalancer(Activity(endpoints), false)
        Map(1.0 -> CachedBalancer(bal, endpoints, 0))
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

  private[this] val weightClasses = partition(dest)
  private[this] val pending = new Promise[ServiceFactory[Req, Rep]]
  private[this] val init: ServiceFactory[Req, Rep] = new DelayedFactory(pending)

  @volatile private[this] var meanWeight = 0.0f
  @volatile private[this] var numWeightClasses = 0.0f

  private[this] val gauges = Seq(
    statsReceiver.addGauge(Verbosity.Debug, "meanweight") { meanWeight },
    statsReceiver.addGauge("num_weight_classes") { numWeightClasses }
  )

  private[this] val busyWeightClasses: Counter = statsReceiver.counter("busy_weight_classes")

  private[this] def updateGauges(
    classes: Iterable[WeightClass[Req, Rep]]
  ): Unit = {
    numWeightClasses = classes.size.toFloat
    val numEndpoints = classes.map(_.size).sum
    meanWeight =
      if (numEndpoints == 0) 0.0f
      else {
        classes
          .map { c => c.weight * c.size }.sum.toFloat / numEndpoints
      }
  }

  // Translate the stream of weightClasses into a stream of underlying
  // ServiceFactories that can service requests.
  private[this] val underlying: Event[ServiceFactory[Req, Rep]] =
    weightClasses.foldLeft(init) {
      case (_, Activity.Ok(wcs)) =>
        val dist = new Distributor(wcs, busyWeightClasses, reuseEndpoints, rng)
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
