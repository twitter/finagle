package com.twitter.finagle.loadbalancer

import com.twitter.finagle._
import com.twitter.finagle.addr.WeightedAddress
import com.twitter.finagle.service.{DelayedFactory, FailingFactory, ServiceFactoryRef}
import com.twitter.finagle.stats.{Counter, NullStatsReceiver, StatsReceiver, Verbosity}
import com.twitter.finagle.util.{Drv, Rng}
import com.twitter.logging.Logger
import com.twitter.util._
import scala.collection.mutable.Builder
import scala.collection.{immutable, mutable}
import scala.util.control.NonFatal

private[finagle] object TrafficDistributor {
  val log = Logger.get(classOf[TrafficDistributor[_, _]])

  /**
   * A [[ServiceFactory]] and its associated weight.
   */
  case class WeightedFactory[Req, Rep](factory: EndpointFactory[Req, Rep], weight: Double)

  /**
   * An intermediate representation of the endpoints that a load balancer
   * operates over, capable of being updated.
   */
  type BalancerEndpoints[Req, Rep] =
    Var[Activity.State[Set[EndpointFactory[Req, Rep]]]]
      with Updatable[
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
    size: Int)

  /**
   * A load balancer and its associated weight. Size refers to the
   * size of the balancers backing collection. The [[Distributor]]
   * operates over these.
   */
  case class WeightClass[Req, Rep](
    balancer: ServiceFactory[Req, Rep],
    endpoints: BalancerEndpoints[Req, Rep],
    weight: Double,
    size: Int)

  /**
   * Transforms a [[Var]] of bound Addresses to an [[Activity]] of bound Addresses
   * @note The Addr.Bound metadata is stripped out in this method, the metadata can
   *       be found in [[AddrMetadataExtraction]]
   */
  private[finagle] def varAddrToActivity(dest: Var[Addr], label: String): Activity[Set[Address]] =
    Activity(dest.map {
      case Addr.Bound(set, _) => Activity.Ok(set)
      case Addr.Neg =>
        log.info(s"$label: name resolution is negative (local dtab: ${Dtab.local})")
        Activity.Ok(Set.empty[Address])
      case Addr.Failed(e) =>
        log.info(s"$label: name resolution failed  (local dtab: ${Dtab.local})", e)
        Activity.Failed(e)
      case Addr.Pending =>
        log.debug(s"$label: name resolution is pending")
        Activity.Pending
    })

  /**
   * Folds and accumulates over an [[Activity]] based event `stream` while biasing
   * for success by suppressing intermediate failures.
   *
   * Once we have seen data (via Activity.Ok), ignore Activity.Pending/Failed state
   * changes and keep using stale data to prevent discarding possibly valid data
   * if the updating activity is having transient failures.
   */
  private[finagle] def safelyScanLeft[T, U](
    init: U,
    stream: Event[Activity.State[T]]
  )(
    f: (U, T) => U
  ): Event[Activity.State[U]] = {
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
   * This interface handles operations when partitioning a Set of elements into a Map.
   */
  trait DiffOps[U, Partition] {

    /** Removes/cleans up an existing partition */
    def remove(partition: Partition): Unit

    /** Constructs a new partition from the an addition set of elements */
    def add(current: Set[U]): Partition

    /** Updates the existing partition with a set of elements */
    def update(current: Set[U], partition: Partition): Partition
  }

  /**
   * Transform the current element Set to a new Partition Map which updates the previous
   * Partition Map by provided operations.
   *
   * @param accumulated Previous Partition Map
   * @param current     Current Set of elements
   * @param getKeys      A discriminator function for current Set to get keys
   * @param diffOps     [[DiffOps]] to handle the diff between the previous partition map and
   *                    newly transformed partition map from the current Set. This function
   *                    usually handles transactions during creating, updating, and removing.
   */
  private[finagle] def updatePartitionMap[Key, Partition, U](
    accumulated: Map[Key, Partition],
    current: Set[U],
    getKeys: U => Seq[Key],
    diffOps: DiffOps[U, Partition]
  ): Map[Key, Partition] = {
    val grouped = groupBy(current, getKeys)
    val removals = accumulated.keySet &~ grouped.keySet
    val additions = grouped.keySet &~ accumulated.keySet
    val updates = grouped.keySet & accumulated.keySet
    removals.foreach { key => diffOps.remove(accumulated(key)) }
    val added = additions.map { key => key -> diffOps.add(grouped(key)) }.toMap
    val updated = updates.map { key => key -> diffOps.update(grouped(key), accumulated(key)) }.toMap
    added ++ updated
  }

  /**
   * A modified version of scala collection's groupBy function. `f` is a multi-mapping function
   * and this achieves many to many mapping.
   */
  private def groupBy[U, Key](coll: Set[U], f: U => Seq[Key]): immutable.Map[Key, Set[U]] = {
    val m = mutable.Map.empty[Key, Builder[U, Set[U]]]
    for (elem <- coll) {
      val keys = f(elem)
      for (k <- keys) {
        val bldr = m.getOrElseUpdate(k, Set.newBuilder)
        bldr += elem
      }
    }
    val b = immutable.Map.newBuilder[Key, Set[U]]
    for ((k, v) <- m)
      b += ((k, v.result))
    b.result
  }

  /**
   * Distributes requests to `classes` according to their weight and size.
   */
  private class Distributor[Req, Rep](
    classes: Iterable[WeightClass[Req, Rep]],
    busyWeightClasses: Counter,
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
  statsReceiver: StatsReceiver = NullStatsReceiver)
    extends ServiceFactory[Req, Rep] {
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
                try f.close()
                catch {
                  case NonFatal(t) => log.warning(t, s"unable to close endpoint $addr")
                }
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

    val balancerDiffOps = new DiffOps[WeightedFactory[Req, Rep], CachedBalancer[Req, Rep]] {
      // Close balancers that don't correspond to new endpoints.
      def remove(cachedBalancer: CachedBalancer[Req, Rep]): Unit =
        try cachedBalancer.balancer.close()
        catch {
          case NonFatal(t) => log.warning(t, "unable to close balancer")
        }

      // Construct new balancers from new endpoints.
      def add(factories: Set[WeightedFactory[Req, Rep]]): CachedBalancer[Req, Rep] = {
        val group = factories.map(_.factory)
        val endpoints: BalancerEndpoints[Req, Rep] = Var(Activity.Ok(group))
        val bal = newBalancer(Activity(endpoints))
        CachedBalancer(bal, endpoints, group.size)
      }

      // Update existing balancers with new endpoints.
      def update(
        factories: Set[WeightedFactory[Req, Rep]],
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
        (weightedFactory: WeightedFactory[Req, Rep]) => Seq(weightedFactory.weight),
        balancerDiffOps)

      // Intercept the empty balancer set and replace it with a single balancer
      // that has an empty set of endpoints.
      if (result.nonEmpty) result
      else {
        // It's important that we construct the balancer within the scanLeft,
        // so a subsequent iteration has a chance to close it. This way,
        // scanLeft is responsible for creating and managing the resource.
        val endpoints: BalancerEndpoints[Req, Rep] = Var(Activity.Ok(Set.empty))
        val bal = newBalancer(Activity(endpoints))
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

  private[this] val weightClasses = partition(weightEndpoints(dest.states))
  private[this] val pending = new Promise[ServiceFactory[Req, Rep]]
  private[this] val init: ServiceFactory[Req, Rep] = new DelayedFactory(pending)

  @volatile private[this] var meanWeight = 0.0f
  @volatile private[this] var numWeightClasses = 0.0f

  private[this] val gauges = Seq(
    statsReceiver.addGauge(Verbosity.Debug, "meanweight") { meanWeight },
    statsReceiver.addGauge("num_weight_classes") { numWeightClasses }
  )

  private[this] val busyWeightClasses: Counter = statsReceiver.counter("busy_weight_classes")

  private[this] def updateGauges(classes: Iterable[WeightClass[Req, Rep]]): Unit = {
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
        val dist = new Distributor(wcs, busyWeightClasses, rng)
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
