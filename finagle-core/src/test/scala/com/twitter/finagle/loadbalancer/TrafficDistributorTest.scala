package com.twitter.finagle.loadbalancer

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.addr.WeightedAddress
import com.twitter.finagle.client.utils.StringClient
import com.twitter.finagle.loadbalancer.distributor.AddrLifecycle.DiffOps
import com.twitter.finagle.loadbalancer.distributor.AddressedFactory
import com.twitter.finagle.server.utils.StringServer
import com.twitter.finagle.stats._
import com.twitter.finagle.util.Rng
import com.twitter.util.{Function => _, _}
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicReference
import com.twitter.finagle.loadbalancer.distributor.AddrLifecycle
import org.scalatest.funsuite.AnyFunSuite

private object TrafficDistributorTest {
  def await[A](f: Future[A]): A = Await.result(f, 5.seconds)

  // The distributor is not privy to this wrapped socket address and
  // it allows us to retrieve the weight class.
  object WeightedTestAddr {
    val key = "test_weight"

    def apply(port: Int, weight: Double): Address =
      Address.Inet(new InetSocketAddress(port), Addr.Metadata(key -> weight))

    def unapply(addr: Address): Option[(Int, Double)] = addr match {
      case Address.Inet(ia, metadata) if metadata.nonEmpty =>
        Some((ia.getPort, metadata(key).asInstanceOf[Double]))
      case _ => None
    }
  }

  val weightClass: (Double, Int) => Set[Address] =
    (w, size) => (0 until size).toSet.map { i: Int => WeightedAddress(WeightedTestAddr(i, w), w) }

  val busyWeight = 2.2
  case class AddressFactory(address: Address) extends ServiceFactory[Int, Int] {
    def apply(conn: ClientConnection) = Future.value(Service.mk(i => Future.value(i)))
    def close(deadline: Time): Future[Unit] = Future.Done
    override def toString: String = s"AddressFactory($address)"
    override def status: Status =
      address match {
        case WeightedTestAddr(_, weight) if weight == busyWeight => Status.Busy
        case _ => Status.Open
      }
  }

  private case class Balancer(
    endpoints: Activity[Iterable[AddressFactory]],
    val disableEagerConnections: Boolean,
    onClose: scala.Function0[Unit] = () => ())
      extends ServiceFactory[Int, Int] {
    var offeredLoad = 0
    var balancerIsClosed = false
    var numOfEndpoints = 0
    var queriedStatus = 0
    def apply(conn: ClientConnection): Future[Service[Int, Int]] = {
      offeredLoad += 1
      // new hotness in load balancing
      val nodes = endpoints.sample().toSeq
      numOfEndpoints = nodes.size
      if (nodes.isEmpty) Future.exception(new NoBrokersAvailableException)
      else nodes((math.random * nodes.size).toInt)(conn)
    }
    def close(deadline: Time): Future[Unit] = {
      balancerIsClosed = true
      onClose()

      Future.Done
    }

    override def status: Status = {
      queriedStatus += 1

      val addressStatus: AddressFactory => Status = a => a.status
      Status.bestOf(endpoints.sample().toSeq, addressStatus)
    }

    override def toString: String = s"Balancer($endpoints)"
  }

  /**
   * Return the distribution for the the given `balancer` as a tuple
   * of (weight, size, offer load).
   */
  def distribution(balancers: Set[Balancer]): Set[(Double, Int, Int)] = {
    balancers.flatMap { b =>
      val endpoints = b.endpoints.sample()
      endpoints.map {
        case AddressFactory(WeightedTestAddr(_, w)) =>
          (w * endpoints.size, endpoints.size, b.offeredLoad)
      }
    }
  }

  private class Ctx {
    var newEndpointCalls = 0
    def newEndpoint(addr: Address): ServiceFactory[Int, Int] = {
      newEndpointCalls += 1
      AddressFactory(addr)
    }

    def numWeightClasses(sr: InMemoryStatsReceiver): Float =
      sr.gauges(Seq("num_weight_classes"))()

    var closeBalancerCalls = 0
    var newBalancerCalls = 0
    var balancers: Set[Balancer] = Set.empty
    def newBalancer(
      eps: Activity[Set[EndpointFactory[Int, Int]]],
      disableEagerConnections: Boolean
    ): ServiceFactory[Int, Int] = {

      newBalancerCalls += 1
      // eagerly establish the lazy endpoints and extract the
      // underlying `AddressFactory`
      val addressFactories = eps.map { set =>
        set.map { epsf =>
          await(epsf())
          epsf.asInstanceOf[LazyEndpointFactory[Int, Int]].self.get.asInstanceOf[AddressFactory]
        }
      }
      val b = Balancer(addressFactories, disableEagerConnections, () => closeBalancerCalls += 1)
      balancers += b
      b
    }

    def newDist(
      dest: Var[Activity.State[Set[Address]]],
      eagerEviction: Boolean = true,
      statsReceiver: StatsReceiver = NullStatsReceiver,
      autoPrime: Boolean = false
    ): TrafficDistributor[Int, Int] = {
      val endpoints = TrafficDistributor.weightEndpoints(
        Activity(dest),
        newEndpoint,
        eagerEviction
      )

      val dist = new TrafficDistributor[Int, Int](
        dest = endpoints,
        newBalancer = newBalancer,
        statsReceiver = statsReceiver,
        rng = Rng("seed".hashCode)
      )

      if (autoPrime) {
        // Primes the distributor such that it creates its underlying
        // resources (i.e. endpoint stacks). Note, do NOT block here
        // since this is a circular dependency. Instead, we can rely
        // on the fact that we are serialized since the same thread
        // that updates `dest` will call this closure as well.
        dest.changes.respond { _ => for (_ <- 0 to 100) { dist() } }
      }

      dist
    }

    def resetCounters(): Unit = {
      newEndpointCalls = 0
      newBalancerCalls = 0
    }
  }

  private class CumulativeGaugeInMemoryStatsReceiver extends StatsReceiverWithCumulativeGauges {
    private[this] val underlying = new InMemoryStatsReceiver()
    override val repr: AnyRef = this
    override def counter(metricBuilder: MetricBuilder): ReadableCounter =
      underlying.counter(metricBuilder)
    override def stat(metricBuilder: MetricBuilder): ReadableStat =
      underlying.stat(metricBuilder)

    protected[this] def registerGauge(metricBuilder: MetricBuilder, f: => Float): Unit =
      underlying.addGauge(metricBuilder.name: _*)(f)

    protected[this] def deregisterGauge(metricBuilder: MetricBuilder): Unit =
      underlying.gauges -= metricBuilder.name

    def counters: Map[Seq[String], Long] = underlying.counters.toMap
    def stats: Map[Seq[String], Seq[Float]] = underlying.stats.toMap
    def gauges: Map[Seq[String], () => Float] = underlying.gauges.toMap

    def numGauges(name: Seq[String]): Int =
      numUnderlying(name: _*)
  }
}

class TrafficDistributorTest extends AnyFunSuite {
  import TrafficDistributorTest._

  test("repicks against a busy balancer")(new Ctx {
    // weight 2.0 is the "Busy" Balancer
    val init: Set[Address] = Seq((1.0, 10), (busyWeight, 10))
      .flatMap(weightClass.tupled).toSet
    val dest = Var(Activity.Ok(init))
    val sr = new InMemoryStatsReceiver
    val dist = newDist(dest, statsReceiver = sr)

    val R = 50
    for (_ <- 0 until R) dist()
    assert(numWeightClasses(sr) == 2)

    val busyBalancer = balancers.find(_.status == Status.Busy).get
    assert(busyBalancer.offeredLoad == 0)
    assert(busyBalancer.queriedStatus > 0)

    // lazy endpoint construction
    assert(busyBalancer.numOfEndpoints == 0)

    // the next available balancer should have received all the traffic
    val openBalancer = balancers.find(_.status == Status.Open).get
    assert(openBalancer.offeredLoad == R)
  })

  test("terminates repicking strategy if all balancers are busy")(new Ctx {
    val init: Set[Address] = Seq((busyWeight, 2))
      .flatMap(weightClass.tupled).toSet
    val dest = Var(Activity.Ok(init))
    val dist = newDist(dest)

    val R = 10
    for (_ <- 0 until R) dist()
    assert(balancers.head.offeredLoad == R)
  })

  test("distributes when weights are uniform")(new Ctx {
    val init: Set[Address] = weightClass(5.0, 100)
    val dest = Var(Activity.Ok(init))
    val sr = new InMemoryStatsReceiver
    val dist = newDist(dest, statsReceiver = sr)

    val R = 50
    for (_ <- 0 until R) dist()
    assert(numWeightClasses(sr) == 1)
    assert(balancers.head.offeredLoad == R)
    assert(sr.gauges(Seq("meanweight"))() == 5.0)
  })

  test("distributes according to non-uniform weights")(new Ctx {
    locally {
      val R = 1000
      val ε: Double = 0.01
      val weightClasses = Seq((1.0, 1000), (2.0, 5), (10.0, 150))
      val classes = weightClasses.flatMap(weightClass.tupled).toSet
      val weightSum = weightClasses.foldLeft(0.0) {
        case (sum, tup) =>
          val (w, t) = tup
          sum + (w * t)
      }

      val dest = Var(Activity.Ok(classes))
      val dist = newDist(dest)
      for (_ <- 0 until R) dist()

      distribution(balancers).foreach {
        case ((w, _, l)) => assert(math.abs(w / weightSum - l / R.toDouble) < ε)
      }
    }

    locally {
      val R = 100 * 1000
      // This shows that weights can still be interpreted as multipliers for load relative
      // to other nodes. For example, a node with weight 2.0 should receive roughly twice
      // the traffic it would have normally received with weight 1.0. We say "roughly"
      // because this assumes a purely equitable distribution from the load balancers.
      // In practice, the lbs aren't purely equitable –- their distribution is impacted
      // by other feedback as well (e.g. latency, failure, etc.) -- but that should be okay
      // for most use-cases of weights.
      val ε: Double = .1 // 10%
      balancers = Set.empty
      val weightClasses = Seq((1.0, 500), (2.0, 1), (3.0, 1), (4.0, 1))
      val classes = weightClasses.flatMap(weightClass.tupled).toSet
      val dest = Var(Activity.Ok(classes))
      val dist = newDist(dest)
      for (_ <- 0 until R) dist()

      val result = distribution(balancers)

      val baseline = result.collectFirst {
        case ((weight, size, loadOffered)) if size / weight == 1.0 => loadOffered / weight
      }.get

      result.foreach { case ((w, s, l)) => assert(math.abs((l / w) - baseline) <= baseline * ε) }
    }

  })

  test("memoize calls to newEndpoint and newBalancer")(new Ctx {
    val init: Set[Address] = (1 to 5).map(Address(_)).toSet
    val dest = Var(Activity.Ok(init))
    val sr = new InMemoryStatsReceiver
    newDist(dest, statsReceiver = sr, autoPrime = true)

    assert(newEndpointCalls == init.size)
    assert(newBalancerCalls == 1)
    assert(numWeightClasses(sr) == 1)
    assert(balancers.head.endpoints.sample() == init.map(AddressFactory))

    val update: Set[Address] = (3 to 10).map(Address(_)).toSet
    dest() = Activity.Ok(update)
    assert(newEndpointCalls != init.size + update.size)
    assert(newEndpointCalls == (init ++ update).size)
    assert(newBalancerCalls == 1)
    assert(balancers.head.endpoints.sample() == update.map(AddressFactory))
  })

  test("partition endpoints into weight classes")(new Ctx {
    val init: Set[Address] = (1 to 5).map { i => WeightedAddress(Address(i), i) }.toSet
    val dest = Var(Activity.Ok(init))
    val sr = new InMemoryStatsReceiver
    newDist(dest, statsReceiver = sr, autoPrime = true)

    assert(newBalancerCalls == init.size)
    assert(numWeightClasses(sr) == init.size)
    assert(newEndpointCalls == init.size)

    // insert new endpoints on existing weight classes
    resetCounters()
    val existingWeight = 3.0
    val newAddrs = Set(
      WeightedAddress(Address(6), existingWeight),
      WeightedAddress(Address(7), existingWeight),
      WeightedAddress(Address(8), existingWeight)
    )
    val update: Set[Address] = init ++ newAddrs
    dest() = Activity.Ok(update)
    assert(newEndpointCalls == newAddrs.size)
    assert(newBalancerCalls == 0)
    val expected = newAddrs.map {
      case WeightedAddress(addr, _) => AddressFactory(addr)
    } + AddressFactory(Address(existingWeight.toInt))
    assert(balancers.count { _.endpoints.sample() == expected } == 1)

    // change weight class for an existing endpoint
    resetCounters()
    val updated = Set(WeightedAddress(Address(8), 20.0))
    val updatedSet = init ++ updated
    assert(updatedSet.size > init.size)
    dest() = Activity.Ok(updatedSet)
    assert(newBalancerCalls == 1)
    assert(newEndpointCalls == 0)
    assert(balancers.count {
      _.endpoints.sample() == updated.map { case WeightedAddress(addr, _) => AddressFactory(addr) }
    } == 1)
  })

  test("respect lazy eviction")(new Ctx {
    val init: Set[Address] = (1 to 5).map(Address(_)).toSet
    val dest = Var(Activity.Ok(init))

    var endpointStatus: Status = Status.Open
    override def newEndpoint(addr: Address) = {
      newEndpointCalls += 1
      new AddressFactory(addr) {
        override def status = endpointStatus
      }
    }

    newDist(dest, eagerEviction = false, autoPrime = true)

    assert(newEndpointCalls == init.size)
    assert(newBalancerCalls == 1)

    val update: Set[Address] = Set(6, 7, 8).map(Address(_))

    resetCounters()
    dest() = Activity.Ok(update)
    assert(newEndpointCalls == update.size)
    assert(newBalancerCalls == 0)
    val stale = (init ++ update).map(AddressFactory)
    assert(balancers.head.endpoints.sample() == stale)

    for (_ <- 0 until 100) {
      resetCounters()
      dest() = Activity.Ok(update)
      assert(newEndpointCalls == 0)
      assert(newBalancerCalls == 0)
      assert(balancers.head.endpoints.sample() == stale)
    }

    // We add an endpoint as the TrafficDistributor dedups
    val nextUpdate = update + Address(9)
    resetCounters()
    endpointStatus = Status.Busy
    dest() = Activity.Ok(nextUpdate)
    assert(newEndpointCalls == 1)
    assert(newBalancerCalls == 0)
    assert(balancers.head.endpoints.sample() == nextUpdate.map(AddressFactory))
  })

  test("transitions between activity states")(new Ctx {
    val init: Activity.State[Set[Address]] = Activity.Pending
    val dest = Var(init)
    val dist = newDist(dest)

    // queue on initial `Pending`
    val q = Future.select(for (_ <- 0 to 100) yield dist())
    assert(!q.isDefined)
    dest() = Activity.Ok(Set(1).map(Address(_)))
    val (first, _) = await(q)
    assert(first.isReturn)
    assert(
      balancers.head.endpoints.sample() ==
        Set(1).map(Address(_)).map(AddressFactory)
    )

    // initial resolution
    val resolved: Set[Address] = Set(1, 2, 3).map(Address(_))
    dest() = Activity.Ok(resolved)
    val bal0 = await(dist())
    assert(await(bal0(10)) == 10)
    assert(balancers.head.endpoints.sample() == resolved.map(AddressFactory))

    // subsequent `Pending` will propagate stale state
    dest() = Activity.Pending
    val bal1 = await(dist())
    assert(await(bal1(10)) == 10)
    assert(balancers.head.endpoints.sample() == resolved.map(AddressFactory))

    // subsequent `Failed` will propagate stale state
    val exc = new Exception("failed activity")
    dest() = Activity.Failed(exc)
    val bal2 = await(dist())
    assert(await(bal2(10)) == 10)
    assert(balancers.head.endpoints.sample() == resolved.map(AddressFactory))
  })

  test("transitions to failure if failure comes first")(new Ctx {
    val init: Activity.State[Set[Address]] = Activity.Pending
    val dest = Var(init)
    val dist = newDist(dest)

    // Failure is only allowed as an initial state
    dest() = Activity.Failed(new Exception)
    intercept[Exception] { await(dist()) }

    // now give it a good value and then make sure that
    // failed never comes back.
    dest() = Activity.Ok(Set(1).map(Address(_)))
    await(dist())

    dest() = Activity.Failed(new Exception)
    await(dist())
  })

  test("status is bestOf all weight classes")(new Ctx {
    val weightClasses = Seq((1.0, 1), (busyWeight, 2))
    val classes = weightClasses.flatMap(weightClass.tupled).toSet
    val dest = Var(Activity.Ok(classes))

    def mkBalancer(
      set: Activity[Set[EndpointFactory[Int, Int]]],
      disableEagerConnections: Boolean
    ): ServiceFactory[Int, Int] = {
      defaultBalancerFactory.newBalancer(
        set.map(_.toVector),
        new NoBrokersAvailableException("test"),
        Stack.Params.empty
      )
    }

    val endpoints = TrafficDistributor.weightEndpoints(
      Activity(dest),
      newEndpoint,
      true
    )

    val dist = new TrafficDistributor[Int, Int](
      dest = endpoints,
      newBalancer = mkBalancer,
      statsReceiver = NullStatsReceiver,
      rng = Rng("seed".hashCode)
    )

    assert(dist.status == Status.Open)
  })

  if (!sys.props.contains("SKIP_FLAKY"))
    test("increment weights on a shard") {
      val server = StringServer.server.serve(
        ":*",
        Service.mk { r: String =>
          Future.value(r.reverse)
        })
      val sr = new CumulativeGaugeInMemoryStatsReceiver()
      val addr = Address(server.boundAddress.asInstanceOf[InetSocketAddress])
      val va = Var[Addr](Addr.Bound(addr))
      val client = StringClient.client
        .configured(param.Stats(sr))
        .newClient(Name.Bound.singleton(va), "test")
        .toService

      // step this socket address through weight classes. Previous weight
      // classes are closed during each step. This is similar to how we
      // redline a shard.
      val N = 5
      for (i <- 1 to N) withClue(s"for i=$i:") {
        val addr =
          WeightedAddress(Address(server.boundAddress.asInstanceOf[InetSocketAddress]), i.toDouble)
        va() = Addr.Bound(addr)
        assert(await(client("hello")) == "hello".reverse)
        assert(sr.counters(Seq("test", "requests")) == i)
        assert(sr.counters(Seq("test", "connects")) == 1)
        // each WC gets a new balancer which adds the addr
        assert(sr.counters(Seq("test", "loadbalancer", "adds")) == i)
        assert(sr.counters(Seq("test", "loadbalancer", "removes")) == i - 1)
        assert(sr.gauges(Seq("test", "loadbalancer", "meanweight"))() == i)
        assert(sr.numGauges(Seq("test", "loadbalancer", "meanweight")) == 1)
        assert(sr.counters(Seq("test", "closes")) == 0)
      }

      va() = Addr.Bound(Set.empty[Address])
      assert(sr.counters(Seq("test", "closes")) == 1)
      assert(sr.counters(Seq("test", "loadbalancer", "adds")) == N)
      assert(sr.counters(Seq("test", "loadbalancer", "removes")) == N)
      assert(sr.gauges(Seq("test", "loadbalancer", "meanweight"))() == 0)
      assert(sr.numGauges(Seq("test", "loadbalancer", "meanweight")) == 1)

      // the TrafficDistributor /may/ close the cached Balancer which
      // holds a reference to the gauge, thus allowing the gauge to be gc-ed.
      sr.gauges.get(Seq("test", "loadbalancer", "size")) match {
        case Some(gauge) => assert(gauge() == 0)
        case None => // it was GC-ed, this is ok too
      }
      assert(sr.numGauges(Seq("test", "loadbalancer", "size")) <= 1)
    }

  test("close a client") {
    val server =
      StringServer.server.serve(":*", Service.mk { r: String => Future.value(r.reverse) })
    val sr = new InMemoryStatsReceiver
    val addr = Address(server.boundAddress.asInstanceOf[InetSocketAddress])
    val va = Var[Addr](Addr.Bound(addr))
    val client = StringClient.client
      .configured(param.Stats(sr))
      .newClient(Name.Bound.singleton(va), "test")
      .toService

    assert(await(client("hello")) == "hello".reverse)
    Await.ready(client.close())
    intercept[ServiceClosedException] { await(client("x")) }
  }

  test("transition to empty balancer on empty address state")(new Ctx {
    val init: Activity.State[Set[Address]] = Activity.Pending
    val dest = Var(init)
    val sr = new InMemoryStatsReceiver
    val dist = newDist(dest, statsReceiver = sr)

    dest() =
      Activity.Ok(Set((1, 2.0), (2, 3.0), (3, 2.0)).map(x => WeightedAddress(Address(x._1), x._2)))
    val bal0 = await(dist())
    assert(numWeightClasses(sr) == 2)

    dest() = Activity.Ok(Set.empty[Address])
    val ex = intercept[NoBrokersAvailableException] { await(dist()) }
    assert(numWeightClasses(sr) == 1)
    assert(closeBalancerCalls == 2)

    // Update 1.0 which is the "empty" balancer index. An update means that we
    // reused the "empty" balancer, tested by the closeBalancerCalls assertion.
    dest() = Activity.Ok(weightClass(1.0, 10))
    val bal1 = await(dist())
    assert(numWeightClasses(sr) == 1)
    assert(closeBalancerCalls == 2)
  })

  test("ensure empty load balancer is closed after address set updates")(new Ctx {
    val init: Activity.State[Set[Address]] = Activity.Pending
    val dest = Var(init)
    val sr = new InMemoryStatsReceiver
    val dist = newDist(dest, statsReceiver = sr)

    dest() =
      Activity.Ok(Set((1, 2.0), (2, 1.0), (3, 2.0)).map(x => WeightedAddress(Address(x._1), x._2)))
    val bal0 = await(dist())
    assert(numWeightClasses(sr) == 2)

    dest() = Activity.Ok(Set.empty[Address])
    val ex = intercept[NoBrokersAvailableException] { await(dist()) }
    assert(numWeightClasses(sr) == 1)

    dest() = Activity.Ok(Set((1, 1.0)).map(x => WeightedAddress(Address(x._1), x._2)))
    val bal2 = await(dist())

    val emptyBalancer = balancers.find(b => b.numOfEndpoints == 0)
    emptyBalancer match {
      case Some(bal) => assert(bal.balancerIsClosed)
      case _ => fail("Empty balancer does not exist")
    }
  })

  test("stream of empty updates returns empty exception")(new Ctx {
    val init: Activity.State[Set[Address]] = Activity.Pending
    val dest = Var(init)
    val sr = new InMemoryStatsReceiver
    val dist = newDist(dest, statsReceiver = sr)
    for (i <- 0 to 2) {
      dest() = Activity.Ok(Set.empty[Address])
      intercept[NoBrokersAvailableException] { await(dist()) }
    }
    // numWeightClasses is the number of balancers emitted by the partition.
    assert(numWeightClasses(sr) == 1.toFloat)

    // The TrafficDistributor ignores the subsequent duplicate updates with
    // the empty endpoint sets. The same balancer is reused.
    assert(newBalancerCalls == 1)
    assert(closeBalancerCalls == 0)

    // Finally, we give the distributor an endpoint. The balancer created from
    // the stream of empty updates is closed.
    dest() = Activity.Ok(weightClass(2.0, 100))
    await(dist())

    // Verify that there's still one balancer in the distribution, because we
    // close the "empty" balancer.
    assert(numWeightClasses(sr) == 1.toFloat)
    assert(newBalancerCalls == 2)
    assert(closeBalancerCalls == 1)
  })

  trait UpdatePartitionMapCtx {
    var closeIsCalled = 0
    case class Partition(value: Var[Set[Int]] with Updatable[Set[Int]]) extends Closable {
      def close(deadline: Time): Future[Unit] = {
        closeIsCalled += 1
        Future.Done
      }
    }

    var removed = 0
    var added = 0
    var updated = 0
    val partitionDiffOps = new DiffOps[Int, Partition] {
      def remove(partition: Partition): Unit = {
        removed += 1
        partition.close()
      }
      def add(items: Set[Int]): Partition = {
        added += 1
        Partition(Var(items))
      }
      def update(items: Set[Int], partition: Partition): Partition = {
        updated += 1
        partition.value.update(items)
        partition
      }
    }

    def getPartitionKey(i: Int) = Seq(i % 3)
  }

  test("updatePartitionMap - add new partitions") {
    new UpdatePartitionMapCtx {
      val current = Set(1, 2, 3, 4, 5, 6)
      val init = Map.empty[Int, Partition]

      val result =
        AddrLifecycle.updatePartitionMap(init, current, getPartitionKey, partitionDiffOps)
      assert(result(0).value.sample() == Set(3, 6))
      assert(result(1).value.sample() == Set(1, 4))
      assert(result(2).value.sample() == Set(2, 5))
    }
  }

  test("updatePartitionMap - remove a partitions") {
    new UpdatePartitionMapCtx {
      val init = Map(
        0 -> Partition(Var(Set(3, 6))),
        1 -> Partition(Var(Set(1, 4))),
        2 -> Partition(Var(Set(2, 5))))

      // remove a partition
      val current1 = Set(1, 3, 4)
      val result =
        AddrLifecycle.updatePartitionMap(init, current1, getPartitionKey, partitionDiffOps)
      assert(result(0).value.sample() == Set(3))
      assert(result(1).value.sample() == Set(1, 4))
      assert(result.get(2) == None)
      assert(closeIsCalled == 1)
      assert(removed == 1)
      assert(updated == 2)
    }
  }

  test("updatePartitionMap - update") {
    new UpdatePartitionMapCtx {
      val init = Map(
        0 -> Partition(Var(Set(3, 6))),
        1 -> Partition(Var(Set(1, 4))),
        2 -> Partition(Var(Set(2, 5))))
      val current1 = Set(7, 2, 3, 4, 5, 6)

      val result1 =
        AddrLifecycle.updatePartitionMap(init, current1, getPartitionKey, partitionDiffOps)
      assert(result1(0).value.sample() == Set(3, 6))
      assert(result1(1).value.sample() == Set(4, 7))
      assert(result1(2).value.sample() == Set(2, 5))
      assert(closeIsCalled == 0)

      val current2 = Set(7, 8, 3, 4, 11, 6)
      val result2 =
        AddrLifecycle.updatePartitionMap(result1, current2, getPartitionKey, partitionDiffOps)
      assert(result2(0).value.sample() == Set(3, 6))
      assert(result2(1).value.sample() == Set(4, 7))
      assert(result2(2).value.sample() == Set(8, 11))
      assert(closeIsCalled == 0)
    }
  }
  test("updatePartitionMap - no addition and removal") {
    new UpdatePartitionMapCtx {
      val init = Map(
        0 -> Partition(Var(Set(3, 6))),
        1 -> Partition(Var(Set(1, 4))),
        2 -> Partition(Var(Set(2, 5))))
      val current = Set(1, 2, 3, 4, 5, 6)

      val result =
        AddrLifecycle.updatePartitionMap(init, current, getPartitionKey, partitionDiffOps)
      assert(result(0).value.sample() == Set(3, 6))
      assert(result(1).value.sample() == Set(1, 4))
      assert(result(2).value.sample() == Set(2, 5))
      assert(removed == 0)
      assert(added == 0)
      assert(updated == 3)
    }
  }
  test("updatePartitionMap - a combination of 3 operations") {
    new UpdatePartitionMapCtx {
      val init = Map(
        0 -> Partition(Var(Set(3, 6))),
        1 -> Partition(Var(Set(1, 4))),
        2 -> Partition(Var(Set(2, 5))))

      val current1 = Set(1, 2, 4, 7)
      val result1 =
        AddrLifecycle.updatePartitionMap(init, current1, getPartitionKey, partitionDiffOps)
      assert(result1.get(0) == None)
      assert(closeIsCalled == 1)
      assert(result1(1).value.sample() == Set(1, 4, 7))
      assert(result1(2).value.sample() == Set(2))
      assert(removed == 1)
      assert(added == 0)
      assert(updated == 2)

      removed = 0
      added = 0
      updated = 0
      closeIsCalled = 0

      val current2 = Set(2, 5, 9, 12)
      val result2 =
        AddrLifecycle.updatePartitionMap(result1, current2, getPartitionKey, partitionDiffOps)
      assert(result2(0).value.sample() == Set(9, 12))
      assert(result2.get(1) == None)
      assert(result2(2).value.sample() == Set(2, 5))
      assert(removed == 1)
      assert(added == 1)
      assert(updated == 1)
      assert(closeIsCalled == 1)
    }
  }

  test("weightEndpoints splits addresses based on weight and address") {
    new Ctx {
      val weightClasses = Seq((1.0, 1), (busyWeight, 2))
      val classes = weightClasses.flatMap(weightClass.tupled).toSet
      val dest = Var(Activity.Ok(classes))

      val evt = TrafficDistributor.weightEndpoints(Activity(dest), newEndpoint, false)
      val ref =
        new AtomicReference[Activity.State[Set[AddressedFactory[Int, Int]]]]()
      val closable = evt.register(Witness(ref))

      ref.get match {
        case Activity.Ok(set) =>
          assert(set.size == 3)
          assert(set.count(_.weight == 1.0) == 1)
          assert(set.count(_.weight == busyWeight) == 2)
        case _ => fail
      }

      closable.close()
    }
  }

  test("weightEndpoints handles additions gracefully") {
    new Ctx {
      val weightClasses = Seq((1.0, 1), (busyWeight, 2))
      val classes = weightClasses.flatMap(weightClass.tupled).toSet
      val dest = Var(Activity.Ok(classes))

      val evt = TrafficDistributor.weightEndpoints(Activity(dest), newEndpoint, false)
      val ref =
        new AtomicReference[Activity.State[Set[AddressedFactory[Int, Int]]]]()
      val closable = evt.register(Witness(ref))

      ref.get match {
        case Activity.Ok(set) =>
          assert(set.size == 3)
          assert(set.count(_.weight == 1.0) == 1)
          assert(set.count(_.weight == busyWeight) == 2)
        case _ => fail
      }

      // adds a new address with a weight we've seen before
      dest() = Activity.Ok(classes + WeightedAddress(WeightedTestAddr(1, 1), 1))

      ref.get match {
        case Activity.Ok(set) =>
          assert(set.size == 4)
          assert(set.count(_.weight == 1.0) == 2)
          assert(set.count(_.weight == busyWeight) == 2)
        case _ => fail
      }

      closable.close()
    }
  }

  test("weightEndpoints handles removals gracefully if they're not open") {
    new Ctx {
      val weightClass1 = weightClass(1.0, 1)
      val weightClass2 = weightClass(busyWeight, 2)
      val dest = Var(Activity.Ok(weightClass1 ++ weightClass2))

      val evt = TrafficDistributor.weightEndpoints(Activity(dest), newEndpoint, false)
      val ref =
        new AtomicReference[Activity.State[Set[AddressedFactory[Int, Int]]]]()
      val closable = evt.register(Witness(ref))

      ref.get match {
        case Activity.Ok(set) =>
          assert(set.size == 3)
          assert(set.count(_.weight == 1.0) == 1)
          assert(set.count(_.weight == busyWeight) == 2)
          // and now we open them up so that they transition to a state where we can examine
          // their statuses. weightEndpoints will only close factories that do not have
          // status `Open`, unless you enable eagerEvictions
          await(Future.join(set.map {
            case AddressedFactory(factory, _) =>
              factory().flatMap(_.close())
          }.toSeq))
        case _ => fail
      }

      // adds a new address with a weight we've seen before
      dest() = Activity.Ok(weightClass1)

      ref.get match {
        case Activity.Ok(set) =>
          assert(set.size == 1)
          assert(set.count(_.weight == 1.0) == 1)
        case _ => fail
      }

      closable.close()
    }
  }

  test(
    "weightEndpoints doesn't remove addresses while they're still open (if eagerEvictions disabled)") {
    new Ctx {
      val weightClass1 = weightClass(1.0, 1)
      val weightClass2 = weightClass(busyWeight, 2)
      val dest = Var(Activity.Ok(weightClass1 ++ weightClass2))

      val evt = TrafficDistributor.weightEndpoints(Activity(dest), newEndpoint, false)
      val ref =
        new AtomicReference[Activity.State[Set[AddressedFactory[Int, Int]]]]()
      val closable = evt.register(Witness(ref))

      ref.get match {
        case Activity.Ok(set) =>
          assert(set.size == 3)
          assert(set.count(_.weight == 1.0) == 1)
          assert(set.count(_.weight == busyWeight) == 2)
        case _ => fail
      }

      // adds a new address with a weight we've seen before
      dest() = Activity.Ok(weightClass2)

      ref.get match {
        case Activity.Ok(set) =>
          assert(set.size == 3)
          assert(set.count(_.weight == 1.0) == 1)
          assert(set.count(_.weight == busyWeight) == 2)
        case _ => fail
      }

      closable.close()
    }
  }

  test(
    "weightEndpoints removes addresses gracefully while they're still open (if eagerEvictions enabled)") {
    new Ctx {
      val weightClass1 = weightClass(1.0, 1)
      val weightClass2 = weightClass(busyWeight, 2)
      val dest = Var(Activity.Ok(weightClass1 ++ weightClass2))

      val evt = TrafficDistributor.weightEndpoints(Activity(dest), newEndpoint, true)
      val ref =
        new AtomicReference[Activity.State[Set[AddressedFactory[Int, Int]]]]()
      val closable = evt.register(Witness(ref))

      ref.get match {
        case Activity.Ok(set) =>
          assert(set.size == 3)
          assert(set.count(_.weight == 1.0) == 1)
          assert(set.count(_.weight == busyWeight) == 2)
        case _ => fail
      }

      // adds a new address with a weight we've seen before
      dest() = Activity.Ok(weightClass2)

      ref.get match {
        case Activity.Ok(set) =>
          assert(set.size == 2)
          assert(set.count(_.weight == busyWeight) == 2)
        case _ => fail
      }

      closable.close()
    }
  }

  test("does not eagerly connect to endpoints in a balancer with a non 1.0 weight class") {
    new Ctx {
      val weightClass1 = weightClass(1.0, 1)
      val weightClass2 = weightClass(1.1, 2)
      val dest = Var(Activity.Ok(weightClass1 ++ weightClass2))
      newDist(dest)

      // we want to populate `numOfEndpoints`
      balancers.foreach(bal => bal())
      val non1Bal = balancers.find(_.numOfEndpoints == 2)
      assert(non1Bal.get.disableEagerConnections == true)

      val normalBal = balancers.find(_.numOfEndpoints == 1)
      assert(normalBal.get.disableEagerConnections == false)
    }
  }
}
