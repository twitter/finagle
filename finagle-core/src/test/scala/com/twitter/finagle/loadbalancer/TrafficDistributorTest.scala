package com.twitter.finagle.loadbalancer

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.addr.WeightedAddress
import com.twitter.finagle.client.utils.StringClient
import com.twitter.finagle.server.utils.StringServer
import com.twitter.finagle.stats._
import com.twitter.finagle.util.Rng
import com.twitter.util.{Function => _, _}
import java.net.InetSocketAddress
import org.scalatest.FunSuite

private object TrafficDistributorTest {
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
    (w, size) =>
      (0 until size).toSet.map { i: Int =>
        WeightedAddress(WeightedTestAddr(i, w), w)
    }

  val busyWeight = 2.0
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
    onClose: scala.Function0[Unit] = () => ())
      extends ServiceFactory[Int, Int] {
    var offeredLoad = 0
    var balancerIsClosed = false
    var numOfEndpoints = 0
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
    override def toString: String = s"Balancer($endpoints)"
  }

  /** Return the distribution for the the given `balancer` as a tuple
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
    def newBalancer(eps: Activity[Set[EndpointFactory[Int, Int]]]): ServiceFactory[Int, Int] = {
      newBalancerCalls += 1
      // eagerly establish the lazy endpoints and extract the
      // underlying `AddressFactory`
      val addressFactories = eps.map { set =>
        set.map { epsf =>
          Await.result(epsf())
          epsf.asInstanceOf[LazyEndpointFactory[Int, Int]].self.get.asInstanceOf[AddressFactory]
        }
      }
      val b = Balancer(addressFactories, () => closeBalancerCalls += 1)
      balancers += b
      b
    }

    def newDist(
      dest: Var[Activity.State[Set[Address]]],
      eagerEviction: Boolean = true,
      statsReceiver: StatsReceiver = NullStatsReceiver,
      autoPrime: Boolean = false
    ): TrafficDistributor[Int, Int] = {
      val dist = new TrafficDistributor[Int, Int](
        dest = Activity(dest),
        newEndpoint = newEndpoint,
        newBalancer = newBalancer,
        eagerEviction = eagerEviction,
        statsReceiver = statsReceiver,
        rng = Rng("seed".hashCode)
      )

      if (autoPrime) {
        // Primes the distributor such that it creates its underlying
        // resources (i.e. endpoint stacks). Note, do NOT block here
        // since this is a circular dependency. Instead, we can rely
        // on the fact that we are serialized since the same thread
        // that updates `dest` will call this closure as well.
        dest.changes.respond { _ =>
          for (_ <- 0 to 100) { dist() }
        }
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
    override def counter(verbosity: Verbosity, name: String*): ReadableCounter =
      underlying.counter(verbosity, name: _*)
    override def stat(verbosity: Verbosity, name: String*): ReadableStat =
      underlying.stat(verbosity, name: _*)

    protected[this] def registerGauge(verbosity: Verbosity, name: Seq[String], f: => Float): Unit =
      underlying.addGauge(name: _*)(f)

    protected[this] def deregisterGauge(name: Seq[String]): Unit =
      underlying.gauges -= name

    def counters: Map[Seq[String], Long] = underlying.counters.toMap
    def stats: Map[Seq[String], Seq[Float]] = underlying.stats.toMap
    def gauges: Map[Seq[String], () => Float] = underlying.gauges.toMap

    def numGauges(name: Seq[String]): Int =
      numUnderlying(name: _*)
  }
}

class TrafficDistributorTest extends FunSuite {
  import TrafficDistributorTest._

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

      result.foreach {case ((w, s, l)) => assert(math.abs( (l / w) - baseline) <= baseline * ε)}
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
    val init: Set[Address] = (1 to 5).map { i =>
      WeightedAddress(Address(i), i)
    }.toSet
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

    resetCounters()
    endpointStatus = Status.Busy
    dest() = Activity.Ok(update)
    assert(newEndpointCalls == 0)
    assert(newBalancerCalls == 0)
    assert(balancers.head.endpoints.sample() == update.map(AddressFactory))
  })

  test("transitions between activity states")(new Ctx {
    val init: Activity.State[Set[Address]] = Activity.Pending
    val dest = Var(init)
    val dist = newDist(dest)

    // queue on initial `Pending`
    val q = Future.select(for (_ <- 0 to 100) yield dist())
    assert(!q.isDefined)
    dest() = Activity.Ok(Set(1).map(Address(_)))
    val (first, _) = Await.result(q, 1.second)
    assert(first.isReturn)
    assert(
      balancers.head.endpoints.sample() ==
        Set(1).map(Address(_)).map(AddressFactory)
    )

    // initial resolution
    val resolved: Set[Address] = Set(1, 2, 3).map(Address(_))
    dest() = Activity.Ok(resolved)
    val bal0 = Await.result(dist())
    assert(Await.result(bal0(10)) == 10)
    assert(balancers.head.endpoints.sample() == resolved.map(AddressFactory))

    // subsequent `Pending` will propagate stale state
    dest() = Activity.Pending
    val bal1 = Await.result(dist())
    assert(Await.result(bal1(10)) == 10)
    assert(balancers.head.endpoints.sample() == resolved.map(AddressFactory))

    // subsequent `Failed` will propagate stale state
    val exc = new Exception("failed activity")
    dest() = Activity.Failed(exc)
    val bal2 = Await.result(dist())
    assert(Await.result(bal2(10)) == 10)
    assert(balancers.head.endpoints.sample() == resolved.map(AddressFactory))
  })

  test("transitions to failure if failure comes first")(new Ctx {
    val init: Activity.State[Set[Address]] = Activity.Pending
    val dest = Var(init)
    val dist = newDist(dest)

    // Failure is only allowed as an initial state
    dest() = Activity.Failed(new Exception)
    intercept[Exception] { Await.result(dist()) }

    // now give it a good value and then make sure that
    // failed never comes back.
    dest() = Activity.Ok(Set(1).map(Address(_)))
    Await.result(dist())

    dest() = Activity.Failed(new Exception)
    Await.result(dist())
  })

  test("status is bestOf all weight classes")(new Ctx {
    val weightClasses = Seq((1.0, 1), (busyWeight, 2))
    val classes = weightClasses.flatMap(weightClass.tupled).toSet
    val dest = Var(Activity.Ok(classes))

    def mkBalancer(set: Activity[Set[EndpointFactory[Int, Int]]]): ServiceFactory[Int, Int] = {
      defaultBalancerFactory.newBalancer(
        set.map(_.toVector),
        new NoBrokersAvailableException("test"),
        Stack.Params.empty
      )
    }

    val dist = new TrafficDistributor[Int, Int](
      dest = Activity(dest),
      newEndpoint = newEndpoint,
      newBalancer = mkBalancer,
      eagerEviction = true,
      statsReceiver = NullStatsReceiver,
      rng = Rng("seed".hashCode)
    )

    assert(dist.status == Status.Open)
  })

  if (!sys.props.contains("SKIP_FLAKY"))
    test("increment weights on a shard") {
      val server = StringServer.server.serve(":*", Service.mk { r: String =>
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
        assert(Await.result(client("hello")) == "hello".reverse)
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
    val server = StringServer.server.serve(":*", Service.mk { r: String =>
      Future.value(r.reverse)
    })
    val sr = new InMemoryStatsReceiver
    val addr = Address(server.boundAddress.asInstanceOf[InetSocketAddress])
    val va = Var[Addr](Addr.Bound(addr))
    val client = StringClient.client
      .configured(param.Stats(sr))
      .newClient(Name.Bound.singleton(va), "test")
      .toService

    assert(Await.result(client("hello")) == "hello".reverse)
    Await.ready(client.close())
    intercept[ServiceClosedException] { Await.result(client("x")) }
  }

  test("transition to empty balancer on empty address state")(new Ctx {
    val init: Activity.State[Set[Address]] = Activity.Pending
    val dest = Var(init)
    val sr = new InMemoryStatsReceiver
    val dist = newDist(dest, statsReceiver = sr)

    dest() =
      Activity.Ok(Set((1, 2.0), (2, 3.0), (3, 2.0)).map(x => WeightedAddress(Address(x._1), x._2)))
    val bal0 = Await.result(dist())
    assert(numWeightClasses(sr) == 2)

    dest() = Activity.Ok(Set.empty[Address])
    val ex = intercept[NoBrokersAvailableException] { Await.result(dist()) }
    assert(numWeightClasses(sr) == 1)
    assert(closeBalancerCalls == 2)

    // Update 1.0 which is the "empty" balancer index. An update means that we
    // reused the "empty" balancer, tested by the closeBalancerCalls assertion.
    dest() = Activity.Ok(weightClass(1.0, 10))
    val bal1 = Await.result(dist())
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
    val bal0 = Await.result(dist())
    assert(numWeightClasses(sr) == 2)

    dest() = Activity.Ok(Set.empty[Address])
    val ex = intercept[NoBrokersAvailableException] { Await.result(dist()) }
    assert(numWeightClasses(sr) == 1)

    dest() = Activity.Ok(Set((1, 1.0)).map(x => WeightedAddress(Address(x._1), x._2)))
    val bal2 = Await.result(dist())

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
      intercept[NoBrokersAvailableException] { Await.result(dist()) }
    }
    // numWeightClasses is the number of balancers emitted by the partition.
    assert(numWeightClasses(sr) == 1.toFloat)

    // New balancer created for each update with an empty endpoint set. Each
    // time we create a new balancer, we close the previous balancer. The
    // assert on closeBalancerCalls would fail if we created the "empty"
    // balancer outside of the scanLeft.
    assert(newBalancerCalls == 3)
    assert(closeBalancerCalls == 2)

    // Finally, we give the distributor an endpoint.
    dest() = Activity.Ok(weightClass(2.0, 100))
    Await.result(dist())

    // Verify that there's still one balancer in the distribution, because we
    // close the "empty" balancer.
    assert(numWeightClasses(sr) == 1.toFloat)
    assert(newBalancerCalls == 4)
    assert(closeBalancerCalls == 3)
  })

}
