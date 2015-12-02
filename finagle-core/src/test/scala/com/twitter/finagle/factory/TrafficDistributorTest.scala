package com.twitter.finagle.factory

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.client.StringClient
import com.twitter.finagle.server.StringServer
import com.twitter.finagle.stats._
import com.twitter.finagle.util.Rng
import com.twitter.util.{Function => _, _}
import java.net.SocketAddress
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

private object TrafficDistributorTest {
  case class TestAddr(id: Int) extends SocketAddress

  // The distributor is not privy to this wrapped socket address and
  // it allows us to retrieve the weight class.
  case class WeightedTestAddr(id: Int, weight: Double) extends SocketAddress

  val weightClass: (Double, Int) => Set[SocketAddress] =
    (w, size) => (0 until size).toSet.map { i: Int =>
      WeightedSocketAddress(WeightedTestAddr(i, w), w)
    }

  case class SocketAddrFactory(addr: SocketAddress) extends ServiceFactory[Int, Int] {
    def apply(conn: ClientConnection) = Future.value(Service.mk(i => Future.value(i)))
    def close(deadline: Time) = Future.Done
    override def toString = s"SocketAddrFactory($addr)"
  }

  case class Balancer(endpoints: Activity[Set[ServiceFactory[Int, Int]]])
    extends ServiceFactory[Int, Int] {
      var offeredLoad = 0
      def apply(conn: ClientConnection) = {
        offeredLoad += 1
        // new hotness in load balancing
        val nodes = endpoints.sample().toSeq
        if (nodes.isEmpty) Future.exception(new NoBrokersAvailableException)
        else nodes((math.random * nodes.size).toInt)(conn)
      }
      def close(deadline:Time) = Future.Done
      override def toString = s"Balancer($endpoints)"
    }

    // Return the distribution for the the given `balancer` as a tuple
    // of (weight, size, offer load).
    def distribution(balancers: Set[Balancer]): Set[(Double, Int, Int)] = {
      balancers.flatMap { b =>
        val endpoints = b.endpoints.sample()
        endpoints.map {
          case s: ServiceFactoryProxy[_, _] => s.self match {
            case SocketAddrFactory(WeightedTestAddr(_, w)) =>
              (w*endpoints.size, endpoints.size, b.offeredLoad)
          }
        }
      }
    }

  class Ctx {
    // var endpointStatus: Status = Status.Open
    var newEndpointCalls = 0
    def newEndpoint(sa: SocketAddress): ServiceFactory[Int, Int] = {
      newEndpointCalls += 1
      SocketAddrFactory(sa)
        // override def status = endpointStatus
    }

    var newBalancerCalls = 0
    var balancers: Set[Balancer] = Set.empty
    def newBalancer(eps: Activity[Set[ServiceFactory[Int, Int]]]): ServiceFactory[Int, Int] = {
      newBalancerCalls += 1
      val b = Balancer(eps)
      balancers += b
      b
    }

    def newDist(
      dest: Var[Activity.State[Set[SocketAddress]]],
      eagerEviction: Boolean = true,
      statsReceiver: StatsReceiver = NullStatsReceiver
    ): ServiceFactory[Int, Int] = {
      new TrafficDistributor[Int, Int](
        dest = Activity(dest),
        newEndpoint = newEndpoint,
        newBalancer = newBalancer,
        eagerEviction = eagerEviction,
        statsReceiver = statsReceiver,
        rng = Rng("seed".hashCode)
      )
    }

    def resetCounters() {
      newEndpointCalls = 0
      newBalancerCalls = 0
    }
  }
}

@RunWith(classOf[JUnitRunner])
class TrafficDistributorTest extends FunSuite {
  import TrafficDistributorTest._

  test("distributes when weights are uniform") (new Ctx {
    val init: Set[SocketAddress] = weightClass(5.0, 100)
    val dest = Var(Activity.Ok(init))
    val sr = new InMemoryStatsReceiver
    val dist = newDist(dest, statsReceiver = sr)

    val R = 100
    for (_ <- 0 until R) dist()
    assert(balancers.size == 1)
    assert(balancers.head.offeredLoad == R)
    assert(sr.gauges(Seq("meanweight"))() == 5.0)
  })

  test("distributes according to non-uniform weights") (new Ctx {
    val R = 10 * 10 * 1000

    locally {
      val ε: Double = 0.01
      val weightClasses = Seq((1.0, 1000), (2.0, 5), (10.0, 150))
      val classes = weightClasses.flatMap(weightClass.tupled).toSet
      val weightSum = weightClasses.foldLeft(0.0) { case (sum, tup) =>
        val (w, t) = tup
        sum + (w*t)
      }

      val dest = Var(Activity.Ok(classes))
      val dist = newDist(dest)
      for (_ <- 0 until R) dist()

      distribution(balancers).foreach {
        case ((w, _, l)) => assert(math.abs(w/weightSum - l/R.toDouble) < ε)
      }
    }

    locally {
      // This shows that weights can still be interpreted as multipliers for load relative
      // to other nodes. For example, a node with weight 2.0 should receive roughly twice
      // the traffic it would have normally received with weight 1.0. We say "roughly"
      // because this assumes a purely equitable distribution from the load balancers.
      // In practice, the lbs aren't purely equitable –- their distribution is impacted
      // by other feedback as well (e.g. latency, failure, etc.) -- but that should be okay
      // for most use-cases of weights.
      val ε: Double = .05 // 5%
      balancers = Set.empty
      val weightClasses = Seq((1.0, 500), (2.0, 1), (3.0, 1), (4.0, 1))
      val classes = weightClasses.flatMap(weightClass.tupled).toSet
      val dest = Var(Activity.Ok(classes))
      val dist = newDist(dest)
      for (_ <- 0 until R) dist()

      val result = distribution(balancers)

      val baseline = result.collect {
        case ((w, s, l)) if s/w == 1.0 => l/w
      }.head

      result.foreach {
        case ((w, _, l)) => assert(math.abs(l/w - baseline) <= baseline*ε)
      }
    }
  })

  test("memoize calls to newEndpoint and newBalancer") (new Ctx {
    val init: Set[SocketAddress] = (1 to 5).map(TestAddr).toSet
    val dest = Var(Activity.Ok(init))

    newDist(dest)

    assert(newEndpointCalls == init.size)
    assert(newBalancerCalls == 1)
    assert(balancers.size == 1)
    assert(balancers.head.endpoints.sample() == init.map(SocketAddrFactory))

    val update: Set[SocketAddress] = (3 to 10).map(TestAddr).toSet
    dest() = Activity.Ok(update)
    assert(newEndpointCalls != init.size + update.size)
    assert(newEndpointCalls == (init ++ update).size)
    assert(newBalancerCalls == 1)
    assert(balancers.head.endpoints.sample() == update.map(SocketAddrFactory))
  })

  test("partition endpoints into weight classes") (new Ctx {
    val init: Set[SocketAddress] = (1 to 5).map { i =>
      WeightedSocketAddress(TestAddr(i), i)
    }.toSet
    val dest = Var(Activity.Ok(init))

    newDist(dest)

    assert(newBalancerCalls == init.size)
    assert(balancers.size == init.size)
    assert(newEndpointCalls == init.size)

    // insert new endpoints on exisiting weight classes
    resetCounters()
    val existingWeight = 3.0
    val newAddrs = Set(
      WeightedSocketAddress(TestAddr(6), existingWeight),
      WeightedSocketAddress(TestAddr(7), existingWeight),
      WeightedSocketAddress(TestAddr(8), existingWeight)
    )
    val update: Set[SocketAddress] = init ++ newAddrs
    dest() = Activity.Ok(update)
    assert(newEndpointCalls == newAddrs.size)
    assert(newBalancerCalls == 0)
    val expected = newAddrs.map(_.addr).map(SocketAddrFactory) +
      SocketAddrFactory(TestAddr(existingWeight.toInt))
    assert(balancers.count { _.endpoints.sample() == expected } == 1)

    // change weight class for an existing endpoint
    resetCounters()
    val updated = Set(WeightedSocketAddress(TestAddr(8), 20.0))
    val updatedSet = init ++ updated
    assert(updatedSet.size > init.size)
    dest() = Activity.Ok(updatedSet)
    assert(newBalancerCalls == 1)
    assert(newEndpointCalls == 0)
    assert(balancers.count {
      _.endpoints.sample() == updated.map(_.addr).map(SocketAddrFactory)
    } == 1)
  })

  test("respect lazy eviction") (new Ctx {
    val init: Set[SocketAddress] = (1 to 5).map(TestAddr).toSet
    val dest = Var(Activity.Ok(init))

    var endpointStatus: Status = Status.Open
    override def newEndpoint(sa: SocketAddress) = {
      newEndpointCalls += 1
      new SocketAddrFactory(sa) {
        override def status = endpointStatus
      }
    }

    newDist(dest, eagerEviction = false)

    assert(newEndpointCalls == init.size)
    assert(newBalancerCalls == 1)

    val update: Set[SocketAddress] = Set(6,7,8).map(TestAddr)

    resetCounters()
    dest() = Activity.Ok(update)
    assert(newEndpointCalls == update.size)
    assert(newBalancerCalls == 0)
    val stale = (init ++ update).map(SocketAddrFactory)
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
    assert(balancers.head.endpoints.sample() == update.map(SocketAddrFactory))
  })

  test("transitions between activity states") (new Ctx {
    val init: Activity.State[Set[SocketAddress]] = Activity.Pending
    val dest = Var(init)
    val dist = newDist(dest)

    // queue on initial `Pending`
    val q = Future.select(for (_ <- 0 to 100) yield dist())
    assert(!q.isDefined)
    dest() = Activity.Ok(Set(1).map(TestAddr))
    val (first, _) = Await.result(q, 1.second)
    assert(first.isReturn)
    assert(balancers.head.endpoints.sample() ==
      Set(1).map(TestAddr).map(SocketAddrFactory))

    // initial resolution
    val resolved: Set[SocketAddress] = Set(1,2,3).map(TestAddr)
    dest() = Activity.Ok(resolved)
    val bal0 = Await.result(dist())
    assert(Await.result(bal0(10)) == 10)
    assert(balancers.head.endpoints.sample() == resolved.map(SocketAddrFactory))

    // subsequent `Pending` will propagate stale state
    dest() = Activity.Pending
    val bal1 = Await.result(dist())
    assert(Await.result(bal1(10)) == 10)
    assert(balancers.head.endpoints.sample() == resolved.map(SocketAddrFactory))

    // subsequent `Failed` will propagate stale state
    val exc = new Exception("failed activity")
    dest() = Activity.Failed(exc)
    val bal2 = Await.result(dist())
    assert(Await.result(bal2(10)) == 10)
    assert(balancers.head.endpoints.sample() == resolved.map(SocketAddrFactory))
  })

  test("transitions to failure if failure comes first") (new Ctx {
    val init: Activity.State[Set[SocketAddress]] = Activity.Pending
    val dest = Var(init)
    val dist = newDist(dest)

    // Failure is only allowed as an initial state
    dest() = Activity.Failed(new Exception)
    intercept[Exception] { Await.result(dist())}

    // now give it a good value and then make sure that
    // failed never comes back.
    dest() = Activity.Ok(Set(1).map(TestAddr))
    Await.result(dist())

    dest() = Activity.Failed(new Exception)
    Await.result(dist())
  })

  // todo: move this to util-stats?
  private class CumulativeGaugeInMemoryStatsReceiver
    extends StatsReceiverWithCumulativeGauges
  {
    private[this] val underlying = new InMemoryStatsReceiver()
    override val repr: AnyRef = this
    override def counter(name: String*): ReadableCounter = underlying.counter(name: _*)
    override def stat(name: String*): ReadableStat = underlying.stat(name: _*)

    protected[this] def registerGauge(name: Seq[String], f: => Float): Unit =
      underlying.addGauge(name: _*)(f)

    protected[this] def deregisterGauge(name: Seq[String]): Unit =
      underlying.gauges -= name

    def counters: Map[Seq[String], Int] = underlying.counters.toMap

    def stats: Map[Seq[String], Seq[Float]] = underlying.stats.toMap

    def gauges: Map[Seq[String], () => Float] = underlying.gauges.toMap

    def numGauges(name: Seq[String]): Int =
      numUnderlying(name: _*)
  }

  test("increment weights on a shard") (new StringClient with StringServer {
    val server = stringServer.serve(":*", Service.mk { r: String =>
      Future.value(r.reverse)
    })
    val sr = new CumulativeGaugeInMemoryStatsReceiver()
    val va = Var[Addr](Addr.Bound(Set(server.boundAddress)))
    val client = stringClient
      .configured(param.Stats(sr))
      .newClient(Name.Bound.singleton(va), "test")
      .toService

    // step this socket address through weight classes. Previous weight
    // classes are closed during each step. This is similar to how we
    // redline a shard.
    for (i <- 1 to 10) withClue(s"for i=$i:") {
      va() = Addr.Bound(WeightedSocketAddress(server.boundAddress, i.toDouble))
      assert(Await.result(client("hello")) == "hello".reverse)
      assert(sr.counters(Seq("test", "requests")) == i)
      assert(sr.counters(Seq("test", "connects")) == 1)
      // each WC gets a new balancer which adds the addr
      assert(sr.counters(Seq("test", "loadbalancer", "adds")) == i)
      assert(sr.counters(Seq("test", "loadbalancer", "removes")) == i - 1)
      assert(sr.gauges(Seq("test", "loadbalancer", "meanweight"))() == i)
      assert(sr.numGauges(Seq("test", "loadbalancer", "meanweight")) == 1)
      assert(sr.counters.get(Seq("test", "closes")).isEmpty)
    }

    va() = Addr.Bound(Set.empty[SocketAddress])
    assert(sr.counters(Seq("test", "closes")) == 1)
    assert(sr.counters(Seq("test", "loadbalancer", "adds")) == 10)
    assert(sr.counters(Seq("test", "loadbalancer", "removes")) == 10)
    assert(sr.gauges(Seq("test", "loadbalancer", "size"))() == 0)
    assert(sr.numGauges(Seq("test", "loadbalancer", "size")) == 1)
    assert(sr.gauges(Seq("test", "loadbalancer", "meanweight"))() == 0)
    assert(sr.numGauges(Seq("test", "loadbalancer", "meanweight")) == 1)
  })

  test("close a client") (new StringClient with StringServer {
    val server = stringServer.serve(":*", Service.mk { r: String =>
      Future.value(r.reverse)
    })
    val sr = new InMemoryStatsReceiver
    val va = Var[Addr](Addr.Bound(Set(server.boundAddress)))
    val client = stringClient
      .configured(param.Stats(sr))
      .newClient(Name.Bound.singleton(va), "test")
      .toService

    assert(Await.result(client("hello")) == "hello".reverse)
    Await.ready(client.close())
    intercept[ServiceClosedException] { Await.result(client("x")) }
  })
}
