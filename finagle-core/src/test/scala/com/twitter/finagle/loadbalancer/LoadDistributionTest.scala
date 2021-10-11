package com.twitter.finagle.loadbalancer

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.service.ConstantService
import com.twitter.finagle.util.Rng
import com.twitter.util.Activity
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.Time
import com.twitter.util.Var
import org.scalatest.OneInstancePerTest
import scala.util.Random
import org.scalatest.funsuite.AnyFunSuite

object LoadDistributionTest {

  type ServerSet = Activity[Set[EndpointFactory[Unit, Unit]]]

  def emptyServerSet: ServerSet = Activity(Var(Activity.Ok(Set.empty[EndpointFactory[Unit, Unit]])))

  class Server extends EndpointFactory[Unit, Unit] {
    def remake() = {}
    val address = Address.Failed(new Exception)

    var load: Int = 0
    var closed: Boolean = false

    def apply(conn: ClientConnection): Future[Service[Unit, Unit]] = {
      load += 1
      Future.value(new ConstantService[Unit, Unit](Future.Done))
    }

    def close(deadline: Time): Future[Unit] = {
      closed = true
      Future.Done
    }

    override def status: Status = if (closed) Status.Closed else Status.Open
  }
}

/**
 * An essential goal of any load balancer (including the given `balancer`) is to
 * spread the load more-or-less uniformly across a number of nodes. This test checks
 * that property for a variety of different scenarios (including deploys/rolling restarts).
 *
 * It's a bit tricky to define load bounds that would work for every LB we support. We decided
 * to go with a very relaxed interval that works for every Finagle LB and provide us with
 * reasonable level of confidence of the changes we make.
 *
 * Here we propose that the upper bound of a load on each server is 2x of optimal, absolutely
 * uniform distribution. While this seems quite relaxed (some of the nodes will get nearly
 * twice as much load), it's still within the allowed load bounds for P2C: `O(log log n)`,
 * where `n` is both number of requests and number of nodes.
 *
 * As any other approximation, `O(log log n)` works great for big numbers, but breaks near zero.
 *
 * Assuming that the upper bound for a load on a given node is 1.5x and solving `log log x = 2`
 * for x, we can get the maximum size of the cluster (~1600) for which these tests are correct.
 */
abstract class LoadDistributionTest(newBalancerFactory: Rng => LoadBalancerFactory)
    extends AnyFunSuite
    with OneInstancePerTest {

  import LoadDistributionTest._

  private[this] val serverset = Var(Vector.empty[EndpointFactory[Unit, Unit]])

  private[this] def newClients(n: Int): Vector[ServiceFactory[Unit, Unit]] =
    Vector.tabulate(n)(i =>
      newBalancerFactory(Rng(i)).newBalancer(
        Activity(serverset.map(Activity.Ok(_))),
        new NoBrokersAvailableException(),
        Stack.Params.empty
      ))

  private[this] def newServers(n: Int): Vector[Server] =
    Vector.fill(n)(new Server)

  private[this] def sendAndWait(load: Int)(sf: ServiceFactory[Unit, Unit]): Unit =
    Await.ready(Future.collect(Seq.fill(load)(sf())), 15.seconds)

  test("clients deploy") {
    val clients = newClients(5)
    val servers = newServers(10)

    serverset.update(servers)

    // Each client sends 150 requests (750 in total).
    clients.foreach(sendAndWait(150))

    // Optimal load is 750 / 10 = 75.
    // With eager connections enabled, we expect 1 more request per client.
    // With eager connections disabled, we expect <= 150, with it enabled we expect <= 155
    // (as we have 5 clients)
    assert(servers.forall(s => s.load <= 155))
  }

  test("servers deploy") {
    val clients = newClients(5)
    val servers = newServers(20)

    // Initial state.
    serverset.update(servers.take(10))
    clients.foreach(sendAndWait(150))

    // Deploy the first batch.
    serverset.update(servers.slice(5, 15))
    clients.foreach(sendAndWait(150))

    // Deploy the second batch.
    serverset.update(servers.slice(10, 20))
    clients.foreach(sendAndWait(150))

    // Optimal load is 750 * 3 / 10 = 225.
    assert(servers.forall(s => s.load <= 450))
  }

  test("servers shrink") {
    val clients = newClients(5)
    val servers = newServers(10)

    // Initial state.
    serverset.update(servers)
    clients.foreach(sendAndWait(150))

    serverset.update(servers.take(5))
    clients.foreach(sendAndWait(150))

    // Optimal load is 750 * 2 / 10 = 150.
    assert(servers.forall(s => s.load <= 300))
  }

  test("servers expand") {
    val clients = newClients(5)
    val servers = newServers(15)

    // Initial state.
    serverset.update(servers.take(10))
    clients.foreach(sendAndWait(150))

    serverset.update(servers)
    clients.foreach(sendAndWait(150))

    // Ideal load is 750 * 2 / 10 = 150.
    assert(servers.forall(s => s.load <= 300))
  }
}

class HeapLoadDistributionTest extends LoadDistributionTest(_ => Balancers.heap(new Random(12345)))

class RoundRobinLoadDistributionTest extends LoadDistributionTest(_ => Balancers.roundRobin())

class P2CLeastLoadedLoadDistributionTest
    extends LoadDistributionTest(notSoRandom => Balancers.p2c(rng = notSoRandom))

class P2CPeakEmwaLoadDistributionTest
    extends LoadDistributionTest(notSoRandom => Balancers.p2cPeakEwma(rng = notSoRandom))

class ApertureLoadDistributionTest
    extends LoadDistributionTest(notSoRandom =>
      Balancers.aperture(rng = notSoRandom, minAperture = 5))
