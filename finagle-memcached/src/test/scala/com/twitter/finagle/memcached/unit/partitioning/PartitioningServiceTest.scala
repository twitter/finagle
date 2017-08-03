package com.twitter.finagle.memcached.unit.partitioning

import com.twitter.conversions.time._
import com.twitter.finagle.client.{StackClient, StringClient}
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.memcached.partitioning.PartitioningService
import com.twitter.finagle.naming.BindingFactory
import com.twitter.finagle.param.Stats
import com.twitter.finagle.server.StringServer
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.{Address, _}
import com.twitter.util._
import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.ConcurrentHashMap
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import scala.collection.mutable

class PartitioningServiceTest
    extends FunSuite
    with StringClient
    with StringServer
    with BeforeAndAfterEach {
  private[this] val EchoDelimiter = ':'
  private[this] val failingHosts = new mutable.HashSet[String]()
  private[this] val slowHosts = new mutable.HashSet[String]()

  private[this] var servers: Seq[(ListeningServer, InetSocketAddress, Int, Int)] = _
  private[this] var client: Service[String, String] = _

  override def beforeEach(): Unit = {
    failingHosts.clear()
    slowHosts.clear()
  }

  override def afterEach(): Unit = {
    client.close()
    servers.foreach(_._1.close())
  }

  private[this] def createCluster(size: Int): Seq[(ListeningServer, InetSocketAddress, Int, Int)] = {
    def echoService(servername: String): Service[String, String] = Service.mk[String, String](
      req => {
        if (failingHosts.contains(servername)) {
          Future.exception(new RuntimeException(s"$servername failed!"))
        } else if (slowHosts.contains(servername)) {
          Future
            .sleep(5.seconds)(DefaultTimer)
            .before(
              Future.value(s"Response from $servername: after sleep")
            )
        } else {
          // sending back the hostname along with the request value, so that the caller can
          // assert that the request landed on the correct host
          Future.value(req + EchoDelimiter + servername) // $port:$hostname
        }
      }
    )
    // create a cluster of multiple servers, listening on unique port numbers
    1 to size map { i =>
      val addr = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
      val server = stringServer.serve(addr, echoService(servername = s"server#$i"))
      val boundAddress = server.boundAddress.asInstanceOf[InetSocketAddress]
      val port = boundAddress.getPort
      (server, boundAddress, port, i)
    }
  }

  private[this] def createClient(
    sr: InMemoryStatsReceiver
  ): Service[String, String] = {
    val dest: Name = Name.bound(servers.map(s => Address(s._2)): _*)

    // create a partitioning aware finagle client by inserting the PartitioningService appropriately
    val newClientStack =
      StackClient
        .newStack[String, String]
        .insertAfter(
          BindingFactory.role,
          SimplePartitioningService.module
        )

    stringClient
      .withStack(newClientStack)
      .withRequestTimeout(1.seconds)
      .configured(Stats(sr))
      .newService(dest, "client")
  }

  test("partitioning service makes the non-batched requests stick to the matching port") {
    val sr = new InMemoryStatsReceiver
    val numServers = 21
    servers = createCluster(numServers)
    client = createClient(sr)

    assert(servers.length == numServers)
    assert(sr.counters(Seq("client", "loadbalancer", "adds")) == numServers)

    // using multiple iterations to test repeatability
    0 until 5 foreach { i =>
      // send the client requests concurrently to test race conditions. One request per client.
      val resFutures: Seq[Future[String]] = servers.map(s => client(s"${s._3}"))

      // wait for all three to finish
      Await.result(Future.join(resFutures), 1.second)

      servers zip resFutures foreach {
        case (s, resFuture) =>
          val res = Await.result(resFuture, 1.second)
          assert(res == s"${s._3}${EchoDelimiter}server#${s._4}", s"i=$i $s res=$res")
      }
      assert(sr.counters(Seq("client", "loadbalancer", "adds")) == numServers, s"i=$i")
    }
  }

  test("partitioning service makes the batched requests stick to the matching port") {
    // for this test, a batched request is a delimiter separated string of multiple sub requests
    val sr = new InMemoryStatsReceiver
    val numServers = 11
    servers = createCluster(numServers)
    client = createClient(sr)

    assert(servers.length == numServers)
    assert(sr.counters(Seq("client", "loadbalancer", "adds")) == numServers)

    // using multiple iterations to test repeatability
    0 until 5 foreach { i =>
      val batchedRequest: String = servers
        .map(s => s"${s._3}")
        .mkString(
          SimplePartitioningService.RequestDelimiter
        )
      val batchedResponse = Await.result(client(batchedRequest), 1.second)
      val responses = batchedResponse.split(SimplePartitioningService.ResponseDelimiter)
      assert(responses.length == numServers)

      val responseMap: Map[String, String] = responses.map { response =>
        val portAndName = response.split(EchoDelimiter)
        assert(portAndName.length == 2)
        val port = portAndName(0)
        val name = portAndName(1)
        port -> name
      }.toMap
      assert(responseMap.size == numServers)

      servers foreach { s =>
        val respondedByShard = responseMap.getOrElse(s._3.toString, fail())
        assert(respondedByShard == s"server#${s._4}")
      }
      assert(sr.counters(Seq("client", "loadbalancer", "adds")) == numServers, s"i=$i")
    }
  }

  test("batched request with failing hosts") {
    val sr = new InMemoryStatsReceiver
    val numServers = 5
    servers = createCluster(numServers)
    client = createClient(sr)
    failingHosts.add("server#3")

    assert(servers.length == numServers)
    assert(sr.counters(Seq("client", "loadbalancer", "adds")) == numServers)

    val batchedRequest: String = servers
      .map(s => s"${s._3}")
      .mkString(
        SimplePartitioningService.RequestDelimiter
      )
    intercept[ChannelClosedException] {
      Await.result(client(batchedRequest), 1.second)
    }
  }

  test("batched request with slow hosts") {
    val sr = new InMemoryStatsReceiver
    val numServers = 5
    servers = createCluster(numServers)
    client = createClient(sr)
    slowHosts.add("server#2")

    assert(servers.length == numServers)
    assert(sr.counters(Seq("client", "loadbalancer", "adds")) == numServers)

    Time.withCurrentTimeFrozen { timeControl =>
      val batchedRequest: String = servers
        .map(s => { s._3 })
        .mkString(
          SimplePartitioningService.RequestDelimiter
        )
      intercept[IndividualRequestTimeoutException] {
        val future = client(batchedRequest)
        timeControl.advance(2.seconds)
        Await.result(future, 3.second)
      }
    }
  }
}

/**
 * This partitioning service uses a simple string based matching ("request -> server port) to pin
 * requests with the specific partitions. To send a given request to a specific named
 * partition, caller needs to set the request to that name.
 */
private[this] object SimplePartitioningService {

  // Using the following delimiters to simulate batched requests. When request string contains
  // multiple delimited strings, it will be treated as a batched request, with each string segment
  // to be served with matching partition.
  val RequestDelimiter = ";"

  // When request was batched the responses will be collected and returned back after combining
  // together using the delimiter
  val ResponseDelimiter = ";"

  def module: Stackable[ServiceFactory[String, String]] = {
    new Stack.Module[ServiceFactory[String, String]] {
      val role = Stack.Role("SimplePartitioning")
      val description = "Maintains host stickiness based on name"

      override def parameters: Seq[Stack.Param[_]] = Seq.empty

      def make(
        params: Stack.Params,
        next: Stack[ServiceFactory[String, String]]
      ): Stack[ServiceFactory[String, String]] = {
        val LoadBalancerFactory.Dest(dest: Var[Addr]) = params[LoadBalancerFactory.Dest]
        val service = new SimplePartitioningService(next, params, dest)
        Stack.Leaf(role, ServiceFactory.const(service))
      }
    }
  }
}

/**
 * This service makes the request stick to the partition using the port that matches the request.
 * So requests can choose the partition they intend to use.
 *
 * BatchedRequests: multiple sub-requests delimited by RequestDelimiter. The responses are
 * concatenated using ResponseDelimiter.
 *
 * This example is just for testing and proof of concept. In a real use case the batched requests
 * will contain multiple keys and the responses will be key-value maps. The merged response
 * will combine the smaller maps into a single key-value map before returning the response to the
 * caller.
 */
private[this] class SimplePartitioningService(
  underlying: Stack[ServiceFactory[String, String]],
  params: Stack.Params,
  dest: Var[Addr]
) extends PartitioningService[String, String] {

  import SimplePartitioningService._

  val unresolvedPartition: Future[Service[String, String]] = {
    val modifiedParams = params + LoadBalancerFactory.Dest(Var.value(Addr.Neg))
    val next: ServiceFactory[String, String] = underlying.make(modifiedParams)
    next()
  }

  // map port numbers to the services representing downstream partitions
  private[this] val serviceMap = new ConcurrentHashMap[String, Future[Service[String, String]]]()

  {
    // initialize the map of port -> service representing a shard
    val serviceMapVar = dest.map {
      case Addr.Bound(addresses: Set[Address], _) =>
        addresses.map {
          case addr @ Address.Inet(ia: InetSocketAddress, _) =>
            val service = mkService(addr)
            serviceMap.put(ia.getPort.toString, service)
            (ia.getPort.toString, service)
          case _ =>
            throw new IllegalStateException
        }.toMap
      case _ =>
        Map.empty
    }
    serviceMapVar.changes.register(Witness({ _: Map[_ <: String, Future[Service[String, String]]] =>
      }))
  }

  private[this] def mkService(addr: Address.Inet): Future[Service[String, String]] = {
    val modifiedParams = params + LoadBalancerFactory.Dest(Var.value(Addr.Bound(addr)))
    val next = underlying.make(modifiedParams)
    next().map { svc =>
      new ServiceProxy(svc) {
        override def close(deadline: Time): Future[Unit] = {
          Future.join(Seq(next.close(deadline), super.close(deadline)))
        }
      }
    }
  }

  override protected def getPartitionFor(request: String): Future[Service[String, String]] = {
    serviceMap.get(request) match {
      case service: Future[Service[String, String]] =>
        service
      case null =>
        unresolvedPartition
    }
  }

  override protected def partitionRequest(batchedRequest: String): Seq[String] = {
    // assuming all sub-requests are unique (one request per partition). If not the following code
    // will need to group requests by partition by using getPartitionFor method
    batchedRequest.split(RequestDelimiter).map(_.trim).toSeq
  }

  override protected def mergeResponses(responses: Seq[String]): String = {
    // responses contain the request keys. So just concatenate. In a real implementation this will
    // typically be a key-value map.
    responses mkString ResponseDelimiter
  }
}
