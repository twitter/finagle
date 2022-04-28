package com.twitter.finagle.partitioning

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.partitioning.PartitioningService.PartitionedResults
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.Address
import com.twitter.finagle._
import com.twitter.util._
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch

class PartitioningServiceTest extends PartitioningServiceTestBase {

  import PartitioningServiceTestBase._

  override def getPartitioningServiceModule: Stackable[ServiceFactory[String, String]] = {
    SimplePartitioningService.module
  }

  test("partitioning service makes the non-batched requests stick to the matching port") {
    val sr = new InMemoryStatsReceiver
    val numServers = 21
    servers = createServers(numServers)
    client = createClient(sr)

    assert(servers.length == numServers)
    assert(sr.counters(Seq("client", "loadbalancer", "adds")) == numServers)

    // using multiple iterations to test repeatability
    0 until 5 foreach { i =>
      // send the client requests concurrently. One request per client.
      val resFutures: Seq[Future[String]] = servers.map(s => client(s._3.toString))

      // wait for all three to finish
      awaitResult(Future.join(resFutures))

      servers zip resFutures foreach {
        case (s, resFuture) =>
          val res = awaitResult(resFuture)
          assert(res == s"${s._3}${EchoDelimiter}server#${s._4}", s"i=$i $s res=$res")
      }
      assert(sr.counters(Seq("client", "loadbalancer", "adds")) == numServers, s"i=$i")
    }
  }

  test("partitioning service makes the batched requests stick to the matching port") {
    // for this test, a batched request is a delimiter separated string of multiple sub requests
    val sr = new InMemoryStatsReceiver
    val numServers = 11
    servers = createServers(numServers)
    client = createClient(sr)

    assert(servers.length == numServers)
    assert(sr.counters(Seq("client", "loadbalancer", "adds")) == numServers)

    // using multiple iterations to test repeatability
    0 until 5 foreach { i =>
      val batchedRequest: String = servers.map(_._3.toString()).mkString(RequestDelimiter)
      val batchedResponse = awaitResult(client(batchedRequest))
      val responses = batchedResponse.split(ResponseDelimiter)
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
    servers = createServers(numServers)
    client = createClient(sr)
    failingHosts.add("server#4")

    assert(servers.length == numServers)
    assert(sr.counters(Seq("client", "loadbalancer", "adds")) == numServers)

    val batchedRequest: String = servers.map(_._3).mkString(RequestDelimiter)
    val batchedResponse = awaitResult(client(batchedRequest))
    val responses = batchedResponse.split(ResponseDelimiter)

    assertPartialSuccess(
      numServers,
      responses,
      "com.twitter.finagle.ChannelClosedException"
    )
  }

  test("batched request with slow hosts") {
    val sr = new InMemoryStatsReceiver
    val numServers = 5
    servers = createServers(numServers)
    client = createClient(sr)
    slowHosts.add("server#4")

    // make the client wait for 4 successful responses. Otherwise the following timeControl.advance
    // will race against the server responses and timeout even the responses from healthy servers
    val latch = new CountDownLatch(numServers - 1)
    serverLatchOpt = Some(latch)

    assert(servers.length == numServers)
    assert(sr.counters(Seq("client", "loadbalancer", "adds")) == numServers)

    Time.withCurrentTimeFrozen { timeControl =>
      val batchedRequest: String = servers.map(_._3).mkString(RequestDelimiter)
      val future = client(batchedRequest)

      // wait for responses from 4 healthy servers to prevent race condition
      latch.await()

      // extra sleep to prevent the TimeoutFilter to race against response processing
      Thread.sleep(100)

      // now force the timeout of remaining response (request timeout is 1.5 seconds)
      timeControl.advance(3.seconds)
      timer.tick()

      val batchedResponse = Await.result(future, 10.seconds)
      val responses = batchedResponse.split(ResponseDelimiter)

      assertPartialSuccess(
        numServers,
        responses,
        "com.twitter.finagle.IndividualRequestTimeoutException"
      )
    }
  }

  private def assertPartialSuccess(
    numServers: Int,
    responses: Array[String],
    expectedException: String
  ): Unit = {
    assert(responses.length == numServers)

    // test partial success. There should be exactly one exception in the response. See
    // the custom test logic in SimplePartitioningService.mergeResponses.
    val lastResponse = responses(responses.length - 1)
    assert(lastResponse == expectedException, responses.mkString(ResponseDelimiter))

    // check the rest of the responses as well
    val responseMap = responses
      .dropRight(1).map { response =>
        val portAndName = response.split(EchoDelimiter)
        assert(portAndName.length == 2, response)
        val port = portAndName(0)
        val name = portAndName(1)
        port -> name
      }.toMap
    assert(responseMap.size == numServers - 1)

    // Remove the last server which we have already asserted for failure above.
    servers.dropRight(1) foreach {
      case (_, _, port, index) =>
        val respondedByShard = responseMap.getOrElse(
          port.toString,
          fail(s"Response for server#$index not found! ${responses.mkString(ResponseDelimiter)}")
        )
        assert(respondedByShard == s"server#$index")
    }
  }
}

/**
 * This partitioning service uses a simple string based matching ("request -> server port) to pin
 * requests with the specific partitions. To send a given request to a specific named
 * partition, caller needs to set the request to that name.
 */
private[this] object SimplePartitioningService {

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
        Stack.leaf(role, ServiceFactory.const(service))
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
  dest: Var[Addr])
    extends PartitioningService[String, String] {

  import PartitioningServiceTestBase._

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

  private[this] def getPartitionFor(request: String): Future[Service[String, String]] = {
    serviceMap.get(request) match {
      case service: Future[Service[String, String]] =>
        service
      case null =>
        unresolvedPartition
    }
  }

  private val stringToTuple = (s: String) => {
    val trimmed = s.trim
    (trimmed, Seq(getPartitionFor(trimmed)))
  }

  override protected def partitionRequest(
    batchedRequest: String
  ): Future[Map[String, Seq[Future[Service[String, String]]]]] = {
    // assuming all sub-requests are unique (one request per partition). If not the following code
    // will need to group requests by partition by using getPartitionFor method
    Future.value(batchedRequest.split(RequestDelimiter).map(stringToTuple).toMap)
  }

  protected override def mergeResponses(
    originalReq: String,
    pr: PartitionedResults[String, String]
  ): String =
    mergeStringResults(originalReq, pr)

  protected def noPartitionInformationHandler(req: String): Future[Nothing] =
    Future.exception(new Exception("boom!"))
}
