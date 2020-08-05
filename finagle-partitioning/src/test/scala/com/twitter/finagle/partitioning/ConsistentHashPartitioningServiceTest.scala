package com.twitter.finagle.partitioning

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Stack.Params
import com.twitter.finagle._
import com.twitter.finagle.partitioning.ConsistentHashPartitioningService.NoPartitioningKeys
import com.twitter.finagle.partitioning.PartitioningService.PartitionedResults
import com.twitter.finagle.partitioning.param.NumReps
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import com.twitter.hashing.KeyHasher
import com.twitter.util._
import java.nio.charset.StandardCharsets.UTF_8
import scala.util.Random

class ConsistentHashPartitioningServiceTest extends PartitioningServiceTestBase {

  import PartitioningServiceTestBase._

  override def getPartitioningServiceModule: Stackable[ServiceFactory[String, String]] = {
    TestConsistentHashPartitioningService.module
  }

  private[this] def randomString(length: Int): String = {
    Random.alphanumeric.take(length).mkString
  }

  // sends random strings to the servers, asserts random distribution and returns the request
  // distribution map
  private[this] def sprayRequests(numKeys: Int): Map[String, String] = {
    (1 to numKeys).map { _ =>
      val request = randomString(50)
      // the response from the server is going to be of the form: $request:$servername
      val response = awaitResult(client(request)).split(EchoDelimiter)
      // assert the response was expected
      assert(response.head == request)
      request -> response(1)
    }.toMap
  }

  test("requests stick to the node that it hashes to for non-batched requests") {
    val sr = new InMemoryStatsReceiver
    val numServers = 5
    servers = createServers(numServers)
    client = createClient(sr)

    assert(servers.length == numServers)
    assert(sr.counters(Seq("client", "loadbalancer", "adds")) == numServers)

    val numKeys = 50
    // first send numKeys requests to the cluster and find the nodes they stick with.
    // ensure they stick to the same node subsequently to verify hashing behavior
    val requestToServer: Map[String, String] = sprayRequests(numKeys)

    // using multiple iterations to test repeatability
    0 until 5 foreach { i =>
      // send the client requests concurrently. One request per client.
      val resFutures: Map[String, Future[String]] = requestToServer map {
        case (request, serverName) =>
          (serverName, client(request))
      }

      // wait for all requests to finish
      awaitResult(Future.join(resFutures.values.toSeq))

      resFutures foreach {
        case (serverName, resFuture) =>
          assert(serverName == awaitResult(resFuture).split(EchoDelimiter)(1), s"i=$i")
      }

      assert(sr.counters(Seq("client", "loadbalancer", "adds")) == numServers)
    }
  }

  test("requests stick to the node that it hashes to for batched requests") {
    val sr = new InMemoryStatsReceiver
    val numServers = 5
    servers = createServers(numServers)
    client = createClient(sr)

    assert(servers.length == numServers)
    assert(sr.counters(Seq("client", "loadbalancer", "adds")) == numServers)

    val numKeys = 25
    // first send numKeys requests to the cluster and find the nodes they stick with.
    // ensure they stick to the same node subsequently to verify hashing behavior
    val keys = 1 to numKeys map { _ => randomString(5) }
    val batchedRequest: String = keys.mkString(RequestDelimiter)

    // capture the request distribution (request -> server) for asserting stickiness
    val requestToServer: Map[String, String] = {
      val batchedResponse = awaitResult(client(batchedRequest))
      val responses = batchedResponse.split(ResponseDelimiter)
      responses.map { response =>
        val requestAndServer = response.split(EchoDelimiter)
        requestAndServer(0) -> requestAndServer(1)
      }.toMap
    }

    // using multiple iterations to test repeatability
    0 until 5 foreach { i =>
      val batchedResponse = awaitResult(client(batchedRequest))
      val responses = batchedResponse.split(ResponseDelimiter)
      responses.map { response =>
        val requestAndServer = response.split(EchoDelimiter)
        val request = requestAndServer(0)
        val serverName = requestAndServer(1)
        // assert that the requests landed on the same host as before
        assert(serverName == requestToServer.getOrElse(request, fail()), s"i=$i")
      }

      assert(sr.counters(Seq("client", "loadbalancer", "adds")) == numServers)
    }
  }

  test("node addition and removal") {
    val sr = new InMemoryStatsReceiver

    // start with 3 servers
    servers = createServers(3)

    val mutableAddrs: ReadWriteVar[Addr] = new ReadWriteVar(
      Addr.Bound(servers.map(s => Address(s._2)): _*)
    )
    val dest: Name = Name.Bound.singleton(mutableAddrs)

    client = createClient(sr, dest)

    assert(sr.counters(Seq("client", "partitioner", "redistributes")) == 1)
    assert(sr.counters(Seq("client", "loadbalancer", "rebuilds")) == 3)
    assert(sr.counters(Seq("client", "loadbalancer", "updates")) == 3)
    assert(sr.counters(Seq("client", "loadbalancer", "adds")) == 3)
    assert(sr.counters(Seq("client", "loadbalancer", "removes")) == 0)

    // add two more servers
    val additions = createServers(2, 3)

    servers = servers ++ additions
    mutableAddrs.update(Addr.Bound(servers.map(s => Address(s._2)).toSet))

    assert(sr.counters(Seq("client", "partitioner", "redistributes")) == 2)
    assert(sr.counters(Seq("client", "loadbalancer", "rebuilds")) == 5)
    assert(sr.counters(Seq("client", "loadbalancer", "updates")) == 5)
    assert(sr.counters(Seq("client", "loadbalancer", "adds")) == 5)
    assert(sr.counters(Seq("client", "loadbalancer", "removes")) == 0)

    // remove one server
    val toDrop = servers.head
    toDrop._1.close()
    servers = servers.toSet.drop(1).toSeq
    mutableAddrs.update(Addr.Bound(servers.map(s => Address(s._2)).toSet))

    assert(sr.counters(Seq("client", "partitioner", "redistributes")) == 3)
    assert(sr.counters(Seq("client", "loadbalancer", "rebuilds")) == 5)
    assert(sr.counters(Seq("client", "loadbalancer", "updates")) == 5)
    assert(sr.counters(Seq("client", "loadbalancer", "adds")) == 5)
    assert(sr.counters(Seq("client", "partitioner", "leaves")) == 1)
    assert(sr.counters(Seq("client", "loadbalancer", "removes")) == 1)
  }

  test("re-hash when bad hosts are ejected") {
    val sr = new InMemoryStatsReceiver

    // start with 5 servers
    servers = createServers(5)
    client = createClient(sr, ejectFailedHosts = true)

    // send some random requests and store the request distribution
    val requestToServer = sprayRequests(50)

    assert(sr.counters(Seq("client", "partitioner", "redistributes")) == 1)

    // kill one of the host
    servers.head._1.close()

    // trigger ejection by sending requests
    Await.ready(Future.join(requestToServer.keySet.map(client(_)).toSeq), Timeout)

    // at least one host should get ejected, because we don't know which hosts were hit by above
    // requests
    eventually {
      assert(sr.counters(Seq("client", "partitioner", "ejections")) == 1)
      assert(sr.counters(Seq("client", "partitioner", "redistributes")) == 2)
    }

    // requests that went to the killed server earlier, should not end up on it now
    val resFutures: Map[String, Future[String]] = requestToServer map {
      case (request, serverName) =>
        (serverName, client(request))
    }
    resFutures foreach {
      case (serverName, resFuture) =>
        if (serverName == "server#0") {
          assert("server#0" != awaitResult(resFuture).split(EchoDelimiter)(1))
        } else {
          assert(serverName == awaitResult(resFuture).split(EchoDelimiter)(1))
        }
    }
  }

  test("host comes back into ring after being ejected (multiple host cluster)") {
    val sr = new InMemoryStatsReceiver

    // start 5 servers
    servers = createServers(5)
    client = createClient(sr, ejectFailedHosts = true)

    // pick a shard by sending a random request
    val request = randomString(50)
    val serverName = awaitResult(client(request)).split(EchoDelimiter)(1)

    Time.withCurrentTimeFrozen { timeControl =>
      // Make the host throw an exception
      failingHosts.add(serverName)
      intercept[ChannelClosedException] {
        awaitResult(client(request))
      }
      failingHosts.clear()

      // Node should have been ejected
      assert(sr.counters.get(List("client", "partitioner", "ejections")).contains(1))

      // request should end up somewhere else
      assert(awaitResult(client(request)) != request + EchoDelimiter + serverName)

      timeControl.advance(10.minutes)
      timer.tick()

      // 10 minutes (markDeadFor duration) have passed, so the request should go back the same host
      assert(sr.counters.get(List("client", "partitioner", "revivals")).contains(1))
      assert(awaitResult(client(request)) == request + EchoDelimiter + serverName)
    }
  }

  test("host comes back into ring after being ejected (single host cluster)") {
    val sr = new InMemoryStatsReceiver

    // start 1 server
    servers = createServers(1)
    client = createClient(sr, ejectFailedHosts = true)

    val request = randomString(50)
    val serverName = "server#0"

    Time.withCurrentTimeFrozen { timeControl =>
      // Make the host throw an exception
      failingHosts.add(serverName)
      intercept[ChannelClosedException] {
        awaitResult(client(request))
      }
      failingHosts.clear()

      // Node should have been ejected
      assert(sr.counters.get(List("client", "partitioner", "ejections")).contains(1))

      // Node should have been marked dead, and still be dead after 5 minutes
      timeControl.advance(5.minutes)
      timer.tick()

      // Shard should be unavailable
      intercept[ShardNotAvailableException] {
        awaitResult(client(request))
      }

      timeControl.advance(5.minutes)
      timer.tick()

      // 10 minutes (markDeadFor duration) have passed, so the request should go back the same host
      assert(sr.counters.get(List("client", "partitioner", "revivals")).contains(1))
      assert(awaitResult(client(request)) == request + EchoDelimiter + serverName)
    }
  }

  test("no partitioning keys") {
    client = createClient(NullStatsReceiver, ejectFailedHosts = true)
    intercept[NoPartitioningKeys] {
      awaitResult(client(""))
    }
  }
}

object TestConsistentHashPartitioningService {

  val role = Stack.Role("KetamaPartitioning")
  val description = "Partitioning Service based on Ketama consistent hashing"

  private[finagle] def module: Stackable[ServiceFactory[String, String]] =
    new ConsistentHashPartitioningService.Module[String, String, String] {

      override val role: Stack.Role = TestConsistentHashPartitioningService.role
      override val description: String = TestConsistentHashPartitioningService.description

      def newConsistentHashPartitioningService(
        underlying: Stack[ServiceFactory[String, String]],
        params: Params
      ): ConsistentHashPartitioningService[String, String, String] = {
        new TestConsistentHashPartitioningService(
          underlying = underlying,
          params = params
        )
      }
    }
}

private[this] class TestConsistentHashPartitioningService(
  underlying: Stack[ServiceFactory[String, String]],
  params: Stack.Params,
  keyHasher: KeyHasher = KeyHasher.KETAMA,
  numReps: Int = NumReps.Default,
  oldLibMemcachedVersionComplianceMode: Boolean = false)
    extends ConsistentHashPartitioningService[String, String, String](
      underlying,
      params,
      keyHasher,
      numReps
    ) {

  import PartitioningServiceTestBase._

  protected override def getKeyBytes(key: String): Array[Byte] = {
    key.getBytes(UTF_8)
  }

  protected override def getPartitionKeys(request: String): Seq[String] = {
    if (request.isEmpty)
      Seq.empty
    else
      request.split(RequestDelimiter).map(_.trim).toSeq
  }

  protected override def createPartitionRequestForKeys(
    request: String,
    pKeys: Seq[String]
  ): String = {
    pKeys.mkString(RequestDelimiter)
  }

  protected override def mergeResponses(
    origReq: String,
    pr: PartitionedResults[String, String]
  ): String =
    mergeStringResults(origReq, pr)

  override protected def noPartitionInformationHandler(req: String): Future[Nothing] =
    Future.exception(new NoPartitioningKeys("TestConsistentHashPartitioningService"))
}
