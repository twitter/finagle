package com.twitter.finagle.memcached.unit

import com.twitter.conversions.DurationOps._
import com.twitter.concurrent.Broker
import com.twitter.finagle._
import com.twitter.finagle.memcached._
import com.twitter.finagle.memcached.protocol._
import com.twitter.io.Buf
import com.twitter.util.{Await, Awaitable, Future, ReadWriteVar}
import scala.collection.mutable
import _root_.java.io.{BufferedReader, InputStreamReader}
import com.twitter.finagle.partitioning.{
  HashNodeKey,
  NodeHealth,
  NodeMarkedDead,
  NodeRevived,
  PartitionNode
}
import org.mockito.Matchers._
import org.mockito.Mockito.{RETURNS_SMART_NULLS, times, verify, verifyZeroInteractions, when}
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class KetamaClientTest extends AnyFunSuite with MockitoSugar {

  val TimeOut = 15.seconds

  private def awaitResult[T](awaitable: Awaitable[T]): T = Await.result(awaitable, TimeOut)

  test("load known good results (key, hash(?), continuum ceiling(?), IP)") {
    val stream = getClass.getClassLoader.getResourceAsStream("ketama_results")
    val reader = new BufferedReader(new InputStreamReader(stream))
    val expected = new mutable.ListBuffer[Array[String]]
    var line: String = null
    do {
      line = reader.readLine
      if (line != null) {
        val segments = line.split(" ")
        assert(segments.length == 4)
        expected += segments
      }
    } while (line != null)
    assert(expected.size == 99)

    // Build Ketama client
    def newMock() = {
      val s = mock[Service[Command, Response]]
      when(s.close(any())) thenReturn Future.Done
      s
    }
    val clients = Map(
      PartitionNode("10.0.1.1", 11211, 600) -> newMock(),
      PartitionNode("10.0.1.2", 11211, 300) -> newMock(),
      PartitionNode("10.0.1.3", 11211, 200) -> newMock(),
      PartitionNode("10.0.1.4", 11211, 350) -> newMock(),
      PartitionNode("10.0.1.5", 11211, 1000) -> newMock(),
      PartitionNode("10.0.1.6", 11211, 800) -> newMock(),
      PartitionNode("10.0.1.7", 11211, 950) -> newMock(),
      PartitionNode("10.0.1.8", 11211, 100) -> newMock()
    )

    def newService(node: PartitionNode) = clients.get(node).get
    val name = Name.bound(clients.keys.toSeq.map(PartitionNode.toAddress): _*)
    val ketamaClient = new KetamaPartitionedClient(name.addr, newService)

    info("pick the correct node")
    val ipToService = clients.map { case (key, service) => key.host -> service }.toMap
    val rng = new scala.util.Random
    for (testcase <- expected) {
      val mockClient = ketamaClient.clientOf(testcase(0))
      val expectedService = ipToService(testcase(3))
      val randomResponse = Number(rng.nextLong)

      when(expectedService.apply(any[Incr])) thenReturn Future.value(randomResponse)

      assert(awaitResult(mockClient.incr("foo")).get == randomResponse.value)
    }

    info("release")
    awaitResult(ketamaClient.close())
    clients.values foreach { client => verify(client, times(1)).close(any()) }
  }

  test("interrupted request does not change ready") {
    val mockService = mock[Service[Command, Response]]
    val client1 = PartitionNode("10.0.1.1", 11211, 600)
    def newService(node: PartitionNode) = mockService
    // create a client with no members (yet)
    val mutableAddrs: ReadWriteVar[Addr] = new ReadWriteVar(Addr.Pending)
    val ketamaClient = new KetamaPartitionedClient(mutableAddrs, newService)

    // simulate a cancelled request
    val r = ketamaClient.getResult(Seq("key"))
    assert(r.poll == None)
    r.raise(new CancelledRequestException())
    try {
      awaitResult(r)
      assert(false)
    } catch {
      case e: Throwable => ()
    }

    // a second request must not be resolved yet
    val r2 = ketamaClient.incr("key")
    assert(r2.poll == None)

    // resolve the group: request proceeds
    verifyZeroInteractions(mockService)
    when(mockService.apply(any[Incr])) thenReturn Future.value(Number(42))
    mutableAddrs.update(Addr.Bound(PartitionNode.toAddress(client1)))
    assert(awaitResult(r2).get == 42)
  }

  test("client fails requests if initial serverset is empty") {
    val mockService = mock[Service[Command, Response]]
    val client1 = PartitionNode("10.0.1.1", 11211, 600)
    def newService(node: PartitionNode) = mockService
    // create a client with no members (yet)
    val mutableAddrs: ReadWriteVar[Addr] = new ReadWriteVar(Addr.Bound())
    val ketamaClient = new KetamaPartitionedClient(mutableAddrs, newService)

    intercept[ShardNotAvailableException] {
      Await.result(ketamaClient.get("key"))
    }

    // resolve the group: request succeed
    when(mockService.apply(any[Incr])) thenReturn Future.value(Number(42))
    mutableAddrs.update(Addr.Bound(PartitionNode.toAddress(client1)))
    val r2 = ketamaClient.incr("key")
    assert(awaitResult(r2).get == 42)
  }

  test("ejects dead clients") {
    trait KetamaClientBuilder {
      val serviceA = mock[Service[Command, Response]](RETURNS_SMART_NULLS)
      val serviceB = mock[Service[Command, Response]](RETURNS_SMART_NULLS)
      val nodeA = PartitionNode("10.0.1.1", 11211, 100)
      val nodeB = PartitionNode("10.0.1.2", 11211, 100)
      val nodeKeyA = HashNodeKey(nodeA.host, nodeA.port, nodeA.weight)
      val nodeKeyB = HashNodeKey(nodeB.host, nodeB.port, nodeB.weight)
      val services = Map(
        nodeA -> serviceA,
        nodeB -> serviceB
      )
      val mutableAddrs: ReadWriteVar[Addr] =
        new ReadWriteVar(Addr.Bound(services.keys.toSeq.map(PartitionNode.toAddress): _*))

      val key = Buf.Utf8("foo")
      val value = mock[Value](RETURNS_SMART_NULLS)
      when(value.key) thenReturn key
      when(serviceA(any())) thenReturn Future.value(Values(Seq(value)))

      val broker = new Broker[NodeHealth]
      def newService(node: PartitionNode) = services.get(node).get
      val ketamaClient = new KetamaPartitionedClient(mutableAddrs, newService, broker)

      awaitResult(ketamaClient.get("foo"))
      verify(serviceA, times(1)).apply(any())

      broker !! NodeMarkedDead(nodeKeyA)
    }

    info("goes to secondary if primary is down")
    new KetamaClientBuilder {
      when(serviceB(Get(Seq(key)))) thenReturn Future.value(Values(Seq(value)))
      awaitResult(ketamaClient.get("foo"))
      verify(serviceB, times(1)).apply(any())
    }

    info("throws ShardNotAvailableException when no nodes available")
    new KetamaClientBuilder {
      broker !! NodeMarkedDead(nodeKeyB)
      intercept[ShardNotAvailableException] {
        awaitResult(ketamaClient.get("foo"))
      }
    }

    info("brings back the dead node")
    new KetamaClientBuilder {
      when(serviceA(any())) thenReturn Future.value(Values(Seq(value)))
      broker !! NodeRevived(nodeKeyA)
      awaitResult(ketamaClient.get("foo"))
      verify(serviceA, times(2)).apply(any())
      broker !! NodeRevived(nodeKeyB)
    }

    info("primary leaves and rejoins")
    new KetamaClientBuilder {
      mutableAddrs.update(Addr.Bound(PartitionNode.toAddress(nodeB))) // nodeA leaves
      when(serviceB(Get(Seq(key)))) thenReturn Future.value(Values(Seq(value)))
      awaitResult(ketamaClient.get("foo"))
      verify(serviceB, times(1)).apply(any())

      mutableAddrs.update(
        Addr.Bound(PartitionNode.toAddress(nodeA), PartitionNode.toAddress(nodeB))
      ) // nodeA joins
      when(serviceA(Get(Seq(key)))) thenReturn Future.value(Values(Seq(value)))
      awaitResult(ketamaClient.get("foo"))
      verify(serviceA, times(2)).apply(any())
    }
  }
}
