package com.twitter.finagle.memcached.unit

import com.twitter.concurrent.Broker
import com.twitter.finagle.{CancelledRequestException, Group, MutableGroup, Service, ShardNotAvailableException}
import com.twitter.finagle.cacheresolver.CacheNode
import com.twitter.finagle.memcached._
import com.twitter.finagle.memcached.protocol._
import com.twitter.hashing.KeyHasher
import com.twitter.io.Buf
import com.twitter.util.{Await, Duration, Future}
import scala.collection.{immutable, mutable}
import _root_.java.io.{BufferedReader, InputStreamReader}
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito.{verify, verifyZeroInteractions, when, times, RETURNS_SMART_NULLS}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class KetamaClientTest extends FunSuite with MockitoSugar {

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
      CacheNode("10.0.1.1", 11211, 600)  -> newMock(),
      CacheNode("10.0.1.2", 11211, 300)  -> newMock(),
      CacheNode("10.0.1.3", 11211, 200)  -> newMock(),
      CacheNode("10.0.1.4", 11211, 350)  -> newMock(),
      CacheNode("10.0.1.5", 11211, 1000) -> newMock(),
      CacheNode("10.0.1.6", 11211, 800)  -> newMock(),
      CacheNode("10.0.1.7", 11211, 950)  -> newMock(),
      CacheNode("10.0.1.8", 11211, 100)  -> newMock()
    )

    def newService(node: CacheNode) = clients.get(node).get
    val ketamaClient = new KetamaPartitionedClient(Group(clients.keys.toSeq:_*), newService)

    info("pick the correct node")
    val ipToService = clients map { case (key, service) => key.host -> service } toMap
    val rng = new scala.util.Random
    for (testcase <- expected) {
      val mockClient = ketamaClient.clientOf(testcase(0))
      val expectedService = ipToService(testcase(3))
      val randomResponse = Number(rng.nextLong)

      when(expectedService.apply(any[Incr])) thenReturn Future.value(randomResponse)

      assert(Await.result(mockClient.incr("foo")).get == randomResponse.value)
    }

    info("release")
    ketamaClient.release()
    clients.values foreach { client =>
      verify(client, times(1)).close(any())
    }
  }

  test("interrupted request does not change ready") {
    val mockService = mock[Service[Command, Response]]
    val client1 = CacheNode("10.0.1.1", 11211, 600)
    def newService(node: CacheNode) = mockService
    // create a client with no members (yet)
    val backends: MutableGroup[CacheNode] = Group.mutable()
    val ketamaClient = new KetamaPartitionedClient(backends, newService)

    // simulate a cancelled request
    val r = ketamaClient.getResult(Seq("key"))
    assert(r.poll == None)
    r.raise(new CancelledRequestException())
    try {
      Await.result(r)
      assert(false)
    } catch {
      case e: Throwable => Unit
    }

    // a second request must not be resolved yet
    val r2 = ketamaClient.incr("key")
    assert(r2.poll == None)

    // resolve the group: request proceeds
    verifyZeroInteractions(mockService)
    when(mockService.apply(any[Incr])) thenReturn Future.value(Number(42))
    backends.update(scala.collection.immutable.Set(client1))
    assert(Await.result(r2).get == 42)
  }

  test("ejects dead clients") {
    trait KetamaClientBuilder {
      val serviceA = mock[Service[Command,Response]](RETURNS_SMART_NULLS)
      val serviceB = mock[Service[Command,Response]](RETURNS_SMART_NULLS)
      val nodeA = CacheNode("10.0.1.1", 11211, 100)
      val nodeB = CacheNode("10.0.1.2", 11211, 100)
      val nodeKeyA = KetamaClientKey(nodeA.host, nodeA.port, nodeA.weight)
      val nodeKeyB = KetamaClientKey(nodeB.host, nodeB.port, nodeB.weight)
      val services = Map(
        nodeA -> serviceA,
        nodeB -> serviceB
      )
      val mutableGroup = Group.mutable(services.keys.toSeq:_*)

      val key = Buf.Utf8("foo")
      val value = mock[Value](RETURNS_SMART_NULLS)
      when(value.key) thenReturn key
      when(serviceA(any())) thenReturn Future.value(Values(Seq(value)))

      val broker = new Broker[NodeHealth]
      def newService(node: CacheNode) = services.get(node).get
      val ketamaClient = new KetamaPartitionedClient(mutableGroup, newService, broker)

      Await.result(ketamaClient.get("foo"))
      verify(serviceA, times(1)).apply(any())

      broker !! NodeMarkedDead(nodeKeyA)
    }

    info("goes to secondary if primary is down")
    new KetamaClientBuilder {
      when(serviceB(Get(Seq(key)))) thenReturn Future.value(Values(Seq(value)))
      Await.result(ketamaClient.get("foo"))
      verify(serviceB, times(1)).apply(any())
    }

    info("throws ShardNotAvailableException when no nodes available")
    new KetamaClientBuilder {
      broker !! NodeMarkedDead(nodeKeyB)
      intercept[ShardNotAvailableException] {
        Await.result(ketamaClient.get("foo"))
      }
    }

    info("brings back the dead node")
    new KetamaClientBuilder {
      when(serviceA(any())) thenReturn Future.value(Values(Seq(value)))
      broker !! NodeRevived(nodeKeyA)
      Await.result(ketamaClient.get("foo"))
      verify(serviceA, times(2)).apply(any())
      broker !! NodeRevived(nodeKeyB)
    }

    info("primary leaves and rejoins")
    new KetamaClientBuilder {
      mutableGroup.update(immutable.Set(nodeB)) // nodeA leaves
      when(serviceB(Get(Seq(key)))) thenReturn Future.value(Values(Seq(value)))
      Await.result(ketamaClient.get("foo"))
      verify(serviceB, times(1)).apply(any())

      mutableGroup.update(immutable.Set(nodeA, nodeB)) // nodeA joins
      when(serviceA(Get(Seq(key)))) thenReturn Future.value(Values(Seq(value)))
      Await.result(ketamaClient.get("foo"))
      verify(serviceA, times(2)).apply(any())
    }
  }
}
