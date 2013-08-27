package com.twitter.finagle.memcached.unit

import com.twitter.finagle.{Group, Service, ShardNotAvailableException}
import com.twitter.finagle.memcached._
import com.twitter.finagle.memcached.protocol._
import com.twitter.hashing.KeyHasher
import com.twitter.concurrent.Broker
import com.twitter.util.{Await, Duration, Future}
import org.jboss.netty.buffer.ChannelBuffers
import org.specs.mock.Mockito
import org.specs.SpecificationWithJUnit
import scala.collection.{immutable, mutable}
import _root_.java.io.{BufferedReader, InputStreamReader}

class ClientSpec extends SpecificationWithJUnit with Mockito {
  "KetamaClient" should {
    // Test from Smile's KetamaNodeLocatorSpec.scala

    // Load known good results (key, hash(?), continuum ceiling(?), IP)
    val stream = getClass.getClassLoader.getResourceAsStream("ketama_results")
    val reader = new BufferedReader(new InputStreamReader(stream))
    val expected = new mutable.ListBuffer[Array[String]]
    var line: String = null
    do {
      line = reader.readLine
      if (line != null) {
        val segments = line.split(" ")
        segments.length mustEqual 4
        expected += segments
      }
    } while (line != null)
    expected.size mustEqual 99

    // Build Ketama client
    def newMock() = {
      val s = mock[Service[Command, Response]]
      s.close(any) returns Future.Done
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
    val mockBuilder =
      (node: CacheNode, k: KetamaClientKey, _: Broker[NodeHealth], _: (Int, Duration)) => clients.get(node).get
    val ketamaClient = new KetamaClient(Group(clients.keys.toSeq:_*), KeyHasher.KETAMA, 160, (Int.MaxValue, Duration.Zero), Some(mockBuilder))

    "pick the correct node" in {
      val ipToService = clients map { case (key, service) => key.host -> service } toMap
      val rng = new scala.util.Random
      for (testcase <- expected) {
        val mockClient = ketamaClient.clientOf(testcase(0))
        val expectedService = ipToService(testcase(3))
        val randomResponse = Number(rng.nextLong)

        expectedService.apply(any[Incr]) returns Future.value(randomResponse)

        Await.result(mockClient.incr("foo")).get mustEqual randomResponse.value
      }
    }

    "release" in {
      ketamaClient.release()
      clients.values foreach { client =>
        there was one(client).close(any)
      }
    }

    "ejects dead clients" in {
      val serviceA = smartMock[Service[Command,Response]]
      val serviceB = smartMock[Service[Command,Response]]
      val nodeA = CacheNode("10.0.1.1", 11211, 100)
      val nodeB = CacheNode("10.0.1.2", 11211, 100)
      val nodeKeyA = KetamaClientKey(nodeA.host, nodeA.port, nodeA.weight)
      val nodeKeyB = KetamaClientKey(nodeB.host, nodeB.port, nodeB.weight)
      val services = Map(
        nodeA -> serviceA,
        nodeB -> serviceB
      )
      val mutableGroup = Group.mutable(services.keys.toSeq:_*)

      val key = ChannelBuffers.wrappedBuffer("foo".getBytes)
      val value = smartMock[Value]
      value.key returns key
      serviceA(any) returns Future.value(Values(Seq(value)))

      var broker = new Broker[NodeHealth]
      val mockBuilder = (node: CacheNode, k: KetamaClientKey, internalBroker: Broker[NodeHealth], _: (Int, Duration)) => {
        broker = internalBroker
        services.get(node).get
      }
      val ketamaClient = new KetamaClient(mutableGroup, KeyHasher.KETAMA, 160, (Int.MaxValue, Duration.Zero), Some(mockBuilder))

      Await.result(ketamaClient.get("foo"))
      there was one(serviceA).apply(any)

      broker !! NodeMarkedDead(nodeKeyA)

      "goes to secondary if primary is down" in {
        serviceB(Get(Seq(key))) returns Future.value(Values(Seq(value)))
        Await.result(ketamaClient.get("foo"))
        there was one(serviceB).apply(any)
      }

      "throws ShardNotAvailableException when no nodes available" in {
        broker !! NodeMarkedDead(nodeKeyB)
        Await.result(ketamaClient.get("foo")) must throwA[ShardNotAvailableException]
      }

      "brings back the dead node" in {
        serviceA(any) returns Future.value(Values(Seq(value)))
        broker !! NodeRevived(nodeKeyA)
        Await.result(ketamaClient.get("foo"))
        there was two(serviceA).apply(any)
      }

      "primary leaves and rejoins" in {
        mutableGroup.update(immutable.Set(nodeB)) // nodeA leaves
        serviceB(Get(Seq(key))) returns Future.value(Values(Seq(value)))
        Await.result(ketamaClient.get("foo"))
        there was one(serviceB).apply(any)

        mutableGroup.update(immutable.Set(nodeA, nodeB)) // nodeA joins
        serviceA(Get(Seq(key))) returns Future.value(Values(Seq(value)))
        Await.result(ketamaClient.get("foo"))
        there was two(serviceA).apply(any)
      }
    }
  }

  "RubyMemCacheClient" should {
    val client1 = mock[Client]
    val client2 = mock[Client]
    val client3 = mock[Client]
    val rubyMemCacheClient = new RubyMemCacheClient(Seq(client1, client2, client3))

    "pick the correct node" in {
      rubyMemCacheClient.clientOf("apple")    must be_==(client1)
      rubyMemCacheClient.clientOf("banana")   must be_==(client2)
      rubyMemCacheClient.clientOf("cow")      must be_==(client1)
      rubyMemCacheClient.clientOf("dog")      must be_==(client1)
      rubyMemCacheClient.clientOf("elephant") must be_==(client3)
    }
    "release" in {
      rubyMemCacheClient.release()
      there was one(client1).release()
      there was one(client2).release()
      there was one(client3).release()
    }
  }

  "PHPMemCacheClient" should {
    val client1 = mock[Client]
    val client2 = mock[Client]
    val client3 = mock[Client]
    val phpMemCacheClient = new PHPMemCacheClient(Array(client1, client2, client3), KeyHasher.FNV1_32)

    "pick the correct node" in {
      phpMemCacheClient.clientOf("apple")    must be_==(client3)
      phpMemCacheClient.clientOf("banana")   must be_==(client1)
      phpMemCacheClient.clientOf("cow")      must be_==(client3)
      phpMemCacheClient.clientOf("dog")      must be_==(client2)
      phpMemCacheClient.clientOf("elephant") must be_==(client2)
    }
    "release" in {
      phpMemCacheClient.release()
      there was one(client1).release()
      there was one(client2).release()
      there was one(client3).release()
    }
  }

}
