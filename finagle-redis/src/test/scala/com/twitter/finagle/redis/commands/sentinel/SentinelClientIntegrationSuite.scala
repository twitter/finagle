package com.twitter.finagle.redis.integration

import com.twitter.conversions.time._
import com.twitter.finagle.redis.naggati.SentinelClientTest
import com.twitter.finagle.redis.tags.{ RedisTest, ClientTest }
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{ Await, Duration, Future, Time }
import com.twitter.finagle.redis.util.{ CBToString, StringToChannelBuffer, Sentinel }
import org.jboss.netty.buffer.ChannelBuffer
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import java.io.File
import java.net.InetAddress

@Ignore
@RunWith(classOf[JUnitRunner])
final class SentinelClientIntegrationSuite extends SentinelClientTest {

  implicit def s2cb(s: String) = StringToChannelBuffer(s)
  implicit def cb2s(cb: ChannelBuffer) = CBToString(cb)

  val sentinelCount = 3
  val masterCount = 2
  val slavesPerMaster = 2
  val count = masterCount * (slavesPerMaster + 1)
  val master0 = masterName(0)
  val master1 = masterName(1)
  val noSuchMaster = masterName(999)

  override def beforeAll(): Unit = {
    super.beforeAll()
    for {
      i <- 0 until masterCount
      j <- 1 to slavesPerMaster
    } withRedisClient(i + masterCount * j) { client =>
      val (host, port) = hostAndPort(redisAddress(i))
      Await.ready(client.slaveOf("127.0.0.1", port))
    }
  }

  private def masterName(i: Int) = "master" + i

  private def waitUntil(msg: String)(check: => Boolean) = {
    println(msg)
    val startTime = Time.now
    val until = startTime + 20.seconds
    def checkLater(): Future[Boolean] = {
      if (Time.now > until) Future.value(false)
      else DefaultTimer.twitter.doLater(1.second) {
        println(Time.now - startTime)
        if (check) Future.value(true)
        else checkLater
      }.flatten
    }
    assert(Await.result(checkLater))
  }

  test("Correctly perform the MONITOR and MASTER/MASTERS command", RedisTest, ClientTest) {
    withSentinelClient(0) { client =>
      // No masters at the beginning
      assert(Await.result(client.masters()) === Nil)

      val expected = (0 until masterCount).map { i =>
        val name = masterName(i)
        val address = redisAddress(i)
        Await.ready(client.monitor(name, address, sentinelCount + i))
        (name -> (address.getHostString, address.getPort, sentinelCount + i))
      }.toMap

      val masters = Await.result(client.masters())
        .map(m => m.name -> (m.ip, m.port, m.quorum))
        .toMap
      assert(masters === expected)

      val oneByOne = expected.keys.map { name =>
        val m = Await.result(client.master(name))
        (m.name -> (m.ip, m.port, m.quorum))
      }.toMap
      assert(oneByOne === expected)
    }
  }

  test("Correctly perform the SENTINELS command", RedisTest, ClientTest) {
    val address = redisAddress(0)
    withSentinelClient(0) { client0 =>
      // Errors should be throw for unknown names
      assert(Await.result(client0.sentinels(noSuchMaster).liftToTry).isThrow)
      // SENTINALS return a list of OTHER sentinels
      assert(Await.result(client0.sentinels(master0)).size === 0)

      (1 to 2).foreach { i =>
        withSentinelClient(i) { client =>
          Await.ready(client.monitor(master0, address, sentinelCount))
        }
      }
      waitUntil("Waiting the sentinel list to be updated ...") {
        val otherSentinels = Await.result(client0.sentinels(master0))
        val actual = otherSentinels.map(_.port).toSet
        val expected = List(sentinelAddress(1).getPort, sentinelAddress(2).getPort).toSet
        actual === expected
      }
    }
  }

  test("Correctly perform the GET-MASTER-ADDRESS-BY-NAME command", RedisTest, ClientTest) {
    withSentinelClient(0) { client =>
      (0 until masterCount).map { i =>
        assert(Await.result(client.getMasterAddrByName(noSuchMaster)).isEmpty)
        val result = Await.result(client.getMasterAddrByName(masterName(i))).get
        assert(result.getHostString == redisAddress(i).getHostString)
        assert(result.getPort == redisAddress(i).getPort)
      }
    }
  }

  // CKQUORUM is introduced in redis 2.8.22. Please run this test with a newer redis server.
  test("Correctly perform the CKQUORUM command", RedisTest, ClientTest) {
    withSentinelClient(0) { client =>
      // Errors should be throw for unknown names
      assert(Await.result(client.ckquorum(noSuchMaster).liftToTry).isThrow)

      Await.result(client.ckquorum(masterName(0)))
      Await.result(client.ckquorum(masterName(1)).liftToTry).isThrow
    }
  }

  test("Correctly perform the SLAVES command", RedisTest, ClientTest) {
    withSentinelClient(0) { client =>
      // Errors should be throw for unknown names
      assert(Await.result(client.slaves(noSuchMaster).liftToTry).isThrow)

      val expected = (0 until masterCount).map { i =>
        masterName(i) -> (1 to slavesPerMaster).map { j =>
          redisAddress(i + masterCount * j).getPort
        }.toSet
      }.toMap

      def slaves() = expected.keys.map { name =>
        name -> Await.result(client.slaves(name))
          .map(s => s.port)
          .toSet
      }.toMap

      // Sentinel PINGs masters every 10 seconds to get the latest slave list.
      // We keep checking the slave lists periodically, until it is updated
      // to the latest.
      waitUntil("Waiting the slave list to be updated ...")(slaves == expected)
    }
  }

  test("Correctly perform the RESET command", RedisTest, ClientTest) {
    withSentinelClient(0) { client =>
      // Errors should be throw for unknown names
      assert(Await.result(client.reset(noSuchMaster).liftToTry).isThrow)

      def numberOfSlaves = Await.result(client.master(master1)).numSlaves

      assert(numberOfSlaves === 2)
      stopRedis(count + sentinelCount - 1)
      Await.ready(client.reset(master1))
      waitUntil("Waiting the master to be reset ...")(numberOfSlaves === 1)
    }
  }

  test("Correctly perform the SET command", RedisTest, ClientTest) {
    withSentinelClient(0) { client =>
      val option = "down-after-milliseconds"
      // Errors should be throw for unknown names
      assert(Await.result(client.set(noSuchMaster, option, "2000").liftToTry).isThrow)

      def currentValue = Await.result(client.master(master0)).downAfterMilliseconds

      val oldValue = currentValue
      val newValue = oldValue + 1000
      Await.ready(client.set(master0, option, newValue.toString))
      assert(currentValue === newValue)
    }
  }

  test("Correctly perform the FAILOVER command", RedisTest, ClientTest) {
    withSentinelClient(0) { client =>
      // Errors should be throw for unknown names
      assert(Await.result(client.failover(noSuchMaster).liftToTry).isThrow)

      def currentMasterPort = Await.result(client.getMasterAddrByName(master0))
        .map(_.getPort)
        .get

      val oldValue = currentMasterPort
      Await.ready(client.failover(master0))
      waitUntil("Waiting failover ...")(currentMasterPort !== oldValue)
    }
  }

  test("Correctly perform the REMOVE command", RedisTest, ClientTest) {
    withSentinelClient(0) { client =>
      // Errors should be throw for unknown names
      assert(Await.result(client.remove(noSuchMaster).liftToTry).isThrow)

      Await.ready(client.remove(master0))
      Await.result(client.master(master0).liftToTry).isThrow
      val masterNames = Await.result(client.masters()).map(_.name)
      assert(masterNames === List(masterName(1)))
    }
  }

  test("Correctly perform the FLUSHCONFIG command", RedisTest, ClientTest) {
    withSentinelClient(0) { client =>
      val configFile = Await.result(client.info("server"))
        .flatMap { cb =>
          val prefix = "config_file:"
          (cb: String).trim
            .split("\n")
            .find(_.startsWith(prefix))
            .map(_.substring(prefix.length))
            .map(new java.io.File(_))
        }.get
      assert(configFile.delete())
      assert(!configFile.exists())
      Await.ready(client.flushConfig())
      assert(configFile.exists())
    }
  }
}