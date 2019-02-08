package com.twitter.finagle.redis.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.redis.SentinelClientTest
import com.twitter.finagle.redis.tags.{ClientTest, RedisTest}
import com.twitter.finagle.redis.util.BufToString
import com.twitter.finagle.util.DefaultTimer
import com.twitter.io.Buf
import com.twitter.logging.Logger
import com.twitter.util.{Await, Awaitable, Future, Time}

final class SentinelClientIntegrationSuite extends SentinelClientTest {

  val log = Logger(getClass)

  val sentinelCount = 3
  val masterCount = 2
  val replicasPerMaster = 2
  val count = masterCount * (replicasPerMaster + 1)
  val master0 = masterName(0)
  val master1 = masterName(1)
  val noSuchMaster = masterName(999)
  val defaultTimeout = 1.second

  def ready[T <: Awaitable[_]](awaitable: T): T = {
    Await.ready(awaitable, defaultTimeout)
  }

  def result[T](awaitable: Awaitable[T]): T = {
    Await.result(awaitable, defaultTimeout)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    for {
      i <- 0 until masterCount
      j <- 1 to replicasPerMaster
    } withRedisClient(i + masterCount * j) { client =>
      val (host, port) = hostAndPort(redisAddress(i))
      ready(client.slaveOf(Buf.Utf8("127.0.0.1"), Buf.Utf8(port.toString)))
    }
  }

  private def masterName(i: Int) = "master" + i

  private def waitUntil(msg: String)(check: => Boolean) = {
    log.info(msg)
    val startTime = Time.now
    val until = startTime + 20.seconds
    def checkLater(): Future[Boolean] = {
      if (Time.now > until) Future.value(false)
      else
        DefaultTimer
          .doLater(1.second) {
            log.info("%s", Time.now - startTime)
            if (check) Future.value(true)
            else checkLater()
          }
          .flatten
    }
    assert(Await.result(checkLater, 25.seconds))
  }

  test("Correctly perform the MONITOR and MASTER/MASTERS command", RedisTest, ClientTest) {
    withSentinelClient(0) { client =>
      // No masters at the beginning
      assert(result(client.masters()) == Nil)

      val expected = (0 until masterCount).map { i =>
        val name = masterName(i)
        val address = redisAddress(i)
        ready(client.monitor(name, address.getHostString, address.getPort, sentinelCount + i))
        (name -> ((address.getHostString, address.getPort, sentinelCount + i)))
      }.toMap

      val masters = result(client.masters())
        .map(m => m.name -> ((m.ip, m.port, m.quorum)))
        .toMap
      assert(masters == expected)

      val oneByOne = expected.keys.map { name =>
        val m = result(client.master(name))
        (m.name -> ((m.ip, m.port, m.quorum)))
      }.toMap
      assert(oneByOne == expected)
    }
  }

  test("Correctly perform the SENTINELS command", RedisTest, ClientTest) {
    val address = redisAddress(0)
    withSentinelClient(0) { client0 =>
      // Errors should be throw for unknown names
      assert(result(client0.sentinels(noSuchMaster).liftToTry).isThrow)
      // SENTINALS return a list of OTHER sentinels
      assert(result(client0.sentinels(master0)).size == 0)

      (1 to 2).foreach { i =>
        withSentinelClient(i) { client =>
          ready(client.monitor(master0, address.getHostString, address.getPort, sentinelCount))
        }
      }
      waitUntil("Waiting the sentinel list to be updated ...") {
        val otherSentinels = result(client0.sentinels(master0))
        val actual = otherSentinels.map(_.port).toSet
        val expected = List(sentinelAddress(1).getPort, sentinelAddress(2).getPort).toSet
        actual == expected
      }
    }
  }

  test("Correctly perform the GET-MASTER-ADDRESS-BY-NAME command", RedisTest, ClientTest) {
    withSentinelClient(0) { client =>
      (0 until masterCount).map { i =>
        assert(result(client.getMasterAddrByName(noSuchMaster)).isEmpty)
        val address = result(client.getMasterAddrByName(masterName(i))).get
        assert(address.getHostString == redisAddress(i).getHostString)
        assert(address.getPort == redisAddress(i).getPort)
      }
    }
  }

  // CKQUORUM is introduced in redis 2.8.22. Please run this test with a newer redis server.
  test("Correctly perform the CKQUORUM command", RedisTest, ClientTest) {
    withSentinelClient(0) { client =>
      // Errors should be throw for unknown names
      assert(result(client.ckQuorum(noSuchMaster).liftToTry).isThrow)

      result(client.ckQuorum(masterName(0)))
      result(client.ckQuorum(masterName(1)).liftToTry).isThrow
    }
  }

  test("Correctly perform the SLAVES command", RedisTest, ClientTest) {
    withSentinelClient(0) { client =>
      // Errors should be throw for unknown names
      assert(result(client.slaves(noSuchMaster).liftToTry).isThrow)

      val expected = (0 until masterCount).map { i =>
        masterName(i) -> (1 to replicasPerMaster).map { j =>
          redisAddress(i + masterCount * j).getPort
        }.toSet
      }.toMap

      def replicas() =
        expected.keys.map { name =>
          name -> result(client.slaves(name))
            .map(s => s.port)
            .toSet
        }.toMap

      // Sentinel PINGs masters every 10 seconds to get the latest replica list.
      // We keep checking the replica lists periodically, until it is updated
      // to the latest.
      waitUntil("Waiting the replica list to be updated ...")(replicas == expected)
    }
  }

  test("Correctly perform the RESET command", RedisTest, ClientTest) {
    withSentinelClient(0) { client =>
      // Errors should be throw for unknown names
      assert(result(client.reset(noSuchMaster).liftToTry).isThrow)

      def numberOfReplicas = result(client.master(master1)).numSlaves

      assert(numberOfReplicas == 2)
      stopRedis(count + sentinelCount - 1)
      ready(client.reset(master1))
      waitUntil("Waiting the master to be reset ...")(numberOfReplicas == 1)
    }
  }

  test("Correctly perform the SET command", RedisTest, ClientTest) {
    withSentinelClient(0) { client =>
      val option = "down-after-milliseconds"
      // Errors should be throw for unknown names
      assert(result(client.set(noSuchMaster, option, "2000").liftToTry).isThrow)

      def currentValue = result(client.master(master0)).downAfterMilliseconds

      val oldValue = currentValue
      val newValue = oldValue + 1000
      ready(client.set(master0, option, newValue.toString))
      assert(currentValue == newValue)
    }
  }

  test("Correctly perform the FAILOVER command", RedisTest, ClientTest) {
    withSentinelClient(0) { client =>
      // Errors should be throw for unknown names
      assert(result(client.failover(noSuchMaster).liftToTry).isThrow)

      def currentMasterPort =
        result(client.getMasterAddrByName(master0))
          .map(_.getPort)
          .get

      val oldValue = currentMasterPort
      ready(client.failover(master0))
      waitUntil("Waiting failover ...")(currentMasterPort !== oldValue)
    }
  }

  test("Correctly perform the REMOVE command", RedisTest, ClientTest) {
    withSentinelClient(0) { client =>
      // Errors should be throw for unknown names
      assert(result(client.remove(noSuchMaster).liftToTry).isThrow)

      ready(client.remove(master0))
      result(client.master(master0).liftToTry).isThrow
      val masterNames = result(client.masters()).map(_.name)
      assert(masterNames == List(masterName(1)))
    }
  }

  test("Correctly perform the FLUSHCONFIG command", RedisTest, ClientTest) {
    withSentinelClient(0) { client =>
      val configFile = result(client.info(Buf.Utf8("server"))).flatMap { buf =>
        val prefix = "config_file:"
        BufToString(buf).trim
          .split("\n")
          .find(_.startsWith(prefix))
          .map(_.substring(prefix.length))
          .map(new java.io.File(_))
      }.get
      assert(configFile.delete())
      assert(!configFile.exists())
      ready(client.flushConfig())
      assert(configFile.exists())
    }
  }
}
