package com.twitter.finagle.redis.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.redis._
import com.twitter.finagle.redis.tags.{ClientTest, RedisTest}
import com.twitter.finagle.redis.util.RedisCluster
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Await, MockTimer, Return, Time}

final class ConnectionClientIntegrationSuite extends RedisClientTest {

  test("Correctly perform the SELECT command", RedisTest, ClientTest) {
    withRedisClient { client => assert(Await.result(client.select(1).liftToTry) == Return.Unit) }
  }

  test("Correctly perform the QUIT command", RedisTest, ClientTest) {
    withRedisClient { client => assert(Await.result(client.quit().liftToTry) == Return.Unit) }
  }

  test("Correctly perform the PING command without arguments", RedisTest, ClientTest) {
    withRedisClient { client => assert(Await.result(client.ping().liftToTry) == Return.Unit) }
  }

  test(
    "Partitioned client establishes new sessions after the previous session disconnects",
    RedisTest,
    ClientTest) {
    val timer = new MockTimer
    val maxLifeTime = 5.seconds
    val statsReceiver = new InMemoryStatsReceiver

    withRedisPartitionedClient(timer, statsReceiver, maxLifeTime) { partitionedClient =>
      Time.withCurrentTimeFrozen { timeControl =>
        // initial request
        assert(Await.result(partitionedClient.ping().liftToTry) == Return.Unit)
        timeControl.advance(maxLifeTime + 1.second)

        assert(statsReceiver.gauges(Seq(RedisCluster.hostAddresses(), "connections"))() == 1)
        assert(
          statsReceiver.counters
            .get(Seq(RedisCluster.hostAddresses(), "connects")).get == 1)
        assert(
          statsReceiver.counters
            .get(Seq(RedisCluster.hostAddresses(), "partitioner", "joins")).get == 1)
        assert(
          statsReceiver.counters
            .get(Seq(RedisCluster.hostAddresses(), "partitioner", "redistributes")).get == 1)

        timer.tick()
        // re-establish
        assert(Await.result(partitionedClient.ping().liftToTry) == Return.Unit)
        assert(statsReceiver.gauges(Seq(RedisCluster.hostAddresses(), "connections"))() == 1)
        assert(
          statsReceiver.counters
            .get(Seq(RedisCluster.hostAddresses(), "connects")).get == 2)
        assert(
          statsReceiver.counters
            .get(Seq(RedisCluster.hostAddresses(), "partitioner", "joins")).get == 1)
        assert(
          statsReceiver.counters
            .get(Seq(RedisCluster.hostAddresses(), "partitioner", "redistributes")).get == 1)
      }
    }
  }
}
