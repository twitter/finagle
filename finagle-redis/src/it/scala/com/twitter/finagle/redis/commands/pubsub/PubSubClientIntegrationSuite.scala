package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.Client
import com.twitter.finagle.redis.RedisClientTest
import com.twitter.finagle.redis.tags.{ClientTest, RedisTest}
import com.twitter.finagle.redis.util._
import com.twitter.io.Buf
import com.twitter.util._
import java.util.concurrent.atomic.AtomicInteger
import org.scalatest.Matchers._

final class PubSubClientIntegrationSuite extends RedisClientTest {

  test("Correctly perform the SUBSCRIBE/UNSUBSCRIBE command", RedisTest, ClientTest) {
    runTest { ctx =>
      ctx.subscribe("foo", "bar", "zoo")
      ctx.publish("foo")
      ctx.publish("bar")
      ctx.publish("zoo")
      ctx.unsubscribe("zoo", "bar")
      ctx.publish("foo")
      the[TimeoutException] thrownBy ctx.publish("bar")
      the[TimeoutException] thrownBy ctx.publish("zoo")
    }
  }

  test("Correctly perform the PSUBSCRIBE/PUNSUBSCRIBE command", RedisTest, ClientTest) {
    runTest { ctx =>
      ctx.pSubscribe("foo.*", "bar.*", "zoo.*")
      ctx.publish("foo.1", Some("foo.*"))
      ctx.publish("foo.2", Some("foo.*"))
      ctx.publish("bar.1", Some("bar.*"))
      ctx.publish("bar.2", Some("bar.*"))
      ctx.publish("zoo.1", Some("zoo.*"))
      ctx.publish("zoo.2", Some("zoo.*"))
      ctx.pUnsubscribe("foo.*", "bar.*")
      ctx.publish("zoo.1", Some("zoo.*"))
      ctx.publish("zoo.2", Some("zoo.*"))
      the[TimeoutException] thrownBy ctx.publish("foo.1")
      the[TimeoutException] thrownBy ctx.publish("foo.2")
      the[TimeoutException] thrownBy ctx.publish("bar.1")
      the[TimeoutException] thrownBy ctx.publish("bar.2")
    }
  }

  test("Correctly perform the PUBSUB CHANNELS command", RedisTest, ClientTest) {
    runTest { ctx =>
      ctx.subscribe("foo")
      assert(ctx.pubSubChannels() == Set("foo"))
      ctx.subscribe("bar")
      assert(ctx.pubSubChannels() == Set("foo", "bar"))
      ctx.unsubscribe("foo")
      assert(ctx.pubSubChannels() == Set("bar"))
    }
  }

  test("Correctly perform the PUBSUB NUMSUB command", RedisTest, ClientTest) {
    runTest { ctx =>
      assert(ctx.pubSubNumSub("foo") == Seq(0))
      ctx.subscribe("foo")
      assert(ctx.pubSubNumSub("foo") == Seq(1))
      ctx.unsubscribe("foo")
      assert(ctx.pubSubNumSub("foo") == Seq(0))
      ctx.subscribe("bar", "zoo")
      assert(ctx.pubSubNumSub("bar", "zoo") == Seq(1, 1))
    }
  }

  test("Correctly perform the PUBSUB NUMPAT command", RedisTest, ClientTest) {
    runTest { ctx =>
      assert(ctx.pubSubNumPat() == 0)
      ctx.pSubscribe("foo.*")
      assert(ctx.pubSubNumPat() == 1)
      ctx.pSubscribe("bar.*")
      assert(ctx.pubSubNumPat() == 2)
      ctx.pUnsubscribe("foo.*")
      assert(ctx.pubSubNumPat() == 1)
      ctx.pUnsubscribe("bar.*")
      assert(ctx.pubSubNumPat() == 0)
    }
  }

  test("Recover from network failure", RedisTest, ClientTest) {
    runTest { ctx =>
      ctx.subscribe("foo")
      ctx.publish("foo")

      val redis = RedisCluster.stop()
      the[Exception] thrownBy ctx.publish("foo")

      RedisCluster.start(redis)
      waitUntilAsserted("recover from network failure") { ctx.publish("foo") }
    }
  }

  def runTest(test: TestContext => Unit): Unit = {
    withRedisClient { c => test(new TestContext(c)) }
  }

  class TestContext(val c: Client) {

    val q = collection.mutable.HashMap[String, Promise[(String, String, Option[String])]]()

    def subscribe(channels: Buf*) = {
      result(c.subscribe(channels) {
        case (channel, message) =>
          q.get(message).map(_.setValue((channel, message, None)))
      })
    }

    def unsubscribe(channels: String*) = {
      result(c.unsubscribe(channels.map(s2b)))
    }

    def pSubscribe(patterns: String*) = {
      result(c.pSubscribe(patterns.map(s2b)) {
        case (pattern, channel, message) =>
          q.get(message).map(_.setValue((channel, message, Some(pattern))))
      })
    }

    def pUnsubscribe(patterns: String*) = {
      result(c.pUnsubscribe(patterns.map(s2b)))
    }

    def pubSubChannels(pattern: Option[String] = None) = {
      result(c.pubSubChannels(pattern.map(s2b))).map(b2s).toSet
    }

    def pubSubNumSub(channels: String*) = {
      val r = result(c.pubSubNumSub(channels.map(s2b)))
      channels.map(r(_))
    }

    def pubSubNumPat() = {
      result(c.pubSubNumPat())
    }

    def publish(channel: String, pattern: Option[String] = None): Unit = {
      val p = new Promise[(String, String, Option[String])]
      val message = nextMessage
      q.put(message, p)
      result(c.publish(channel, message))
      assert(result(p) == ((channel, message, pattern)))
    }

    val i = new AtomicInteger(0)

    def nextMessage = "message-" + i.incrementAndGet()
  }
}
