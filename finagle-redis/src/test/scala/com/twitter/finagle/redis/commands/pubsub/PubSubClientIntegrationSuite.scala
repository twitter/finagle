package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.naggati.RedisClientTest
import com.twitter.finagle.redis.tags.{ ClientTest, RedisTest }
import com.twitter.util.Await
import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.time._
import com.twitter.finagle.redis.Client
import com.twitter.finagle.redis.SubscribeClient
import com.twitter.finagle.redis.util._
import com.twitter.util._
import java.util.concurrent.atomic.AtomicInteger
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers._
import org.jboss.netty.buffer.ChannelBuffer

@Ignore
@RunWith(classOf[JUnitRunner])
final class PubSubClientIntegrationSuite extends RedisClientTest {

  implicit def s2cb(s: String) = StringToChannelBuffer(s)
  implicit def cb2s(cb: ChannelBuffer) = CBToString(cb)

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
  
  test("Recover from connection failure", RedisTest, ClientTest) {
    runTest { ctx =>
      ctx.subscribe("foo")
      ctx.publish("foo")
      
      val redis = RedisCluster.stop()
      the[Exception] thrownBy ctx.publish("foo")
      
      RedisCluster.start(redis)
      def loop(count: Int = 10) {
        Try(ctx.publish("foo")).onFailure { _ =>
          Thread.sleep(1000)
          loop(count - 1)
        }
      }
      loop()
    }
  }

  def runTest(test: TestContext => Unit) {
    withSubscribeClient { sc =>
      withRedisClient { c =>
        test(new TestContext(sc, c))
      }
    }
  }

  class TestContext(val sc: SubscribeClient, val c: Client) {

    val q = collection.mutable.HashMap[String, Promise[(String, String, Option[String])]]()

    def subscribe(channels: String*) {
      Await.ready(sc.subscribe(channels.map(s2cb)) { (channel, message) =>
        q.get(message).map(_.setValue((channel, message, None)))
      })
    }

    def unsubscribe(channels: String*) {
      Await.ready(sc.unsubscribe(channels.map(s2cb)))
    }

    def pSubscribe(patterns: String*) {
      Await.ready(sc.pSubscribe(patterns.map(s2cb)) { (pattern, channel, message) =>
        q.get(message).map(_.setValue((channel, message, Some(pattern))))
      })
    }

    def pUnsubscribe(patterns: String*) {
      Await.ready(sc.pUnsubscribe(patterns.map(s2cb)))
    }
    
    def pubSubChannels(pattern: Option[String] = None) = {
      Await.result(c.pubSubChannels(pattern.map(s2cb))).map(cb2s).toSet
    }

    def pubSubNumSub(channels: String*) = {
      val result = Await.result(c.pubSubNumSub(channels.map(s2cb)))
      channels.map(result(_))
    }

    def pubSubNumPat() = {
      Await.result(c.pubSubNumPat())
    }

    def publish(channel: String, pattern: Option[String] = None) {
      val p = new Promise[(String, String, Option[String])]
      val message = nextMessage
      q.put(message, p)
      Await.result(c.publish(channel, message))
      assert(Await.result(p, 1.second) == (channel, message, pattern))
    }
    
    val i = new AtomicInteger(0)
    
    def nextMessage = "message-" + i.incrementAndGet()
  }
}
