package com.twitter.finagle.redis.integration

import com.twitter.finagle.Redis
import com.twitter.finagle.redis.Client
import com.twitter.finagle.redis.naggati.RedisClientTest
import com.twitter.finagle.redis.tags.{ClientTest, RedisTest}
import com.twitter.finagle.redis.util._
import com.twitter.util._
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import org.jboss.netty.buffer.ChannelBuffer
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.Matchers._
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConverters._

@Ignore
@RunWith(classOf[JUnitRunner])
final class PubSubClientIntegrationSuite2 extends RedisClientTest {

  lazy val master: ExternalRedis = RedisCluster.start().head
  lazy val slave: ExternalRedis = RedisCluster.start().head

  override def beforeAll(): Unit = {
    ensureMasterSlave()
  }

  override def afterAll(): Unit = RedisCluster.stopAll()

  test("Subscribe to a channel only once", RedisTest, ClientTest) {
    runTest { implicit ctx =>
      (1 to 10).foreach(_ => subscribeAndAssert(foo))
      (1 to 10).foreach(_ => subscribeAndAssert(foo, bar, baz))
    }
  }

  test("Subscribe to a pattern only once", RedisTest, ClientTest) {
    runTest { implicit ctx =>
      (1 to 10).foreach(_ => pSubscribeAndAssert(foo))
      assert(ctx.pubSubNumPat() == 1)
      (1 to 10).foreach(_ => pSubscribeAndAssert(foo, baz, moo))
      assert(ctx.pubSubNumPat() == 3)
    }
  }

  test("Correctly unsubscribe from channels", RedisTest, ClientTest) {
    runTest { implicit ctx =>
      subscribeAndAssert(foo, bar, baz, boo, moo, num)
      unsubscribeAndAssert(foo)
      unsubscribeAndAssert(foo, bar, baz)
    }
  }

  test("Correctly unsubscribe from patterns", RedisTest, ClientTest) {
    runTest { implicit ctx =>
      pSubscribeAndAssert(foo, bar, baz, boo, moo, num)
      assert(ctx.pubSubNumPat() == 6)
      pUnsubscribeAndAssert(foo)
      assert(ctx.pubSubNumPat() == 5)
      pUnsubscribeAndAssert(foo, bar, baz)
      assert(ctx.pubSubNumPat() == 3)
    }
  }

  test("Recover from network failures") {
    runTest { implicit ctx =>
      master.stop()
      slave.stop()
      the[Exception] thrownBy subscribeAndAssert(foo, bar)
      the[Exception] thrownBy pSubscribeAndAssert(baz, boo)
      master.start()
      slave.start()
      ensureMasterSlave()
      waitUntilAsserted("recover from network failure") { assertSubscribed(foo, bar) }
      waitUntilAsserted("recover from network failure") { assertPSubscribed(baz, boo) }
    }
  }

  def subscribeAndAssert(channels: ChannelBuffer*)(implicit ctx: TestContext) {
    ctx.subscribe(channels)
    assertSubscribed(channels: _*)
  }

  def assertSubscribed(channels: ChannelBuffer*)(implicit ctx: TestContext) {
    channels.foreach { channel =>
      assert(ctx.pubSubNumSub(channel) == 1)
      val messages = (1 to 10).map(_ => ctx.publish(channel))
      messages.foreach(message => assert(ctx.recvCount(message) == 1))
    }
  }

  def pSubscribeAndAssert(channels: ChannelBuffer*)(implicit ctx: TestContext) {
    ctx.pSubscribe(channels)
    assertPSubscribed(channels: _*)
  }

  def assertPSubscribed(channels: ChannelBuffer*)(implicit ctx: TestContext) {
    channels.foreach { channel =>
      val messages = (1 to 10).map(_ => ctx.publish(channel, Some(channel)))
      messages.foreach(message => assert(ctx.recvCount(message) == 1))
    }
  }

  def unsubscribeAndAssert(channels: ChannelBuffer*)(implicit ctx: TestContext) {
    ctx.unsubscribe(channels)
    channels.foreach { channel =>
      assert(ctx.pubSubNumSub(channel) == 0)
      the[TimeoutException] thrownBy ctx.publish(channel)
    }
  }

  def pUnsubscribeAndAssert(channels: ChannelBuffer*)(implicit ctx: TestContext) {
    ctx.pUnsubscribe(channels)
    channels.foreach { channel =>
      the[TimeoutException] thrownBy ctx.publish(channel)
    }
  }

  def ensureMasterSlave() {
    slave.withClient { client =>
      val masterAddr = master.address.get
      result(client.slaveOf(masterAddr.getHostString, masterAddr.getPort.toString))
      waitUntil("master-slave replication") {
        val status = b2s(result(client.info(StringToBuf("replication"))).get)
          .split("\n")
          .map(_.trim)
          .find(_.startsWith("master_link_status:"))
          .map(_.substring("master_link_status:".length))
        status == Some("up")
      }
    }
  }

  def runTest(test: TestContext => Unit) {
    master.withClient { masterClnt =>
      slave.withClient { slaveClnt =>
        val dest = Seq(master, slave)
          .map(node => s"127.0.0.1:${node.address.get.getPort}")
          .mkString(",")
        val subscribeClnt = Redis.newRichClient(dest)
        val ctx = new TestContext(masterClnt, slaveClnt, subscribeClnt)
        try test(ctx) finally subscribeClnt.close()
      }
    }
  }

  class TestContext(val masterClnt: Client, val slaveClnt: Client, val clusterClnt: Client) {

    val q = collection.mutable.HashMap[String, Promise[(String, String, Option[String])]]()
    val c = new ConcurrentHashMap[String, AtomicInteger]().asScala

    def subscribe(channels: Seq[ChannelBuffer]) = {
      result(clusterClnt.subscribe(channels) {
        case (channel, message) =>
          q.get(message).map(_.setValue((channel, message, None)))
          c.getOrElseUpdate(message, new AtomicInteger(0)).getAndIncrement
      })
    }

    def unsubscribe(channels: Seq[ChannelBuffer]) = {
      result(clusterClnt.unsubscribe(channels))
    }

    def pSubscribe(patterns: Seq[ChannelBuffer]) = {
      result(clusterClnt.pSubscribe(patterns) {
        case (pattern, channel, message) =>
          q.get(message).map(_.setValue((channel, message, Some(pattern))))
          c.getOrElseUpdate(message, new AtomicInteger(0)).getAndIncrement
      })
    }

    def pUnsubscribe(patterns: Seq[ChannelBuffer]) = {
      result(clusterClnt.pUnsubscribe(patterns))
    }

    def pubSubChannels(client: Client, pattern: Option[ChannelBuffer] = None) = {
      result(client.pubSubChannels(pattern)).toSet
    }

    def pubSubNumSub(client: Client, channel: ChannelBuffer): Long = {
      result(client.pubSubNumSub(Seq(channel))).head._2
    }

    def pubSubNumSub(channel: ChannelBuffer): Long = {
      List(masterClnt, slaveClnt).map(pubSubNumSub(_, channel)).sum
    }

    def pubSubNumPat(): Long = {
      List(masterClnt, slaveClnt).map(clnt => result(clnt.pubSubNumPat())).sum
    }

    def publish(channel: String, pattern: Option[String] = None) = {
      val p = new Promise[(String, String, Option[String])]
      val message = nextMessage
      q.put(message, p)
      result(masterClnt.publish(channel, message))
      assert(result(p) == ((channel, message, pattern)))
      message
    }

    def recvCount(message: String) = c.get(message).map(_.get()).getOrElse(0)

    val i = new AtomicInteger(0)

    def nextMessage = "message-" + i.incrementAndGet()
  }
}
