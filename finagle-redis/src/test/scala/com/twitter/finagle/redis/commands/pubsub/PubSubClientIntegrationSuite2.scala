package com.twitter.finagle.redis.integration

import com.twitter.finagle.Redis
import com.twitter.finagle.redis.Client
import com.twitter.finagle.redis.RedisClientTest
import com.twitter.finagle.redis.tags.{ClientTest, RedisTest}
import com.twitter.finagle.redis.util._
import com.twitter.io.Buf
import com.twitter.util._
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
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
      (1 to 10).foreach(_ => subscribeAndAssert(bufFoo))
      (1 to 10).foreach(_ => subscribeAndAssert(bufFoo, bufBar, bufBaz))
    }
  }

  test("Subscribe to a pattern only once", RedisTest, ClientTest) {
    runTest { implicit ctx =>
      (1 to 10).foreach(_ => pSubscribeAndAssert(bufFoo))
      assert(ctx.pubSubNumPat() == 1)
      (1 to 10).foreach(_ => pSubscribeAndAssert(bufFoo, bufBar, bufMoo))
      assert(ctx.pubSubNumPat() == 3)
    }
  }

  test("Correctly unsubscribe from channels", RedisTest, ClientTest) {
    runTest { implicit ctx =>
      subscribeAndAssert(bufFoo, bufBar, bufBaz, bufBoo, bufMoo, bufNum)
      unsubscribeAndAssert(bufFoo)
      unsubscribeAndAssert(bufFoo, bufBar, bufBaz)
    }
  }

  test("Correctly unsubscribe from patterns", RedisTest, ClientTest) {
    runTest { implicit ctx =>
      pSubscribeAndAssert(bufFoo, bufBar, bufBaz, bufBoo, bufMoo, bufNum)
      assert(ctx.pubSubNumPat() == 6)
      pUnsubscribeAndAssert(bufFoo)
      assert(ctx.pubSubNumPat() == 5)
      pUnsubscribeAndAssert(bufFoo, bufBar, bufBaz)
      assert(ctx.pubSubNumPat() == 3)
    }
  }

  test("Recover from network failures") {
    runTest { implicit ctx =>
      master.stop()
      slave.stop()
      the[Exception] thrownBy subscribeAndAssert(bufFoo, bufBar)
      the[Exception] thrownBy pSubscribeAndAssert(bufBaz, bufBoo)
      master.start()
      slave.start()
      ensureMasterSlave()
      waitUntilAsserted("recover from network failure") { assertSubscribed(bufFoo, bufBar) }
      waitUntilAsserted("recover from network failure") { assertPSubscribed(bufBaz, bufBoo) }
    }
  }

  def subscribeAndAssert(channels: Buf*)(implicit ctx: TestContext) {
    ctx.subscribe(channels)
    assertSubscribed(channels: _*)
  }

  def assertSubscribed(channels: Buf*)(implicit ctx: TestContext) {
    channels.foreach { channel =>
      assert(ctx.pubSubNumSub(channel) == 1)
      val messages = (1 to 10).map(_ => ctx.publish(channel))
      messages.foreach(message => assert(ctx.recvCount(message) == 1))
    }
  }

  def pSubscribeAndAssert(channels: Buf*)(implicit ctx: TestContext) {
    ctx.pSubscribe(channels)
    assertPSubscribed(channels: _*)
  }

  def assertPSubscribed(channels: Buf*)(implicit ctx: TestContext) {
    channels.foreach { channel =>
      val messages = (1 to 10).map(_ => ctx.publish(channel, Some(channel)))
      messages.foreach(message => assert(ctx.recvCount(message) == 1))
    }
  }

  def unsubscribeAndAssert(channels: Buf*)(implicit ctx: TestContext) {
    ctx.unsubscribe(channels)
    channels.foreach { channel =>
      assert(ctx.pubSubNumSub(channel) == 0)
      the[TimeoutException] thrownBy ctx.publish(channel)
    }
  }

  def pUnsubscribeAndAssert(channels: Buf*)(implicit ctx: TestContext) {
    ctx.pUnsubscribe(channels)
    channels.foreach { channel =>
      the[TimeoutException] thrownBy ctx.publish(channel)
    }
  }

  def ensureMasterSlave() {
    slave.withClient { client =>
      val masterAddr = master.address.get
      result(
        client.slaveOf(Buf.Utf8(masterAddr.getHostString), Buf.Utf8(masterAddr.getPort.toString))
      )
      waitUntil("master-slave replication") {
        val status = b2s(result(client.info(Buf.Utf8("replication"))).get)
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
        try test(ctx)
        finally subscribeClnt.close()
      }
    }
  }

  class TestContext(val masterClnt: Client, val slaveClnt: Client, val clusterClnt: Client) {

    val q = collection.mutable.HashMap[String, Promise[(String, String, Option[String])]]()
    val c = new ConcurrentHashMap[String, AtomicInteger]().asScala

    def subscribe(channels: Seq[Buf]) = {
      result(clusterClnt.subscribe(channels) {
        case (channel, message) =>
          q.get(message).map(_.setValue((channel, message, None)))
          c.getOrElseUpdate(message, new AtomicInteger(0)).getAndIncrement
      })
    }

    def unsubscribe(channels: Seq[Buf]) = {
      result(clusterClnt.unsubscribe(channels))
    }

    def pSubscribe(patterns: Seq[Buf]) = {
      result(clusterClnt.pSubscribe(patterns) {
        case (pattern, channel, message) =>
          q.get(message).map(_.setValue((channel, message, Some(pattern))))
          c.getOrElseUpdate(message, new AtomicInteger(0)).getAndIncrement
      })
    }

    def pUnsubscribe(patterns: Seq[Buf]) = {
      result(clusterClnt.pUnsubscribe(patterns))
    }

    def pubSubChannels(client: Client, pattern: Option[Buf] = None) = {
      result(client.pubSubChannels(pattern)).toSet
    }

    def pubSubNumSub(client: Client, channel: Buf): Long = {
      result(client.pubSubNumSub(Seq(channel))).head._2
    }

    def pubSubNumSub(channel: Buf): Long = {
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
