package com.twitter.finagle.redis.integration

import com.twitter.finagle.Redis
import com.twitter.finagle.redis.Client
import com.twitter.finagle.redis.RedisClientTest
import com.twitter.finagle.redis.tags.{ClientTest, RedisTest}
import com.twitter.finagle.redis.util._
import com.twitter.io.Buf
import com.twitter.util._
import java.util.concurrent.atomic.AtomicInteger
import org.scalatest.Matchers._

final class PubSubClientIntegrationSuite2 extends RedisClientTest {

  lazy val master: ExternalRedis = RedisCluster.start().head
  lazy val replica: ExternalRedis = RedisCluster.start().head

  override def beforeAll(): Unit = {
    ensureMasterReplica()
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
      replica.stop()
      the[Exception] thrownBy subscribeAndAssert(bufFoo, bufBar)
      the[Exception] thrownBy pSubscribeAndAssert(bufBaz, bufBoo)
      master.start()
      replica.start()
      ensureMasterReplica()
      waitUntilAsserted("recover from network failure") { assertSubscribed(bufFoo, bufBar) }
      waitUntilAsserted("recover from network failure") { assertPSubscribed(bufBaz, bufBoo) }
    }
  }

  def subscribeAndAssert(channels: Buf*)(implicit ctx: TestContext): Unit = {
    ctx.subscribe(channels)
    assertSubscribed(channels: _*)
  }

  def assertSubscribed(channels: Buf*)(implicit ctx: TestContext): Unit = {
    channels.foreach { channel =>
      assert(ctx.pubSubNumSub(channel) == 1)
      val messages = (1 to 10).map(_ => ctx.publish(channel))
      messages.foreach(message => assert(ctx.recvCount(message) == 1))
    }
  }

  def pSubscribeAndAssert(channels: Buf*)(implicit ctx: TestContext): Unit = {
    ctx.pSubscribe(channels)
    assertPSubscribed(channels: _*)
  }

  def assertPSubscribed(channels: Buf*)(implicit ctx: TestContext): Unit = {
    channels.foreach { channel =>
      val messages = (1 to 10).map(_ => ctx.publish(channel, Some(channel)))
      messages.foreach(message => assert(ctx.recvCount(message) == 1))
    }
  }

  def unsubscribeAndAssert(channels: Buf*)(implicit ctx: TestContext): Unit = {
    ctx.unsubscribe(channels)
    channels.foreach { channel =>
      assert(ctx.pubSubNumSub(channel) == 0)
      the[TimeoutException] thrownBy ctx.publish(channel)
    }
  }

  def pUnsubscribeAndAssert(channels: Buf*)(implicit ctx: TestContext): Unit = {
    ctx.pUnsubscribe(channels)
    channels.foreach { channel => the[TimeoutException] thrownBy ctx.publish(channel) }
  }

  def ensureMasterReplica(): Unit = {
    replica.withClient { client =>
      val masterAddr = master.address.get
      result(
        client.slaveOf(Buf.Utf8(masterAddr.getHostString), Buf.Utf8(masterAddr.getPort.toString))
      )
      waitUntil("master-replica replication") {
        val status = b2s(result(client.info(Buf.Utf8("replication"))).get)
          .split("\n")
          .map(_.trim)
          .find(_.startsWith("master_link_status:"))
          .map(_.substring("master_link_status:".length))
        status == Some("up")
      }
    }
  }

  def runTest(test: TestContext => Unit): Unit = {
    master.withClient { masterClnt =>
      replica.withClient { replicaClnt =>
        val dest = Seq(master, replica)
          .map(node => s"127.0.0.1:${node.address.get.getPort}")
          .mkString(",")
        val subscribeClnt = Redis.newRichClient(dest)
        val ctx = new TestContext(masterClnt, replicaClnt, subscribeClnt)
        try test(ctx)
        finally subscribeClnt.close()
      }
    }
  }

  class TestContext(val masterClnt: Client, val replicaClnt: Client, val clusterClnt: Client) {

    private[this] val i = new AtomicInteger(0)
    // We want subscription updates to be atomic so access to these
    // two collections are mediated by the `lock`.
    private[this] val lock = new Object
    private[this] val q =
      collection.mutable.HashMap[String, Promise[(String, String, Option[String])]]()
    private[this] val c = collection.mutable.HashMap[String, AtomicInteger]()

    def subscribe(channels: Seq[Buf]): Map[Buf, Throwable] = {
      result(clusterClnt.subscribe(channels) {
        case (channel, message) =>
          val pending = lock.synchronized {
            c.getOrElseUpdate(message, new AtomicInteger(0)).getAndIncrement
            q.get(message)
          }
          pending.map(_.setValue((channel, message, None)))
      })
    }

    def unsubscribe(channels: Seq[Buf]): Map[Buf, Throwable] = {
      result(clusterClnt.unsubscribe(channels))
    }

    def pSubscribe(patterns: Seq[Buf]): Map[Buf, Throwable] = {
      result(clusterClnt.pSubscribe(patterns) {
        case (pattern, channel, message) =>
          val pending = lock.synchronized {
            c.getOrElseUpdate(message, new AtomicInteger(0)).getAndIncrement
            q.get(message)
          }
          pending.map(_.setValue((channel, message, Some(pattern))))
      })
    }

    def pUnsubscribe(patterns: Seq[Buf]): Map[Buf, Throwable] = {
      result(clusterClnt.pUnsubscribe(patterns))
    }

    def pubSubChannels(client: Client, pattern: Option[Buf] = None): Set[Buf] = {
      result(client.pubSubChannels(pattern)).toSet
    }

    def pubSubNumSub(client: Client, channel: Buf): Long = {
      result(client.pubSubNumSub(Seq(channel))).head._2
    }

    def pubSubNumSub(channel: Buf): Long = {
      List(masterClnt, replicaClnt).map(pubSubNumSub(_, channel)).sum
    }

    def pubSubNumPat(): Long = {
      List(masterClnt, replicaClnt).map(clnt => result(clnt.pubSubNumPat())).sum
    }

    def publish(channel: String, pattern: Option[String] = None): String = {
      val p = new Promise[(String, String, Option[String])]
      val message = nextMessage
      lock.synchronized(q.put(message, p))
      result(masterClnt.publish(channel, message))
      assert(result(p) == ((channel, message, pattern)))
      message
    }

    def recvCount(message: String): Int =
      lock.synchronized(c.get(message)).map(_.get()).getOrElse(0)

    private[this] def nextMessage: String = "message-" + i.incrementAndGet()
  }
}
