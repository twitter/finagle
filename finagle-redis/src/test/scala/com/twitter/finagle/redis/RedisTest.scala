package com.twitter.finagle.redis.naggati

import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.redis.naggati.test.TestCodec
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util._
import com.twitter.finagle.redis.{Client, TransactionalClient, SentinelClient}
import com.twitter.finagle.{Redis, redis}
import com.twitter.io.Buf
import com.twitter.util.{Await, Awaitable, Duration, Future, Try}
import java.net.InetSocketAddress
import org.jboss.netty.buffer.ChannelBuffer
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import scala.language.implicitConversions

trait RedisTest extends FunSuite {
  protected def wrap(s: String): ChannelBuffer = StringToChannelBuffer(s)
  protected def string2ChanBuf(s: String): ChannelBuffer = wrap(s)
  protected def chanBuf2String(cb: ChannelBuffer): String = CBToString(cb)

  protected val bufFoo = StringToBuf("foo")
  protected val bufBar = StringToBuf("bar")
  protected val bufBaz = StringToBuf("baz")
  protected val bufBoo = StringToBuf("boo")
  protected val bufMoo = StringToBuf("moo")
  protected val bufNum = StringToBuf("num")

  protected val foo = StringToChannelBuffer("foo")
  protected val bar = StringToChannelBuffer("bar")
  protected val baz = StringToChannelBuffer("baz")
  protected val boo = StringToChannelBuffer("boo")
  protected val moo = StringToChannelBuffer("moo")
  protected val num = StringToChannelBuffer("num")

  def result[T](awaitable: Awaitable[T], timeout: Duration = 1.second): T =
    Await.result(awaitable, timeout)

  def ready[T <: Awaitable[_]](awaitable: T, timeout: Duration = 1.second): T =
    Await.ready(awaitable, timeout)

  def waitUntil(message: String, countDown: Int = 10)(ready: => Boolean): Unit = {
    if (countDown > 0) {
      if (!ready) {
        Thread.sleep(1000)
        waitUntil(message, countDown - 1)(ready)
      }
    }
    else throw new IllegalStateException(s"Timeout: ${message}")
  }

  def waitUntilAsserted(message: String, countDown: Int = 10)(assert: => Unit): Unit = {
    waitUntil(message, countDown)(Try(assert).isReturn)
  }
}

trait RedisResponseTest extends RedisTest {
  protected val replyCodec = new ReplyCodec
  protected val (codec, counter) = TestCodec(replyCodec.decode, replyCodec.encode)
}

trait RedisRequestTest extends RedisTest {
  protected val commandCodec = new CommandCodec
  protected val (codec, counter) = TestCodec(commandCodec.decode, commandCodec.encode)


  def unwrap(list: Seq[AnyRef])(fn: PartialFunction[Command,Unit]) = list.toList match {
    case head :: Nil => head match {
      case c: Command => fn.isDefinedAt(c) match {
        case true => fn(c)
        case false => fail("Didn't find expected type in list: %s".format(c.getClass))
      }
      case _ => fail("Expected to find a command in the list")
    }
    case _ => fail("Expected single element list")
  }
}

trait RedisClientTest extends RedisTest with BeforeAndAfterAll {

  implicit def s2cb(s: String): ChannelBuffer = StringToChannelBuffer(s)
  implicit def cb2s(cb: ChannelBuffer): String = CBToString(cb)

  implicit def s2b(s: String): Buf = StringToBuf(s)
  implicit def b2s(b: Buf): String = BufToString(b)

  override def beforeAll(): Unit = RedisCluster.start()
  override def afterAll(): Unit = RedisCluster.stop()

  protected def withRedisClient(testCode: Client => Any): Unit = {
    val client = Redis.newRichClient(RedisCluster.hostAddresses())
    Await.result(client.flushAll)
    try {
      testCode(client)
    }
    finally {
      client.close()
    }
  }
}

trait SentinelClientTest extends RedisTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    RedisCluster.start(count = count, mode = RedisMode.Standalone)
    RedisCluster.start(count = sentinelCount, mode = RedisMode.Sentinel)
  }
  override def afterAll(): Unit = RedisCluster.stop()

  val sentinelCount: Int

  val count: Int

  def hostAndPort(address: InetSocketAddress) = {
    (address.getHostString, address.getPort)
  }

  def stopRedis(index: Int) = {
    RedisCluster.instanceStack(index).stop()
  }

  def redisAddress(index: Int) = {
    RedisCluster.address(sentinelCount + index).get
  }

  def sentinelAddress(index: Int) = {
    RedisCluster.address(index).get
  }

  protected def withRedisClient(testCode: TransactionalClient => Any) {
    withRedisClient(sentinelCount, sentinelCount + count)(testCode)
  }

  protected def withRedisClient(index: Int)(testCode: TransactionalClient => Any) {
    withRedisClient(sentinelCount + index, sentinelCount + index + 1)(testCode)
  }

  protected def withRedisClient(from: Int, until: Int)(testCode: TransactionalClient => Any) {
    val client = Redis.newTransactionalClient(RedisCluster.hostAddresses(from, until))
    try {
      testCode(client)
    }
    finally {
      client.close()
    }
  }

  protected def withSentinelClient(index: Int)(testCode: SentinelClient => Any) {
    val client = SentinelClient(
      ClientBuilder()
        .codec(new redis.Redis())
        .hosts(RedisCluster.hostAddresses(from = index, until = index + 1))
        .hostConnectionLimit(1)
        .buildFactory())
    try {
      testCode(client)
    }
    finally {
      client.close()
    }
  }
}

trait RedisClientServerIntegrationTest extends RedisTest with BeforeAndAfterAll {

  private[this] lazy val svcClient = ClientBuilder()
    .name("redis-client")
    .codec(redis.Redis())
    .hosts(RedisCluster.hostAddresses())
    .hostConnectionLimit(2)
    .retries(2)
    .build()

  private[this] val service = new Service[Command, Reply] {
    def apply(cmd: Command): Future[Reply] = {
      svcClient(cmd)
    }
  }

  private[this] val server = ServerBuilder()
    .name("redis-server")
    .codec(redis.Redis())
    .bindTo(new InetSocketAddress(0))
    .build(service)

  override def beforeAll(): Unit = RedisCluster.start()
  override def afterAll(): Unit = RedisCluster.stop()

  protected val OKStatusReply = StatusReply("OK")

  protected def withRedisClient(testCode: Service[Command, Reply] => Any): Unit = {
    val client = ClientBuilder()
      .name("redis-client")
      .codec(redis.Redis())
      .hosts(server.boundAddress.asInstanceOf[InetSocketAddress])
      .hostConnectionLimit(1)
      .retries(2)
      .build()
    Await.result(client(FlushAll))
    try {
      testCode(client)
    }
    finally {
      client.close()
    }
  }

  protected def assertMBulkReply(reply: Future[Reply], expects: List[String],
    contains: Boolean = false) = Await.result(reply) match {
      case MBulkReply(msgs) => contains match {
        case true =>
          assert(expects.isEmpty == false, "Test did no supply a list of expected replies.")
          val newMsgs = ReplyFormat.toString(msgs)
          expects.foreach({ msg =>
            val doesMBulkReplyContainMessage = newMsgs.contains(msg)
            assert(doesMBulkReplyContainMessage == true)
          })
        case false =>
          val actualMessages = ReplyFormat.toChannelBuffers(msgs).map({ msg =>
            chanBuf2String(msg)
          })
          assert(actualMessages == expects)
      }
      case EmptyMBulkReply() => {
        val isEmpty = true
        val actualReply = expects.isEmpty
        assert(actualReply == isEmpty)
      }
      case r: Reply => fail("Expected MBulkReply, got %s".format(r))
      case _ => fail("Expected MBulkReply")
  }

  def assertBulkReply(reply: Future[Reply], expects: String) = Await.result(reply) match {
    case BulkReply(msg) => assert(BufToString(msg) == expects)
    case _ => fail("Expected BulkReply")
  }

  def assertBulkReply(reply: Future[Reply], expects: Buf) = Await.result(reply) match {
    case BulkReply(msg) => assert(msg == expects)
    case _ => fail("Expected BulkReply")
  }
}
