package com.twitter.finagle.redis.integration

import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.redis.naggati.test.TestCodec
import com.twitter.finagle.redis.naggati.RedisTest
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util._
import com.twitter.finagle.redis.{Redis, TransactionalClient}
import com.twitter.util.{Await, Future, Time}
import org.jboss.netty.buffer.ChannelBuffer
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import java.net.InetSocketAddress
import com.twitter.finagle.Service
import com.twitter.finagle.redis.Client

trait RedisClientServerIntegrationTest extends RedisTest with BeforeAndAfterAll {

  private[this] lazy val svcClient = ClientBuilder()
    .name("redis-client")
    .codec(Redis())
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
    .codec(Redis())
    .bindTo(new InetSocketAddress(0))
    .build(service)

  override def beforeAll(): Unit = RedisCluster.start()
  override def afterAll(): Unit = RedisCluster.stop()

  protected val OKStatusReply = StatusReply("OK")

  protected def withRedisClient(testCode: Service[Command, Reply] => Any) {
    val client = ClientBuilder()
          .name("redis-client")
          .codec(Redis())
          .hosts(server.boundAddress)
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
          assert(expects.isEmpty === false, "Test did no supply a list of expected replies.")
          val newMsgs = ReplyFormat.toString(msgs)
          expects.foreach({ msg =>
            val doesMBulkReplyContainMessage = newMsgs.contains(msg)
            assert(doesMBulkReplyContainMessage === true)
          })
        case false =>
          val actualMessages = ReplyFormat.toChannelBuffers(msgs).map({ msg =>
            chanBuf2String(msg)
          })
          assert(actualMessages === expects)
      }
      case EmptyMBulkReply() => {
        val isEmpty = true
        val actualReply = expects.isEmpty
        assert(actualReply === isEmpty)
      }
      case r: Reply => fail("Expected MBulkReply, got %s".format(r))
      case _ => fail("Expected MBulkReply")
    }

  def assertBulkReply(reply: Future[Reply], expects: String) = Await.result(reply) match {
    case BulkReply(msg) => assert(BytesToString(msg.array) === expects)
    case _ => fail("Expected BulkReply")
  }
}
