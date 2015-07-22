package com.twitter.finagle.redis.naggati

import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.redis.naggati.test.TestCodec
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util._
import com.twitter.finagle.redis.{Redis, TransactionalClient}
import com.twitter.util.{Await, Future, Time}
import org.jboss.netty.buffer.ChannelBuffer
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import java.net.InetSocketAddress
import com.twitter.finagle.Service
import com.twitter.finagle.redis.Client

trait RedisTest extends FunSuite {
  protected def wrap(s: String): ChannelBuffer = StringToChannelBuffer(s)
  protected def string2ChanBuf(s: String): ChannelBuffer = wrap(s)
  protected def chanBuf2String(cb: ChannelBuffer): String = CBToString(cb)

  protected val foo = StringToChannelBuffer("foo")
  protected val bar = StringToChannelBuffer("bar")
  protected val baz = StringToChannelBuffer("baz")
  protected val boo = StringToChannelBuffer("boo")
  protected val moo = StringToChannelBuffer("moo")
  protected val num = StringToChannelBuffer("num")
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

  override def beforeAll(): Unit = RedisCluster.start()
  override def afterAll(): Unit = RedisCluster.stop()

  protected def withRedisClient(testCode: TransactionalClient => Any) {
    val client = TransactionalClient(
      ClientBuilder()
        .codec(new Redis())
        .hosts(RedisCluster.hostAddresses())
        .hostConnectionLimit(1)
        .buildFactory())
    Await.result(client.flushAll)
    try {
      testCode(client)
    }
    finally {
      client.release
    }
  }
}

