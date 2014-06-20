package com.twitter.finagle.redis.naggati

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.redis.naggati.test.TestCodec
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util.{CBToString, RedisCluster, ReplyFormat, StringToChannelBuffer}
import com.twitter.finagle.redis.{Redis, TransactionalClient}
import com.twitter.util.Await
import org.jboss.netty.buffer.ChannelBuffer
import org.scalatest.{BeforeAndAfterAll, FunSuite}

trait FinagleRedisTest extends FunSuite {
  protected def wrap(s: String): ChannelBuffer = StringToChannelBuffer(s)
  protected def string2ChanBuf(s: String): ChannelBuffer = wrap(s)

  protected val foo = StringToChannelBuffer("foo")
  protected val bar = StringToChannelBuffer("bar")
  protected val baz = StringToChannelBuffer("baz")
  protected val boo = StringToChannelBuffer("boo")
  protected val moo = StringToChannelBuffer("moo")

}

trait FinagleRedisResponseTest extends FinagleRedisTest {
  protected val replyCodec = new ReplyCodec
  protected val (codec, counter) = TestCodec(replyCodec.decode, replyCodec.encode)
}

trait FinagleRedisRequestTest extends FinagleRedisTest {
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

trait FinagleRedisClientSuite extends FinagleRedisTest with BeforeAndAfterAll {

  override def beforeAll(configMap: Map[String, Any]): Unit = {
    RedisCluster.start()
  }

  override def afterAll(configMap: Map[String, Any]): Unit =  {
    RedisCluster.stop()
  }

  protected def withRedisClient(testCode: TransactionalClient => Any) {
    val client = TransactionalClient(
      ClientBuilder()
        .codec(new Redis())
        .hosts(RedisCluster.hostAddresses())
        .hostConnectionLimit(1)
        .buildFactory())
    Await.result(client.flushDB())
    try {
      testCode(client)
    }
    finally {
      client.release
    }
  }
}
