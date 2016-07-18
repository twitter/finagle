package com.twitter.finagle.redis

import com.twitter.conversions.time._
import com.twitter.finagle.redis.naggati.test.TestCodec
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util._
import com.twitter.finagle.Redis
import com.twitter.finagle.redis.naggati._
import com.twitter.io.Buf
import com.twitter.util.{Await, Awaitable, Duration, Future, Try}
import org.jboss.netty.buffer.ChannelBuffer
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalacheck.{Arbitrary, Gen}
import java.net.InetSocketAddress
import scala.language.implicitConversions

trait RedisTest extends FunSuite {

  protected def wrap(s: String): ChannelBuffer = StringToChannelBuffer(s)

  protected val bufFoo = StringToBuf("foo")
  protected val bufBar = StringToBuf("bar")
  protected val bufBaz = StringToBuf("baz")
  protected val bufBoo = StringToBuf("boo")
  protected val bufMoo = StringToBuf("moo")
  protected val bufNum = StringToBuf("num")

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
  protected val (codec, counter) = TestCodec(replyCodec.decode, Codec.NONE)
}

trait RedisRequestTest extends RedisTest with GeneratorDrivenPropertyChecks {

  case class NelList[A](list: List[A])

  def genNelList[A: Arbitrary]: Gen[NelList[A]] =
    Gen.nonEmptyListOf(Arbitrary.arbitrary[A]).map(NelList.apply)

  implicit def arbitraryNelList[A: Arbitrary]: Arbitrary[NelList[A]] = Arbitrary(genNelList[A])

  // Gen non empty Bufs (per Redis protocol)
  def genBuf: Gen[Buf] = for {
    s <- Gen.alphaStr.suchThat(_.nonEmpty)
    b <- Gen.oneOf(
      Buf.Utf8(s),
      Buf.ByteArray.Owned(s.getBytes("UTF-8"))
    )
  } yield b

  implicit val arbitraryBuf: Arbitrary[Buf] = Arbitrary(genBuf)

  def genZMember: Gen[ZMember] = for {
    d <- Arbitrary.arbitrary[Double]
    b <- genBuf
  } yield ZMember(d, b)


  implicit val arbitraryZMember: Arbitrary[ZMember] = Arbitrary(genZMember)

  def genZInterval: Gen[ZInterval] = for {
    d <- Arbitrary.arbitrary[Double]
    i <- Gen.oneOf(ZInterval.MAX, ZInterval.MIN, ZInterval.exclusive(d), ZInterval(d))
  } yield i

  implicit val arbitraryZInterval: Arbitrary[ZInterval] = Arbitrary(genZInterval)

  def genWeights: Gen[Weights] =
    genNelList[Double].map(nel => Weights(nel.list.toArray))

  implicit val arbitraryWeights: Arbitrary[Weights] = Arbitrary(genWeights)

  def genAgregate: Gen[Aggregate] = Gen.oneOf(Aggregate.Max, Aggregate.Min, Aggregate.Sum)

  implicit val arbitraryAgregate: Arbitrary[Aggregate] = Arbitrary(genAgregate)

  def encode(c: Command): Seq[String] = {
    val strings = CBToString(c.toChannelBuffer).split("\r\n")

    val length = strings.head.toList match {
      case '*' :: rest => rest.mkString.toInt
      case _ => fail("* is expected")
    }

    val result = strings.tail.grouped(2).foldRight(List.empty[String]) {
      case (Array(argLength, arg), acc) =>
        val al = argLength.toList match {
          case '$' :: rest => rest.mkString.toInt
          case _ => fail("$ is expected")
        }
        // Make sure $n corresponds to the argument length.
        assert(al == arg.length)
        arg :: acc
    }

    // Make sure *n corresponds to the number of arguments.
    assert(length == result.length)
    result
  }

  implicit class BufAsString(b: Buf) {
    def asString: String = b match {
      case Buf.Utf8(s) => s
    }
  }

  def checkSingleKey(c: String, f: Buf => Command): Unit = {
    forAll { key: Buf =>
      assert(encode(f(key)) == c +: Seq(key.asString))
    }

    intercept[ClientError](f(Buf.Empty))
  }

  def checkMultiKey(c: String, f: Seq[Buf] => Command): Unit = {
    forAll { keys: NelList[Buf] =>
      assert(
        encode(f(keys.list)) == c +: keys.list.map(_.asString)
      )
    }

    intercept[ClientError](encode(f(Seq.empty)))
    intercept[ClientError](encode(f(Seq(Buf.Empty))))
  }

  def checkSingleKeyMultiVal(c: String, f: (Buf, Seq[Buf]) => Command): Unit = {
    forAll { (key: Buf, vals: NelList[Buf]) =>
      assert(
        encode(f(key, vals.list)) == c +: key.asString +: vals.list.map(_.asString)
      )
    }

    intercept[ClientError](encode(f(Buf.Empty, Seq.empty)))
    intercept[ClientError](encode(f(Buf.Utf8("x"), Seq.empty)))
  }

  def checkSingleKeySingleVal(c: String, f: (Buf, Buf) => Command): Unit = {
    forAll { (key: Buf, value: Buf) =>
      assert(
        encode(f(key, value)) == c +: Seq(key.asString, value.asString)
      )
    }

    intercept[ClientError](encode(f(Buf.Empty, Buf.Empty)))
    intercept[ClientError](encode(f(Buf.Utf8("x"), Buf.Empty)))
  }

  def checkSingleKeyArbitraryVal[A: Arbitrary](c: String, f: (Buf, A) => Command): Unit = {
    forAll { (key: Buf, value: A) =>
      assert(
        encode(f(key, value)) == c +: Seq(key.asString, value.toString)
      )
    }

    Arbitrary.arbitrary[A].sample.foreach(a =>
      intercept[ClientError](encode(f(Buf.Empty, a)))
    )
  }

  def checkSingleKey2ArbitraryVals[A: Arbitrary, B: Arbitrary](
    c: String, f: (Buf, A, B) => Command
  ): Unit = {
    forAll { (key: Buf, a: A, b: B) =>
      assert(encode(f(key, a, b)) == c +: Seq(key.asString, a.toString, b.toString))
    }

    for {
      aa <- Arbitrary.arbitrary[A].sample
      bb <- Arbitrary.arbitrary[B].sample
    } intercept[ClientError](encode(f(Buf.Empty, aa, bb)))
  }
}

trait RedisClientTest extends RedisTest with BeforeAndAfterAll {

  implicit def s2b(s: String): Buf = StringToBuf(s)
  implicit def b2s(b: Buf): String = BufToString(b)

  override def beforeAll(): Unit = RedisCluster.start()
  override def afterAll(): Unit = RedisCluster.stop()

  protected def withRedisClient(testCode: Client => Any): Unit = {
    val client = Redis.newRichClient(RedisCluster.hostAddresses())
    Await.result(client.flushAll())
    try { testCode(client) } finally { client.close() }
  }

  protected def assertMBulkReply(reply: Future[Reply], expects: List[String],
    contains: Boolean = false) = Await.result(reply) match {
    case MBulkReply(msgs) => contains match {
      case true =>
        assert(!expects.isEmpty, "Test did no supply a list of expected replies.")
        val newMsgs = ReplyFormat.toString(msgs)
        expects.foreach({ msg =>
          val doesMBulkReplyContainMessage = newMsgs.contains(msg)
          assert(doesMBulkReplyContainMessage)
        })
      case false =>
        val actualMessages = ReplyFormat.toBuf(msgs).flatMap(Buf.Utf8.unapply)
        assert(actualMessages == expects)
    }

    case EmptyMBulkReply =>
      val isEmpty = true
      val actualReply = expects.isEmpty
      assert(actualReply == isEmpty)

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

  protected def withRedisClient(from: Int, until: Int)(testCode: TransactionalClient => Any) = {
    val client = Redis.newTransactionalClient(RedisCluster.hostAddresses(from, until))
    try { testCode(client) } finally { client.close() }
  }

  protected def withSentinelClient(index: Int)(testCode: SentinelClient => Any) {
    val client = SentinelClient(
      Redis.client.newClient(RedisCluster.hostAddresses(from = index, until = index + 1)))
    try { testCode(client) } finally { client.close() }
  }
}
