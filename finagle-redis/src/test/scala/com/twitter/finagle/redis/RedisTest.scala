package com.twitter.finagle.redis

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util._
import com.twitter.finagle.Redis
import com.twitter.io.Buf
import com.twitter.util.{Await, Awaitable, Duration, Future, Try}
import org.scalatest.{BeforeAndAfterAll, Tag}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalacheck.{Arbitrary, Gen}
import java.net.InetSocketAddress
import org.scalactic.source.Position
import scala.language.implicitConversions
import java.lang.{Long => JLong}
import org.scalatest.funsuite.AnyFunSuite

trait RedisTest extends AnyFunSuite {

  protected val bufFoo = Buf.Utf8("foo")
  protected val bufBar = Buf.Utf8("bar")
  protected val bufBaz = Buf.Utf8("baz")
  protected val bufBoo = Buf.Utf8("boo")
  protected val bufMoo = Buf.Utf8("moo")
  protected val bufNum = Buf.Utf8("num")

  def result[T](awaitable: Awaitable[T], timeout: Duration = 5.second): T =
    Await.result(awaitable, timeout)

  def ready[T <: Awaitable[_]](awaitable: T, timeout: Duration = 5.second): T =
    Await.ready(awaitable, timeout)

  def waitUntil(message: String, countDown: Int = 10)(ready: => Boolean): Unit = {
    if (countDown > 0) {
      if (!ready) {
        Thread.sleep(1000)
        waitUntil(message, countDown - 1)(ready)
      }
    } else throw new IllegalStateException(s"Timeout: $message")
  }

  def waitUntilAsserted(message: String, countDown: Int = 10)(assert: => Unit): Unit = {
    waitUntil(message, countDown)(Try(assert).isReturn)
  }
}

trait MissingInstances {

  val Eol = Buf.Utf8("\r\n")

  def genNonEmptyString: Gen[String] =
    Gen.nonEmptyListOf(Gen.alphaChar).map(_.mkString)

  // Gen non empty Bufs (per Redis protocol)
  def genBuf: Gen[Buf] =
    for {
      s <- genNonEmptyString
      b <- Gen.oneOf(
        Buf.Utf8(s),
        Buf.ByteArray.Owned(s.getBytes("UTF-8"))
      )
    } yield b

  implicit val arbitraryBuf: Arbitrary[Buf] = Arbitrary(genBuf)

  def genOptionalJLong: Gen[Option[JLong]] =
    for {
      i <- Gen.choose(-1000L, 1000L)
      o <- Option(new JLong(i))
    } yield o

  implicit val arbitraryOptionJLong: Arbitrary[Option[JLong]] = Arbitrary(genOptionalJLong)

  def genStatusReply: Gen[StatusReply] = genNonEmptyString.map(StatusReply.apply)
  def genErrorReply: Gen[ErrorReply] = genNonEmptyString.map(ErrorReply.apply)
  def genIntegerReply: Gen[IntegerReply] = Arbitrary.arbLong.arbitrary.map(IntegerReply.apply)
  def genBulkReply: Gen[BulkReply] = genBuf.map(BulkReply.apply)

  def genFlatMBulkReply: Gen[MBulkReply] =
    for {
      n <- Gen.choose(1, 10)
      line <- Gen.listOfN(
        n,
        Gen.oneOf(genStatusReply, genErrorReply, genIntegerReply, genBulkReply)
      )
    } yield MBulkReply(line)

  def genMBulkReply(maxDeep: Int): Gen[MBulkReply] =
    if (maxDeep == 0) genFlatMBulkReply
    else
      for {
        n <- Gen.choose(1, 10)
        line <- Gen.listOfN(
          n,
          Gen.oneOf(
            genStatusReply,
            genErrorReply,
            genIntegerReply,
            genBulkReply,
            genMBulkReply(maxDeep - 1)
          )
        )
      } yield MBulkReply(line)

  def genReply: Gen[Reply] = Gen.oneOf(
    genStatusReply,
    genErrorReply,
    genIntegerReply,
    genBulkReply,
    genMBulkReply(10)
  )

  implicit def arbitraryStatusReply: Arbitrary[StatusReply] = Arbitrary(genStatusReply)
  implicit def arbitraryErrorReply: Arbitrary[ErrorReply] = Arbitrary(genErrorReply)
  implicit def arbitraryIntegerReply: Arbitrary[IntegerReply] = Arbitrary(genIntegerReply)
  implicit def arbitraryBulkReply: Arbitrary[BulkReply] = Arbitrary(genBulkReply)
  implicit def arbitraryMBulkReply: Arbitrary[MBulkReply] = Arbitrary(genMBulkReply(10))
  implicit def arbitraryReply: Arbitrary[Reply] = Arbitrary(genReply)
}

trait RedisResponseTest
    extends RedisTest
    with ScalaCheckDrivenPropertyChecks
    with MissingInstances {

  private[this] def chunk(buf: Buf): Gen[Seq[Buf]] =
    if (buf.isEmpty) Gen.const(Seq.empty[Buf])
    else
      Gen.choose(1, buf.length).flatMap { n =>
        val taken = math.min(buf.length, n)
        val a = buf.slice(0, taken)
        val b = buf.slice(taken, buf.length)

        chunk(b).map(rest => a +: rest)
      }

  def genChunkedReply[R <: Reply](implicit a: Arbitrary[R]): Gen[(R, Seq[Buf])] =
    a.arbitrary.flatMap(r => chunk(encodeReply(r)).map(chunks => r -> chunks))

  def testDecodingInChunks(replyAndChunks: (Reply, Seq[Buf])): Unit = replyAndChunks match {
    case (expected, chunks) =>
      val decoder = new StageDecoder(Reply.decode)
      val actual = chunks.map(c => decoder.absorb(c)).dropWhile(_ == null).headOption

      assert(actual.contains(expected))
  }

  def decodeReply(s: String): Option[Reply] =
    Option(new StageDecoder(Reply.decode).absorb(Buf.Utf8(s)))

  def encodeReply(r: Reply): Buf = r match {
    case StatusReply(s) => Buf.Utf8("+").concat(Buf.Utf8(s)).concat(Eol)
    case ErrorReply(s) => Buf.Utf8("-").concat(Buf.Utf8(s)).concat(Eol)
    case IntegerReply(i) => Buf.Utf8(":").concat(Buf.Utf8(i.toString)).concat(Eol)
    case BulkReply(buf) =>
      Buf
        .Utf8("$")
        .concat(Buf.Utf8(buf.length.toString))
        .concat(Eol)
        .concat(buf)
        .concat(Eol)
    case MBulkReply(replies) =>
      val header =
        Buf
          .Utf8("*")
          .concat(Buf.Utf8(replies.size.toString))
          .concat(Eol)
      val body = replies.map(encodeReply).reduce((a, b) => a.concat(b))
      header.concat(body)
    case _ => Buf.Empty
  }
}

trait RedisRequestTest extends RedisTest with ScalaCheckDrivenPropertyChecks with MissingInstances {

  def genZMember: Gen[ZMember] =
    for {
      d <- Arbitrary.arbitrary[Double]
      b <- genBuf
    } yield ZMember(d, b)

  implicit val arbitraryZMember: Arbitrary[ZMember] = Arbitrary(genZMember)

  def genZInterval: Gen[ZInterval] =
    for {
      d <- Arbitrary.arbitrary[Double]
      i <- Gen.oneOf(ZInterval.MAX, ZInterval.MIN, ZInterval.exclusive(d), ZInterval(d))
    } yield i

  implicit val arbitraryZInterval: Arbitrary[ZInterval] = Arbitrary(genZInterval)

  def genWeights: Gen[Weights] =
    Gen.nonEmptyListOf(Arbitrary.arbDouble.arbitrary).map(l => Weights(l.toArray))

  implicit val arbitraryWeights: Arbitrary[Weights] = Arbitrary(genWeights)

  def genAggregate: Gen[Aggregate] = Gen.oneOf(Aggregate.Max, Aggregate.Min, Aggregate.Sum)

  implicit val arbitraryAggregate: Arbitrary[Aggregate] = Arbitrary(genAggregate)

  def encodeCommand(c: Command): Seq[String] = {
    val strings = BufToString(Command.encode(c)).split("\r\n")

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
    forAll { key: Buf => assert(encodeCommand(f(key)) == c.split(" ").toSeq ++ Seq(key.asString)) }

    intercept[ClientError](f(Buf.Empty))
  }

  def checkMultiKey(c: String, f: Seq[Buf] => Command): Unit = {
    forAll(Gen.nonEmptyListOf(genBuf)) { keys =>
      assert(
        encodeCommand(f(keys)) == c.split(" ").toSeq ++ keys.map(_.asString)
      )
    }

    intercept[ClientError](encodeCommand(f(Seq.empty)))
    intercept[ClientError](encodeCommand(f(Seq(Buf.Empty))))
  }

  def checkSingleKeyMultiVal(c: String, f: (Buf, Seq[Buf]) => Command): Unit = {
    forAll(genBuf, Gen.nonEmptyListOf(genBuf)) { (key, vals) =>
      assert(
        encodeCommand(f(key, vals)) == c.split(" ").toSeq ++ Seq(key.asString) ++ vals.map(
          _.asString
        )
      )

    }

    intercept[ClientError](encodeCommand(f(Buf.Empty, Seq.empty)))
    intercept[ClientError](encodeCommand(f(Buf.Utf8("x"), Seq.empty)))
  }

  def checkSingleKeySingleVal(c: String, f: (Buf, Buf) => Command): Unit = {
    forAll { (key: Buf, value: Buf) =>
      assert(
        encodeCommand(f(key, value)) == c.split(" ").toSeq ++ Seq(key.asString, value.asString)
      )
    }

    intercept[ClientError](encodeCommand(f(Buf.Empty, Buf.Empty)))
    intercept[ClientError](encodeCommand(f(Buf.Utf8("x"), Buf.Empty)))
  }

  def checkSingleKeyOptionCount(c: String, f: (Buf, Option[JLong]) => Command): Unit = {
    forAll { (key: Buf, count: Option[JLong]) =>
      count match {
        case Some(_) =>
          assert(
            encodeCommand(f(key, count)) == c.split(" ").toSeq ++
              Seq(key.asString, count.get.toString)
          )
        case None =>
          assert(
            encodeCommand(f(key, count)) == c.split(" ").toSeq ++ Seq(key.asString)
          )
      }
    }
  }

  def checkSingleKeyDoubleVal(c: String, f: (Buf, Buf, Buf) => Command): Unit = {
    forAll { (key: Buf, value: Buf, value2: Buf) =>
      assert(
        encodeCommand(f(key, value, value2)) == c.split(" ").toSeq ++ Seq(
          key.asString,
          value.asString,
          value2.asString
        )
      )
    }

    intercept[ClientError](encodeCommand(f(Buf.Empty, Buf.Empty, Buf.Empty)))
    intercept[ClientError](encodeCommand(f(Buf.Utf8("x"), Buf.Empty, Buf.Empty)))
    intercept[ClientError](encodeCommand(f(Buf.Empty, Buf.Utf8("x"), Buf.Empty)))
    intercept[ClientError](encodeCommand(f(Buf.Empty, Buf.Empty, Buf.Utf8("x"))))
  }

  def checkSingleKeyArbitraryVal[A: Arbitrary](c: String, f: (Buf, A) => Command): Unit = {
    forAll { (key: Buf, value: A) =>
      assert(
        encodeCommand(f(key, value)) == c +: Seq(key.asString, value.toString)
      )
    }

    Arbitrary
      .arbitrary[A]
      .sample
      .foreach(a => intercept[ClientError](encodeCommand(f(Buf.Empty, a))))
  }

  def checkSingleKey2ArbitraryVals[A: Arbitrary, B: Arbitrary](
    c: String,
    f: (Buf, A, B) => Command
  ): Unit = {
    forAll { (key: Buf, a: A, b: B) =>
      assert(encodeCommand(f(key, a, b)) == c +: Seq(key.asString, a.toString, b.toString))
    }

    for {
      aa <- Arbitrary.arbitrary[A].sample
      bb <- Arbitrary.arbitrary[B].sample
    } intercept[ClientError](encodeCommand(f(Buf.Empty, aa, bb)))
  }
}

trait RedisClientTest extends RedisTest with BeforeAndAfterAll {

  override protected def test(
    testName: String,
    testTags: Tag*
  )(
    f: => Any
  )(
    implicit pos: Position
  ): Unit = {
    if (RedisTestHelper.redisServerExists) {
      super.test(testName, testTags: _*)(f)(pos)
    } else {
      ignore(testName)(f)(pos)
    }
  }

  implicit def s2b(s: String): Buf = Buf.Utf8(s)
  implicit def b2s(b: Buf): String = BufToString(b)

  override def beforeAll(): Unit = RedisCluster.start()
  override def afterAll(): Unit = RedisCluster.stopAll()

  protected def withRedisClient(testCode: Client => Any): Unit = {
    val client = Redis.newRichClient(RedisCluster.hostAddresses())
    Await.result(client.flushAll())
    try { testCode(client) }
    finally { client.close() }
  }

  protected def assertMBulkReply(
    reply: Future[Reply],
    expects: List[String],
    contains: Boolean = false
  ) = Await.result(reply) match {
    case MBulkReply(msgs) =>
      contains match {
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

  override def test(testName: String, testTags: Tag*)(f: => Any)(implicit pos: Position): Unit = {
    if (RedisTestHelper.redisServerExists) {
      super.test(testName, testTags: _*)(f)(pos)
    } else {
      ignore(testName)(f)(pos)
    }
  }

  override def beforeAll(): Unit = {
    RedisCluster.start(count = count, mode = RedisMode.Standalone)
    RedisCluster.start(count = sentinelCount, mode = RedisMode.Sentinel)
  }
  override def afterAll(): Unit = RedisCluster.stopAll()

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

  protected def withRedisClient(testCode: TransactionalClient => Any): Unit = {
    withRedisClient(sentinelCount, sentinelCount + count)(testCode)
  }

  protected def withRedisClient(index: Int)(testCode: TransactionalClient => Any): Unit = {
    withRedisClient(sentinelCount + index, sentinelCount + index + 1)(testCode)
  }

  protected def withRedisClient(from: Int, until: Int)(testCode: TransactionalClient => Any) = {
    val client = Redis.newTransactionalClient(RedisCluster.hostAddresses(from, until))
    try { testCode(client) }
    finally { client.close() }
  }

  protected def withSentinelClient(index: Int)(testCode: SentinelClient => Any): Unit = {
    val client = SentinelClient(
      Redis.client.newClient(RedisCluster.hostAddresses(from = index, until = index + 1))
    )
    try { testCode(client) }
    finally { client.close() }
  }
}
