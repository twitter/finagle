package com.twitter.finagle.redis.unit

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.addr.WeightedAddress
import com.twitter.finagle.naming.BindingFactory
import com.twitter.finagle.partitioning.zk.ZkMetadata
import com.twitter.finagle.redis.RedisPartitioningService
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.stack.nilStack
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.io.Buf
import com.twitter.util.Await
import com.twitter.util.Duration
import com.twitter.util.Future
import java.net.InetAddress
import java.net.InetSocketAddress
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatestplus.mockito.MockitoSugar
import scala.collection.{Set => SSet}
import org.scalatest.funsuite.AnyFunSuite

class RedisPartitioningServiceTest extends AnyFunSuite with MockitoSugar with BeforeAndAfterEach {

  private[this] val clientName = "unit_test"
  private[this] val Timeout: Duration = 15.seconds
  private[this] val numServers = 5

  private[this] var mockService: Service[Command, Reply] = _
  private[this] var client: redis.Client = _
  private[this] var statsReceiver: InMemoryStatsReceiver = _

  def newAddress(port: Int, shardId: Int, weight: Int = 1): Address = {
    val inet = new InetSocketAddress(InetAddress.getLoopbackAddress, port)
    val md = ZkMetadata.toAddrMetadata(ZkMetadata(Some(shardId)))
    val addr = new Address.Inet(inet, md) {
      override def toString: String = s"Address($port)-($shardId)"
    }
    WeightedAddress(addr, weight)
  }

  private[this] def createClient(dest: Name, sr: InMemoryStatsReceiver) = {
    val stack: Stack[ServiceFactory[Command, Reply]] = {
      val builder = new StackBuilder[ServiceFactory[Command, Reply]](nilStack[Command, Reply])
      builder.push(MockService.module)
      builder.push(RedisPartitioningService.module)
      builder.push(BindingFactory.module)
      builder.result
    }

    val params: Stack.Params = Stack.Params.empty +
      param.Stats(sr.scope(clientName)) +
      BindingFactory.Dest(dest)

    redis.Client(ServiceFactory.const(await(stack.make(params)())))
  }

  private[this] def await[T](f: Future[T]): T = Await.result(f, Timeout)

  override protected def beforeEach(): Unit = {
    mockService = mock[Service[Command, Reply]]
    statsReceiver = new InMemoryStatsReceiver
    client = createClient(
      dest = Name.bound((1 to numServers).map(i => newAddress(i, i)): _*),
      statsReceiver
    )
  }

  override protected def afterEach(): Unit = {
    client.close()
  }

  def assertPartitionerStats(): Unit =
    assert(statsReceiver.counters(Seq(clientName, "partitioner", "redistributes")) == 1)

  private[this] def utf8bufs(xs: String*): Seq[Buf] = xs.map(Buf.Utf8.apply)

  test("set") {
    assertPartitionerStats()

    when(mockService.apply(any[Command])).thenReturn(Future.value(StatusReply("OK")))
    await(client.set(Buf.Utf8("foo"), Buf.Utf8("bar")))
    verify(mockService, times(1)).apply(any[Set])
  }

  test("cache miss") {
    assertPartitionerStats()
    when(mockService.apply(any[Get])).thenReturn(Future.value(EmptyBulkReply))

    assert(await(client.get(Buf.Utf8("foo"))) == None)
  }

  test("single key") {
    assertPartitionerStats()

    when(mockService.apply(any[Get])).thenReturn(Future.value(BulkReply(Buf.Utf8("bar"))))

    await(client.get(Buf.Utf8("foo"))) match {
      case Some(Buf.Utf8(str)) => assert(str == "bar")
      case None => fail("client returned None")
    }
  }

  test("does scatter/gather with multiple keys on mget") {
    assertPartitionerStats()

    val data: Map[Buf, Buf] = Map(
      Buf.Utf8("a") -> Buf.Utf8("a"),
      Buf.Utf8("b") -> Buf.Utf8("b"),
      Buf.Utf8("c") -> Buf.Utf8("c")
    )

    when(mockService.apply(any[MGet])).thenAnswer(i =>
      i.getArguments()(0).asInstanceOf[Command] match {
        case MGet(keys) =>
          Future.value(
            MBulkReply(
              keys.map { k =>
                data.get(k) match {
                  case Some(buf) => BulkReply(buf)
                  case None => EmptyBulkReply
                }
              }.toList
            )
          )

        case x => fail(s"unexpected command: $x")
      })

    val results = await(client.mGet(utf8bufs("a", "b", "q")))

    val xs =
      results.map {
        case Some(Buf.Utf8(s)) => Some(s)
        case None => None
      }

    assert(xs == Seq(Some("a"), Some("b"), None))
  }

  test("set multiple keys via scatter/gather") {
    assertPartitionerStats()

    when(mockService.apply(any[MSet])).thenReturn(
      Future.value(StatusReply("OK"))
    )

    await(
      client.mSet(
        Map(
          utf8bufs("a", "b", "c").zip(utf8bufs("x", "y", "z")): _*
        )))

    verify(mockService, times(3)).apply(any[MSet])
  }

  test("test multiple partition read and write") {
    assertPartitionerStats()

    // use a large number of keys here so that it's pretty much guaranteed that
    // multiple different backend partitions will receive the writes and reads.
    val keys = (0 to 99).map(_.toString)

    val data = keys.map { k =>
      val b = Buf.Utf8(k)
      b -> b
    }.toMap

    when(mockService.apply(any[Command])).thenAnswer(i =>
      i.getArguments()(0).asInstanceOf[Command] match {
        case MGet(ks) =>
          Future.value(
            MBulkReply(
              ks.map { k =>
                data.get(k) match {
                  case Some(buf) => BulkReply(buf)
                  case None => EmptyBulkReply
                }
              }.toList
            )
          )

        case MSet(_) => Future.value(StatusReply("OK"))

        case x => fail(s"unexpected command: $x")
      })

    await(client.mSet(data))

    val results = await(client.mGet(keys.sorted.map(Buf.Utf8.apply)))

    val xs =
      results.map {
        case Some(Buf.Utf8(s)) => Some(s)
        case None => None
      }

    assert(xs.forall(_.isDefined))
    assert(xs.flatten.length == data.size)
    assert(xs.flatten == keys.sorted)
  }

  test("test multi-partition Del command") {
    assertPartitionerStats()

    when(mockService.apply(any[Del])).thenAnswer(i =>
      i.getArguments()(0).asInstanceOf[Command] match {
        case del: Del => Future.value(IntegerReply(del.keys.length))
        case x => fail(s"expected Del argument, got $x")
      })

    val res = await(client.dels(utf8bufs("a", "b", "c")))
    assert(res == 3)
  }

  test("test multi-partition pfCount command") {
    assertPartitionerStats()

    when(mockService.apply(any[PFCount])).thenAnswer(i =>
      i.getArguments()(0).asInstanceOf[Command] match {
        case pfc: PFCount => Future.value(IntegerReply(pfc.keys.length))
        case x => fail(s"expected PFCount argument, got $x")
      })

    val res = await(client.pfCount(utf8bufs("a", "b", "c")))
    assert(res == 3)
  }

  test("test multi-partition sInter command") {
    assertPartitionerStats()

    val data: Map[Buf, Seq[Buf]] = Map(
      Buf.Utf8("a") -> utf8bufs("a", "b", "c"),
      Buf.Utf8("b") -> utf8bufs("b", "c", "d"),
      Buf.Utf8("c") -> utf8bufs("c", "d", "e")
    )

    when(mockService.apply(any[SInter])).thenAnswer { i =>
      i.getArguments()(0).asInstanceOf[Command] match {
        case sinter: SInter =>
          val sets = sinter.keys.map { k =>
            data.get(k) match {
              case Some(bufs) => bufs.toSet
              case None => SSet.empty[Buf]
            }
          }

          Future.value(
            if (sets.isEmpty) {
              EmptyMBulkReply
            } else {
              val s =
                if (sets.length == 1)
                  sets.flatten
                else
                  sets.reduce(_ intersect _)

              if (s.isEmpty)
                EmptyMBulkReply
              else
                MBulkReply(s.toList.map(BulkReply))
            }
          )

        case x => fail(s"expected SInter arg, got $x")
      }
    }

    // multiple results aggregation
    val res = await(client.sInter(utf8bufs("a", "b", "c")))
    assert(res == utf8bufs("c").toSet)

    // aggregation with empty set
    assert(await(client.sInter(utf8bufs("a", "d"))) == SSet.empty)

    // only empty set
    assert(await(client.sInter(utf8bufs("d"))) == SSet.empty)

    // single set aggregation
    assert(await(client.sInter(utf8bufs("a"))) == utf8bufs("a", "b", "c").toSet)
  }

  object MockService {
    def module: Stackable[ServiceFactory[Command, Reply]] = {
      new Stack.Module[ServiceFactory[Command, Reply]] {
        val role: Stack.Role = Stack.Role("MockService")
        val description: String = "bogus!"
        def parameters: Seq[Stack.Param[_]] = Seq.empty

        def make(
          params: Stack.Params,
          next: Stack[ServiceFactory[Command, Reply]]
        ): Stack[ServiceFactory[Command, Reply]] = {
          Stack.leaf(role, ServiceFactory.const(mockService))
        }
      }
    }
  }
}
