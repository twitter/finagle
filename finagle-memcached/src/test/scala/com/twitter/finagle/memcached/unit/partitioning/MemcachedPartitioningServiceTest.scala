package com.twitter.finagle.memcached.unit.partitioning

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.addr.WeightedAddress
import com.twitter.finagle.memcached.TwemcacheClient
import com.twitter.finagle.memcached.partitioning.MemcachedPartitioningService
import com.twitter.finagle.memcached.partitioning.MemcachedPartitioningService.UnsupportedCommand
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.naming.BindingFactory
import com.twitter.finagle.partitioning.zk.ZkMetadata
import com.twitter.finagle.stack.nilStack
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.io.Buf
import com.twitter.util.{Command => _, _}
import java.net.InetAddress
import java.net.InetSocketAddress
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

class MemcachedPartitioningServiceTest
    extends AnyFunSuite
    with MockitoSugar
    with Matchers
    with BeforeAndAfterEach {

  private[this] val clientName = "unit_test"
  private[this] val Timeout: Duration = 15.seconds
  private[this] val numServers = 5

  private[this] var mockService: Service[Command, Response] = _
  private[this] var client: TwemcacheClient = _
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
    val stack = {
      val builder = new StackBuilder[ServiceFactory[Command, Response]](nilStack[Command, Response])
      builder.push(MockService.module)
      builder.push(MemcachedPartitioningService.module)
      builder.push(BindingFactory.module)
      builder.result
    }
    val params = Stack.Params.empty +
      param.Stats(sr.scope(clientName)) +
      BindingFactory.Dest(dest)
    TwemcacheClient(awaitResult(stack.make(params)()))
  }

  private[this] def awaitResult[T](awaitable: Awaitable[T]): T = Await.result(awaitable, Timeout)

  override def beforeEach(): Unit = {
    mockService = mock[Service[Command, Response]]
    statsReceiver = new InMemoryStatsReceiver
    client = createClient(
      dest = Name.bound((1 to numServers).map(i => newAddress(i, i)): _*),
      statsReceiver
    )
  }

  override def afterEach(): Unit = {
    client.close()
  }

  test("cache miss") {
    assert(statsReceiver.counters(Seq(clientName, "partitioner", "redistributes")) == 1)

    when(mockService.apply(any[Get])).thenReturn(
      Future.value(Values(Seq.empty))
    )
    awaitResult(client.get("foo")) must be(None)
  }

  test("single key") {
    assert(statsReceiver.counters(Seq(clientName, "partitioner", "redistributes")) == 1)

    when(mockService.apply(any[Get])).thenReturn(
      Future.value(
        Values(
          Seq(
            Value(Buf.Utf8("foo"), Buf.Utf8("bar"))
          )
        )
      )
    )
    val result = awaitResult(client.get(Seq("foo"))).map {
      case (k, Buf.Utf8(v)) =>
        (k, v)
    }
    result must be(Map("foo" -> "bar"))
  }

  test("multiple keys") {
    assert(statsReceiver.counters(Seq(clientName, "partitioner", "redistributes")) == 1)

    when(mockService.apply(any[Get])).thenReturn(
      Future.value(
        Values(
          Seq(
            Value(Buf.Utf8("foo"), Buf.Utf8("bar")),
            Value(Buf.Utf8("baz"), Buf.Utf8("boing"))
          )
        )
      )
    )
    val result = awaitResult(client.get(Seq("foo", "baz", "notthere"))).map {
      case (k, Buf.Utf8(v)) =>
        (k, v)
    }
    result must be(Map("foo" -> "bar", "baz" -> "boing"))
  }

  test("large batch") {
    assert(statsReceiver.counters(Seq(clientName, "partitioner", "redistributes")) == 1)

    val numKeys = 500
    val kvMap = (1 to numKeys).map { i => s"key-$i" -> s"value-$i" }.toMap

    val values = Values(
      (1 to numKeys).map { i => Value(Buf.Utf8(s"key-$i"), Buf.Utf8(s"value-$i")) }
    )

    when(mockService.apply(any[Get])).thenReturn(Future.value(values))

    val result = awaitResult(client.get(kvMap.keySet.toSeq)).map {
      case (k, Buf.Utf8(v)) =>
        (k, v)
    }
    result must be(kvMap)
  }

  test("storage command") {
    assert(statsReceiver.counters(Seq(clientName, "partitioner", "redistributes")) == 1)

    when(mockService.apply(any[Command])).thenReturn(Future.value(Stored))
    awaitResult(client.set("foo", Buf.Utf8("bar")))
    verify(mockService, times(1)).apply(any[Set])
  }

  test("arithmetic command") {
    assert(statsReceiver.counters(Seq(clientName, "partitioner", "redistributes")) == 1)

    when(mockService.apply(any[Command])).thenReturn(Future.value(Number(100)))
    awaitResult(client.incr("foo", 10L)) must be(Some(100))
    verify(mockService, times(1)).apply(any[Incr])
  }

  test("delete command") {
    assert(statsReceiver.counters(Seq(clientName, "partitioner", "redistributes")) == 1)

    when(mockService.apply(any[Command])).thenReturn(Future.value(Deleted))
    awaitResult(client.delete("foo")) must be(java.lang.Boolean.TRUE)
    verify(mockService, times(1)).apply(any[Delete])
  }

  test("unsupported command") {
    intercept[UnsupportedCommand] {
      awaitResult(client.stats())
    }
  }

  test("adds and removes") {
    val sr = new InMemoryStatsReceiver
    val va: ReadWriteVar[Addr] = new ReadWriteVar(Addr.Bound())
    val client = createClient(dest = Name.Bound.singleton(va), sr)

    // start with 3 nodes
    val addr11 = newAddress(1, 1)
    val addr22 = newAddress(2, 2)
    val addr33 = newAddress(3, 3)

    va() = Addr.Bound(addr11, addr22, addr33)

    assert(sr.counters(Seq(clientName, "partitioner", "redistributes")) == 1)
    assert(sr.counters(Seq(clientName, "partitioner", "joins")) == 3)

    // bring down addr33 and add addr44
    val addr44 = newAddress(4, 4)
    va() = Addr.Bound(addr11, addr22, addr44)
    assert(sr.counters(Seq(clientName, "partitioner", "redistributes")) == 2)
    assert(sr.counters(Seq(clientName, "partitioner", "joins")) == 4)
    assert(sr.counters(Seq(clientName, "partitioner", "leaves")) == 1)

    // remove addr11
    va() = Addr.Bound(addr22, addr44)
    assert(sr.counters(Seq(clientName, "partitioner", "redistributes")) == 3)
    assert(sr.counters(Seq(clientName, "partitioner", "joins")) == 4)
    assert(sr.counters(Seq(clientName, "partitioner", "leaves")) == 2)

    // restart shard#2 (replace addr22 with addr52)
    val addr52 = newAddress(5, 2)
    va() = Addr.Bound(addr52, addr44)
    assert(sr.counters(Seq(clientName, "partitioner", "redistributes")) == 4)
    assert(sr.counters(Seq(clientName, "partitioner", "joins")) == 5)
    assert(sr.counters(Seq(clientName, "partitioner", "leaves")) == 3)

    // only weight changed. That should force node recreation (join++ and leaves++)
    val addr52w2 = newAddress(5, 2, 2)
    va() = Addr.Bound(addr52w2, addr44)
    assert(sr.counters(Seq(clientName, "partitioner", "redistributes")) == 5)
    assert(sr.counters(Seq(clientName, "partitioner", "joins")) == 6)
    assert(sr.counters(Seq(clientName, "partitioner", "leaves")) == 4)
  }

  test("client fails requests if initial serverset is empty") {
    val sr = new InMemoryStatsReceiver
    val va: ReadWriteVar[Addr] = new ReadWriteVar(Addr.Bound())
    val client = createClient(dest = Name.Bound.singleton(va), sr)

    intercept[ShardNotAvailableException] {
      awaitResult(client.get("foo"))
    }

    when(mockService.apply(any[Get])).thenReturn(
      Future.value(
        Values(
          Seq(
            Value(Buf.Utf8("foo"), Buf.Utf8("bar"))
          )
        )
      )
    )
    val addr11 = newAddress(1, 1)
    val addr22 = newAddress(2, 2)
    val addr33 = newAddress(3, 3)
    va() = Addr.Bound(addr11, addr22, addr33)

    val result = awaitResult(client.get(Seq("foo"))).map {
      case (k, Buf.Utf8(v)) =>
        (k, v)
    }
    result must be(Map("foo" -> "bar"))
  }

  object MockService {

    def module: Stackable[ServiceFactory[Command, Response]] = {
      new Stack.Module[ServiceFactory[Command, Response]] {

        val role = Stack.Role("MockService")
        val description = "fake news!"

        override def parameters: Seq[Stack.Param[_]] = Seq.empty

        def make(
          params: Stack.Params,
          next: Stack[ServiceFactory[Command, Response]]
        ): Stack[ServiceFactory[Command, Response]] = {
          Stack.leaf(role, ServiceFactory.const(mockService))
        }
      }
    }
  }
}
