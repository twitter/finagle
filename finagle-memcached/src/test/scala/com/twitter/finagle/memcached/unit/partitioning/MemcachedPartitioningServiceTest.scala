package com.twitter.finagle.memcached.unit.partitioning

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.memcached.TwemcacheClient
import com.twitter.finagle.memcached.partitioning.MemcachedPartitioningService
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.naming.BindingFactory
import com.twitter.finagle.stack.nilStack
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.io.Buf
import com.twitter.util.{Await, Awaitable, Duration, Future}
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FunSuite, MustMatchers}

@RunWith(classOf[JUnitRunner])
class MemcachedPartitioningServiceTest
    extends FunSuite
    with MockitoSugar
    with MustMatchers
    with BeforeAndAfterEach {

  private[this] val clientName = "unit_test"
  private[this] val Timeout: Duration = 15.seconds
  private[this] val numServers = 5

  private[this] var mockService: Service[Command, Response] = _
  private[this] var client: TwemcacheClient = _
  private[this] var statsReceiver: InMemoryStatsReceiver = _

  private[this] def createClient(dest: Name) = {
    val stack = {
      val builder = new StackBuilder[ServiceFactory[Command, Response]](nilStack[Command, Response])
      builder.push(MockService.module)
      builder.push(MemcachedPartitioningService.module)
      builder.push(BindingFactory.module)
      builder.result
    }
    val params = Stack.Params.empty +
      param.Stats(statsReceiver.scope(clientName)) +
      BindingFactory.Dest(dest)
    TwemcacheClient(Await.result(stack.make(params)()))
  }

  private[this] def awaitResult[T](awaitable: Awaitable[T]): T = Await.result(awaitable, Timeout)

  override def beforeEach(): Unit = {
    mockService = mock[Service[Command, Response]]
    statsReceiver = new InMemoryStatsReceiver
    client = createClient(
      dest = Name.bound((1 to numServers).map(Address("localhost", _)): _*)
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
    val kvMap = (1 to numKeys).map { i =>
      s"key-$i" -> s"value-$i"
    }.toMap

    val values = Values(
      (1 to numKeys).map { i =>
        Value(Buf.Utf8(s"key-$i"), Buf.Utf8(s"value-$i"))
      }
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
          Stack.Leaf(role, ServiceFactory.const(mockService))
        }
      }
    }
  }
}
