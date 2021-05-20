package com.twitter.finagle.thriftmux.exp.partitioning

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.mux.{Request, Response}
import com.twitter.finagle.partitioning.{PartitionNodeManager, SnapPartitioner}
import com.twitter.finagle.thrift.exp.partitioning.ThriftPartitioningService.PartitioningStrategyException
import com.twitter.finagle.thrift.exp.partitioning.{
  ClientCustomStrategy,
  CustomPartitioningStrategy,
  HashingPartitioningStrategy,
  ThriftCustomPartitioningService,
  ThriftHashingPartitioningService
}
import com.twitter.finagle.{Service, ServiceFactory, Stack}
import com.twitter.io.Buf
import com.twitter.util.{Await, Awaitable, Duration, Future}
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class DynamicPartitioningServiceTest extends AnyFunSuite with MockitoSugar {

  def await[T](a: Awaitable[T], d: Duration = 5.seconds): T =
    Await.result(a, d)

  val svcFactory = ServiceFactory.const(Service.mk { _: Request =>
    Future.value(Response(Buf.Utf8("hi")))
  })

  val testService = new DynamicPartitioningService(
    params = Stack.Params.empty,
    next = Stack.leaf(Stack.Role("test"), svcFactory))

  test("partitioning service is applied per-request") {
    assert(testService.getPool.isEmpty)
    //disabled
    val a = testService(Request.empty)
    assert(testService.getPool.size() == 0)
    assert(await(a).body == Buf.Utf8("hi"))

    val customStrategy = mock[CustomPartitioningStrategy]
    val nodeManager =
      mock[PartitionNodeManager[Request, Response, _, ClientCustomStrategy.ToPartitionedMap]]
    when[PartitionNodeManager[Request, Response, _, ClientCustomStrategy.ToPartitionedMap]](
      customStrategy.newNodeManager[Request, Response](any(), any())
    ).thenReturn(nodeManager)
    when(nodeManager.snapshotSharder()).thenReturn(
      SnapPartitioner[Request, Response, ClientCustomStrategy.ToPartitionedMap](
        PartialFunction.empty,
        Map.empty
      )
    )
    DynamicPartitioningService.letStrategy(customStrategy) {

      intercept[PartitioningStrategyException](await(testService(Request.empty)))
      assert(testService.getPool.size() == 1)
      assert(
        testService.getPool.get(customStrategy).isInstanceOf[ThriftCustomPartitioningService[_, _]])
    }

    val hashingStrategy = mock[HashingPartitioningStrategy]
    DynamicPartitioningService.letStrategy(hashingStrategy) {
      intercept[PartitioningStrategyException](testService(Request.empty))
      assert(testService.getPool.size() == 2)
      assert(
        testService.getPool
          .get(hashingStrategy).isInstanceOf[ThriftHashingPartitioningService[_, _]])
    }
  }
}
