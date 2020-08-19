package com.twitter.finagle.thriftmux.exp.partitioning

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.mux.{Request, Response}
import com.twitter.finagle.thrift.exp.partitioning.ThriftPartitioningService.PartitioningStrategyException
import com.twitter.finagle.thrift.exp.partitioning.{
  CustomPartitioningStrategy,
  HashingPartitioningStrategy,
  ThriftCustomPartitioningService,
  ThriftHashingPartitioningService
}
import com.twitter.finagle.{Service, ServiceFactory, Stack}
import com.twitter.io.Buf
import com.twitter.util.{Await, Awaitable, Duration, Future}
import org.scalatest.FunSuite
import org.scalatestplus.mockito.MockitoSugar

class DynamicPartitioningServiceTest extends FunSuite with MockitoSugar {

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
