package com.twitter.finagle.thrift.exp.partitioning

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.partitioning.{PartitionNodeManager, PartitioningService}
import com.twitter.finagle.thrift.ClientDeserializeCtx
import com.twitter.finagle.{Service, ServiceFactory, Stack}
import com.twitter.finagle.thrift.exp.partitioning.PartitioningStrategy.ResponseMergerRegistry
import com.twitter.finagle.thrift.exp.partitioning.ThriftCustomPartitioningService.PartitioningStrategyException
import com.twitter.io.Buf
import com.twitter.scrooge.ThriftStructIface
import com.twitter.util.{Await, Awaitable, Duration, Future, Return}
import org.scalatest.{FunSuite, PrivateMethodTester}
import org.mockito.Mockito.when
import org.mockito.internal.util.reflection.FieldSetter

class ThriftCustomPartitioningServiceTest
    extends FunSuite
    with ThriftPartitioningTest
    with PrivateMethodTester {
  def await[T](a: Awaitable[T], d: Duration = 5.seconds): T =
    Await.result(a, d)

  val customPartitioningStrategy = new ClientCustomStrategy {
    def getPartitionIdAndRequest: ToPartitionedMap = {
      case aRequest: ARequest =>
        val idsAndRequests = aRequest.alist.groupBy(a => a % 3).map {
          case (id, list) => id -> ARequest(list)
        }
        Future.value(idsAndRequests)
    }

    override val responseMergerRegistry: PartitioningStrategy.ResponseMergerRegistry =
      ResponseMergerRegistry.create.add(AMethod, aResponseMerger)

  }

  val testService = new ThriftCustomPartitioningService[ARequest, Int](
    underlying = mock[Stack[ServiceFactory[ARequest, Int]]],
    thriftMarshallable = thriftMarshallable,
    params = Stack.Params.empty,
    customPartitioningStrategy
  )

  test("getPartitionIdAndRequestMap") {
    val request = ARequest(List(1, 2, 3, 4))
    val serdeCtx = new ClientDeserializeCtx[Int](request, _ => Return(Int.MinValue))
    val getPartitionIdAndRequestMap =
      PrivateMethod[Future[Map[Int, ThriftStructIface]]]('getPartitionIdAndRequestMap)
    Contexts.local.let(ClientDeserializeCtx.Key, serdeCtx) {
      assert(
        await(testService.invokePrivate(getPartitionIdAndRequestMap(request))) ==
          Map(0 -> ARequest(Seq(3)), 1 -> ARequest(Seq(1, 4)), 2 -> ARequest(Seq(2))))
    }
  }

  test("getPartitionIdAndRequestMap -- exception when request type is not registered") {
    val request = mock[ThriftStructIface]
    val serdeCtx = new ClientDeserializeCtx[Int](request, _ => Return(Int.MinValue))
    val getPartitionIdAndRequestMap =
      PrivateMethod[Future[Map[Int, ThriftStructIface]]]('getPartitionIdAndRequestMap)
    Contexts.local.let(ClientDeserializeCtx.Key, serdeCtx) {
      intercept[PartitioningStrategyException] {
        await(testService.invokePrivate(getPartitionIdAndRequestMap(request)))
      }
    }
  }

  test("isSinglePartition") {
    val isSinglePartition = PrivateMethod[Future[Boolean]]('isSinglePartition)
    val fanoutRequest = ARequest(List(1, 2, 3, 4))
    val serdeCtx1 = new ClientDeserializeCtx[Int](fanoutRequest, _ => Return(Int.MinValue))
    Contexts.local.let(ClientDeserializeCtx.Key, serdeCtx1) {
      assert(!await(testService.invokePrivate(isSinglePartition(fanoutRequest))))
    }

    val singletonRequest = ARequest(List(3, 6, 9, 12))
    val serdeCtx2 = new ClientDeserializeCtx[Int](singletonRequest, _ => Return(Int.MinValue))
    Contexts.local.let(ClientDeserializeCtx.Key, serdeCtx2) {
      assert(await(testService.invokePrivate(isSinglePartition(singletonRequest))))
    }
  }

  test("singleton partition request - getPartitionFor") {
    val getPartitionFor = PrivateMethod[Future[Service[ARequest, Int]]]('getPartitionFor)
    val fakeNodeManager = mock[PartitionNodeManager[ARequest, Int]]
    when(fakeNodeManager.getServiceByPartitionId(0)).thenReturn(Future.value {
      Service.mk { _: ARequest => Future.value(0) }
    })
    new FieldSetter(testService, testService.getClass.getDeclaredField("nodeManager"))
      .set(fakeNodeManager)
    val singletonRequest = ARequest(List(3, 6, 9, 12))
    val serdeCtx2 = new ClientDeserializeCtx[Int](singletonRequest, _ => Return(Int.MinValue))
    Contexts.local.let(ClientDeserializeCtx.Key, serdeCtx2) {
      val service = await(testService.invokePrivate(getPartitionFor(singletonRequest)))
      assert(await(service(singletonRequest)) == 0)
    }
  }

  test("fan-out request - partitionRequest") {
    val partitionRequest =
      PrivateMethod[Future[Map[ARequest, Future[Service[ARequest, Int]]]]]('partitionRequest)
    val fakeNodeManager = mock[PartitionNodeManager[ARequest, Int]]
    when(fakeNodeManager.getServiceByPartitionId(0)).thenReturn(Future.value {
      Service.mk { _: ARequest => Future.value(0) }
    })
    new FieldSetter(testService, testService.getClass.getDeclaredField("nodeManager"))
      .set(fakeNodeManager)
    val fanoutRequest = ARequest(List(1, 2, 3, 4))
    val serdeCtx1 = new ClientDeserializeCtx[Int](fanoutRequest, _ => Return(Int.MinValue))
    Contexts.local.let(ClientDeserializeCtx.Key, serdeCtx1) {
      serdeCtx1.rpcName("A")
      assert(await(testService.invokePrivate(partitionRequest(fanoutRequest))).size == 3)
    }
  }

  test("response - mergeResponses") {
    val toBeMergedHeadIsOne = PartitioningService.PartitionedResults(
      successes = List((ARequest(List(1)), 1), (ARequest(List(2)), 2)),
      failures = List((ARequest(List(3)), new Exception))
    )
    val request = ARequest(List(1, 2, 3, 4))
    val serdeCtx = new ClientDeserializeCtx[Int](
      request,
      rep => Return(Buf.U32BE.unapply(Buf.ByteArray.Owned(rep)).get._1))
    val mergeResponses = PrivateMethod[Int]('mergeResponses)

    Contexts.local.let(ClientDeserializeCtx.Key, serdeCtx) {
      serdeCtx.rpcName("A")
      val rep1 = testService.invokePrivate(mergeResponses(request, toBeMergedHeadIsOne))
      assert(rep1 == thriftMarshallable.emptyResponse)
      val resultInCtx1 = ClientDeserializeCtx.get.deserialize(Array.emptyByteArray)
      assert(resultInCtx1 == Return(1))

      val toBeMergedHeadIsTwo = PartitioningService.PartitionedResults(
        successes = List((ARequest(List(2)), 2), (ARequest(List(1)), 1)),
        failures = List((ARequest(List(3)), new Exception))
      )
      val rep2 = testService.invokePrivate(mergeResponses(request, toBeMergedHeadIsTwo))
      assert(rep2 == thriftMarshallable.emptyResponse)
      val resultInCtx2 = ClientDeserializeCtx.get.deserialize(Array.emptyByteArray)
      assert(resultInCtx2 == Return(2))
    }
  }

}
