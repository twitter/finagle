package com.twitter.finagle.thrift.exp.partitioning

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.partitioning.{PartitionNodeManager, PartitioningService}
import com.twitter.finagle.thrift.ClientDeserializeCtx
import com.twitter.finagle.{Service, ServiceFactory, Stack}
import com.twitter.finagle.thrift.exp.partitioning.ThriftPartitioningService.PartitioningStrategyException
import com.twitter.io.Buf
import com.twitter.scrooge.ThriftStructIface
import com.twitter.util.{Future, Return}
import org.mockito.Mockito.when
import org.mockito.internal.util.reflection.FieldSetter
import org.scalatest.{FunSuite, PrivateMethodTester}

class ThriftCustomPartitioningServiceTest
    extends FunSuite
    with ThriftPartitioningTest
    with PrivateMethodTester {

  val customPartitioningStrategy = new ClientCustomStrategy({
    case aRequest: ARequest =>
      val idsAndRequests = aRequest.alist.groupBy(a => a % 3).map {
        case (id, list) => id -> ARequest(list)
      }
      Future.value(idsAndRequests)
  })
  customPartitioningStrategy.responseMergerRegistry.add(AMethod, aResponseMerger)

  val mbCustomStrategy = new MethodBuilderCustomStrategy[ARequest, Int](
    getPartitionIdAndRequest = {
      case aRequest: ARequest =>
        val idsAndRequests = aRequest.alist.groupBy(a => a % 3).map {
          case (id, list) => id -> ARequest(list)
        }
        Future.value(idsAndRequests)
    },
    responseMerger = Some(aResponseMerger)
  )

  def testService(strategy: CustomPartitioningStrategy) =
    new ThriftCustomPartitioningService[ARequest, Int](
      underlying = mock[Stack[ServiceFactory[ARequest, Int]]],
      thriftMarshallable = thriftMarshallable,
      params = Stack.Params.empty,
      strategy
    )

  val serviceWithClientStrategy = testService(customPartitioningStrategy)
  val serviceWithMbStrategy = testService(mbCustomStrategy)

  test("getPartitionIdAndRequestMap") {
    val request = ARequest(List(1, 2, 3, 4))
    val serdeCtx = new ClientDeserializeCtx[Int](request, _ => Return(Int.MinValue))
    val getPartitionIdAndRequestMap =
      PrivateMethod[Future[Map[Int, ThriftStructIface]]]('getPartitionIdAndRequestMap)
    Contexts.local.let(ClientDeserializeCtx.Key, serdeCtx) {
      assert(
        await(serviceWithClientStrategy.invokePrivate(getPartitionIdAndRequestMap(request))) ==
          Map(0 -> ARequest(Seq(3)), 1 -> ARequest(Seq(1, 4)), 2 -> ARequest(Seq(2))))

      //methodbuilder
      assert(
        await(serviceWithMbStrategy.invokePrivate(getPartitionIdAndRequestMap(request))) ==
          Map(0 -> ARequest(Seq(3)), 1 -> ARequest(Seq(1, 4)), 2 -> ARequest(Seq(2))))
    }
  }

  test("getPartitionIdAndRequestMap -- exception when request type is not registered") {
    val request = mock[ThriftStructIface]
    val serdeCtx = new ClientDeserializeCtx[Int](request, _ => Return(Int.MinValue))
    val getPartitionIdAndRequestMap =
      PrivateMethod[Future[Map[Int, ThriftStructIface]]]('getPartitionIdAndRequestMap)
    Contexts.local.let(ClientDeserializeCtx.Key, serdeCtx) {
      val ex1 = intercept[PartitioningStrategyException] {
        await(serviceWithClientStrategy.invokePrivate(getPartitionIdAndRequestMap(request)))
      }
      assert(
        ex1.getMessage.contains(
          "An unspecified endpoint has been applied to the partitioning service"))

      // methodBuilder
      val ex2 = intercept[PartitioningStrategyException] {
        await(serviceWithMbStrategy.invokePrivate(getPartitionIdAndRequestMap(request)))
      }
      assert(
        ex2.getMessage.contains(
          "MethodBuilder Strategy request type doesn't match with the actual request type"))
    }
  }

  test("fan-out request - partitionRequest") {
    val partitionRequest =
      PrivateMethod[Future[Map[ARequest, Future[Service[ARequest, Int]]]]]('partitionRequest)
    val fakeNodeManager = mock[PartitionNodeManager[ARequest, Int]]
    when(fakeNodeManager.getServiceByPartitionId(0)).thenReturn(Future.value {
      Service.mk { _: ARequest => Future.value(0) }
    })
    when(fakeNodeManager.getServiceByPartitionId(1)).thenReturn(Future.value {
      Service.mk { _: ARequest => Future.value(1) }
    })
    when(fakeNodeManager.getServiceByPartitionId(2)).thenReturn(Future.value {
      Service.mk { _: ARequest => Future.value(2) }
    })
    new FieldSetter(
      serviceWithClientStrategy,
      serviceWithClientStrategy.getClass.getDeclaredField("nodeManager"))
      .set(fakeNodeManager)
    val fanoutRequest = ARequest(List(1, 2, 3, 4))
    val serdeCtx1 = new ClientDeserializeCtx[Int](fanoutRequest, _ => Return(Int.MinValue))
    Contexts.local.let(ClientDeserializeCtx.Key, serdeCtx1) {
      serdeCtx1.rpcName("A")
      assert(
        (await(serviceWithClientStrategy.invokePrivate(partitionRequest(fanoutRequest)))).size == 3)
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
      val rep1 =
        serviceWithClientStrategy.invokePrivate(mergeResponses(request, toBeMergedHeadIsOne))
      assert(rep1 == thriftMarshallable.emptyResponse)
      val resultInCtx1 = ClientDeserializeCtx.get.deserialize(Array.emptyByteArray)
      assert(resultInCtx1 == Return(1))

      val toBeMergedHeadIsTwo = PartitioningService.PartitionedResults(
        successes = List((ARequest(List(2)), 2), (ARequest(List(1)), 1)),
        failures = List((ARequest(List(3)), new Exception))
      )
      // mergeResponses should be called once during one reqRep travel
      // mimic multiple requests in one local scope per testing
      val rep2 =
        serviceWithClientStrategy.invokePrivate(mergeResponses(request, toBeMergedHeadIsTwo))
      assert(rep2 == thriftMarshallable.emptyResponse)
      val resultInCtx2 = ClientDeserializeCtx.get.deserialize(Array.emptyByteArray)
      assert(resultInCtx2 == Return(2))

      //methodBuilder
      val rep3 =
        serviceWithMbStrategy.invokePrivate(mergeResponses(request, toBeMergedHeadIsTwo))
      assert(rep3 == thriftMarshallable.emptyResponse)
      val resultInCtx3 = ClientDeserializeCtx.get.deserialize(Array.emptyByteArray)
      assert(resultInCtx3 == Return(2))
    }
  }

}
