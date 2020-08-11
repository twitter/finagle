package com.twitter.finagle.thrift.exp.partitioning

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.partitioning.PartitioningService
import com.twitter.finagle.thrift.ClientDeserializeCtx
import com.twitter.finagle.thrift.exp.partitioning.ThriftPartitioningService.PartitioningStrategyException
import com.twitter.finagle.{ServiceFactory, Stack}
import com.twitter.io.Buf
import com.twitter.scrooge.ThriftStructIface
import com.twitter.util.Return
import org.scalatest.{FunSuite, PrivateMethodTester}

class ThriftHashingPartitioningServiceTest
    extends FunSuite
    with ThriftPartitioningTest
    with PrivateMethodTester {

  val hashingStrategy = new ClientHashingStrategy({
    case aRequest: ARequest =>
      aRequest.alist.groupBy(a => a % 2).map {
        case (key, list) =>
          key -> ARequest(list)
      }
  })
  hashingStrategy.requestMergerRegistry.add(AMethod, aRequestMerger)
  hashingStrategy.responseMergerRegistry.add(AMethod, aResponseMerger)

  val mbHashingStrategy = new MethodBuilderHashingStrategy[ARequest, Int](
    getHashingKeyAndRequest = aRequest =>
      aRequest.alist.groupBy(a => a % 2).map {
        case (key, list) =>
          key -> ARequest(list)
      },
    requestMerger = Some(aRequestMerger),
    responseMerger = Some(aResponseMerger)
  )

  def testService(strategy: HashingPartitioningStrategy) =
    new ThriftHashingPartitioningService[ARequest, Int](
      underlying = mock[Stack[ServiceFactory[ARequest, Int]]],
      thriftMarshallable = thriftMarshallable,
      params = Stack.Params.empty,
      strategy
    )

  val serviceWithClientStrategy = testService(hashingStrategy)
  val serviceWithMbStrategy = testService(mbHashingStrategy)

  test("getKeyAndRequestMap") {
    val request = ARequest(List(1, 2, 3, 4))
    val serdeCtx = new ClientDeserializeCtx[Int](request, _ => Return(Int.MinValue))
    val getKeyAndRequestMap = PrivateMethod[Map[Any, ThriftStructIface]]('getKeyAndRequestMap)
    Contexts.local.let(ClientDeserializeCtx.Key, serdeCtx) {
      assert(
        serviceWithClientStrategy.invokePrivate(getKeyAndRequestMap()) ==
          Map(0 -> ARequest(Seq(2, 4)), 1 -> ARequest(Seq(1, 3))))

      // methodbuilder
      assert(
        serviceWithMbStrategy.invokePrivate(getKeyAndRequestMap()) ==
          Map(0 -> ARequest(Seq(2, 4)), 1 -> ARequest(Seq(1, 3))))
    }
  }

  test("getKeyAndRequestMap - wrong strategy for an endpoint") {
    val falseRequest = mock[ThriftStructIface]
    val serdeCtx = new ClientDeserializeCtx[Int](falseRequest, _ => Return(Int.MinValue))
    val getKeyAndRequestMap = PrivateMethod[Map[Any, ThriftStructIface]]('getKeyAndRequestMap)
    Contexts.local.let(ClientDeserializeCtx.Key, serdeCtx) {
      assert(
        serviceWithClientStrategy.invokePrivate(getKeyAndRequestMap()) ==
          Map(None -> falseRequest))

      // methodbuilder
      val e = intercept[PartitioningStrategyException] {
        serviceWithMbStrategy.invokePrivate(getKeyAndRequestMap())
      }
      assert(
        e.getMessage.contains(
          "MethodBuilder Strategy request type doesn't match with the actual request type"))
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

      // methodbuilder
      val rep3 =
        serviceWithMbStrategy.invokePrivate(mergeResponses(request, toBeMergedHeadIsOne))
      assert(rep3 == thriftMarshallable.emptyResponse)
      val resultInCtx3 = ClientDeserializeCtx.get.deserialize(Array.emptyByteArray)
      assert(resultInCtx3 == Return(1))
    }
  }
}
