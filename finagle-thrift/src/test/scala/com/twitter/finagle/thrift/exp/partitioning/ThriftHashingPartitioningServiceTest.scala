package com.twitter.finagle.thrift.exp.partitioning

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.partitioning.PartitioningService
import com.twitter.finagle.thrift.ClientDeserializeCtx
import com.twitter.finagle.thrift.exp.partitioning.PartitioningStrategy.{
  RequestMergerRegistry,
  ResponseMergerRegistry
}
import com.twitter.finagle.{ServiceFactory, Stack}
import com.twitter.io.Buf
import com.twitter.scrooge.ThriftStructIface
import com.twitter.util.Return
import org.scalatest.{FunSuite, PrivateMethodTester}

class ThriftHashingPartitioningServiceTest
    extends FunSuite
    with ThriftPartitioningTest
    with PrivateMethodTester {

  val hashingStrategy = new ClientHashingStrategy {
    def getHashingKeyAndRequest: ToPartitionedMap = {
      case aRequest: ARequest =>
        aRequest.alist.groupBy(a => a % 2).map {
          case (key, list) =>
            key -> ARequest(list)
        }
    }
    override val requestMergerRegistry: RequestMergerRegistry =
      RequestMergerRegistry.create.add(AMethod, aRequestMerger)

    override val responseMergerRegistry: ResponseMergerRegistry =
      ResponseMergerRegistry.create.add(AMethod, aResponseMerger)
  }

  val testService = new ThriftHashingPartitioningService[ARequest, Int](
    underlying = mock[Stack[ServiceFactory[ARequest, Int]]],
    thriftMarshallable = thriftMarshallable,
    params = Stack.Params.empty,
    hashingStrategy
  )

  test("getKeyAndRequestMap") {
    val request = ARequest(List(1, 2, 3, 4))
    val serdeCtx = new ClientDeserializeCtx[Int](request, _ => Return(Int.MinValue))
    val getKeyAndRequestMap = PrivateMethod[Map[Any, ThriftStructIface]]('getKeyAndRequestMap)
    Contexts.local.let(ClientDeserializeCtx.Key, serdeCtx) {
      assert(
        testService.invokePrivate(getKeyAndRequestMap()) ==
          Map(0 -> ARequest(Seq(2, 4)), 1 -> ARequest(Seq(1, 3))))
    }
  }

  test("request - createPartitionRequestForKeys") {
    val request = ARequest(List(1, 2, 3, 4))
    val serdeCtx = new ClientDeserializeCtx[Int](request, _ => Return(Int.MinValue))
    val createPartitionRequestForKeys = PrivateMethod[ARequest]('createPartitionRequestForKeys)
    Contexts.local.let(ClientDeserializeCtx.Key, serdeCtx) {
      serdeCtx.rpcName("A")
      val mergedResult =
        testService.invokePrivate(createPartitionRequestForKeys(request, Seq(0, 1)))
      assert(deserialize(mergedResult.serialized.get).toSet == Set(1, 2, 3, 4))

      val key0Result = testService.invokePrivate(createPartitionRequestForKeys(request, Seq(0)))
      assert(deserialize(key0Result.serialized.get).toSet == Set(2, 4))

      val key1Result = testService.invokePrivate(createPartitionRequestForKeys(request, Seq(1)))
      assert(deserialize(key1Result.serialized.get).toSet == Set(1, 3))
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
