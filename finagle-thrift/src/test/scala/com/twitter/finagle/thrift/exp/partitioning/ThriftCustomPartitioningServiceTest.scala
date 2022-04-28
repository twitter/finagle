package com.twitter.finagle.thrift.exp.partitioning

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.partitioning.PartitioningService
import com.twitter.finagle.partitioning.zk.ZkMetadata
import com.twitter.finagle.stack.nilStack
import com.twitter.finagle.thrift.ClientDeserializeCtx
import com.twitter.finagle.thrift.exp.partitioning.ThriftPartitioningService.PartitioningStrategyException
import com.twitter.finagle.Addr
import com.twitter.finagle.Address
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.Stack
import com.twitter.finagle.Stackable
import com.twitter.finagle.StackBuilder
import com.twitter.io.Buf
import com.twitter.scrooge.ThriftStructIface
import com.twitter.test.thriftscala.B
import com.twitter.util.Future
import com.twitter.util.Return
import com.twitter.util.Var
import org.scalatest.PrivateMethodTester
import org.scalatest.funsuite.AnyFunSuite

class ThriftCustomPartitioningServiceTest
    extends AnyFunSuite
    with ThriftPartitioningTest
    with PrivateMethodTester {

  val customPartitioningStrategy = ClientCustomStrategy.noResharding(
    {
      case args: B.MergeableAdd.Args =>
        val idsAndRequests = args.alist.groupBy(a => a % 3).map {
          case (id, list) => id -> B.MergeableAdd.Args(list)
        }
        Future.value(idsAndRequests)
    },
    { shardId => Seq(shardId % 3) }
  )
  customPartitioningStrategy.responseMergerRegistry.add(AMethod, aResponseMerger)

  val mbCustomStrategy = new MethodBuilderCustomStrategy[B.MergeableAdd.Args, Int](
    getPartitionIdAndRequest = {
      case args: B.MergeableAdd.Args =>
        val idsAndRequests = args.alist.groupBy(a => a % 3).map {
          case (id, list) => id -> B.MergeableAdd.Args(list)
        }
        Future.value(idsAndRequests)
    },
    responseMerger = Some(aResponseMerger)
  )

  val sf = ServiceFactory.const(Service.const(Future.value(0)))
  val factory: Stackable[ServiceFactory[ThriftStructIface, Int]] =
    new Stack.Module1[LoadBalancerFactory.Dest, ServiceFactory[ThriftStructIface, Int]] {
      val role = LoadBalancerFactory.role
      val description: String = "mock the Stack[ServiceFactory[Req, Rep] for node manager"

      def make(
        param: LoadBalancerFactory.Dest,
        next: ServiceFactory[ThriftStructIface, Int]
      ): ServiceFactory[ThriftStructIface, Int] = sf
    }

  val stack =
    new StackBuilder[ServiceFactory[ThriftStructIface, Int]](nilStack[ThriftStructIface, Int])
      .push(factory)
      .result

  def addresses(num: Int): Var[Addr] = {
    val sfAddresses: Seq[Address] = for {
      shardId <- 0 until num
    } yield Address.ServiceFactory(sf, ZkMetadata.toAddrMetadata(ZkMetadata(Some(shardId))))
    Var.value(Addr.Bound(sfAddresses.toSet))
  }

  def testService(strategy: CustomPartitioningStrategy) =
    new ThriftCustomPartitioningService[ThriftStructIface, Int](
      underlying = stack,
      thriftMarshallable = thriftMarshallable,
      params = Stack.Params.empty + LoadBalancerFactory.Dest(addresses(4)),
      strategy
    )

  val serviceWithClientStrategy = testService(customPartitioningStrategy)
  val serviceWithMbStrategy = testService(mbCustomStrategy)

  test(
    "getPartitionIdAndRequestMap uses the provided partitioning function and request to pick an appropriate service") {
    val request = B.MergeableAdd.Args(List(1, 2, 3, 4))
    val serdeCtx = new ClientDeserializeCtx[Int](request, _ => Return(Int.MinValue))
    val toPartitionedMap: PartialFunction[
      ThriftStructIface,
      Future[Map[Int, ThriftStructIface]]
    ] = {
      case args: B.MergeableAdd.Args =>
        Future.value(args.alist.groupBy(_ % 3).mapValues(B.MergeableAdd.Args(_)).toMap)
    }
    Contexts.local.let(ClientDeserializeCtx.Key, serdeCtx) {
      assert(
        await(serviceWithClientStrategy.getPartitionIdAndRequestMap(toPartitionedMap)) ==
          Map(
            0 -> B.MergeableAdd.Args(Seq(3)),
            1 -> B.MergeableAdd.Args(Seq(1, 4)),
            2 -> B.MergeableAdd.Args(Seq(2))))

      //methodbuilder
      assert(
        await(serviceWithMbStrategy.getPartitionIdAndRequestMap(toPartitionedMap)) ==
          Map(
            0 -> B.MergeableAdd.Args(Seq(3)),
            1 -> B.MergeableAdd.Args(Seq(1, 4)),
            2 -> B.MergeableAdd.Args(Seq(2)))
      )
    }
  }

  test("getPartitionIdAndRequestMap -- exception when request type is not registered") {
    val request = mock[ThriftStructIface]
    val serdeCtx = new ClientDeserializeCtx[Int](request, _ => Return(Int.MinValue))
    val toPartitionedMap: PartialFunction[
      ThriftStructIface,
      Future[Map[Int, ThriftStructIface]]
    ] = {
      case args: B.MergeableAdd.Args =>
        Future.value(args.alist.groupBy(_ % 3).mapValues(B.MergeableAdd.Args(_)).toMap)
    }
    Contexts.local.let(ClientDeserializeCtx.Key, serdeCtx) {
      val ex1 = intercept[PartitioningStrategyException] {
        await(serviceWithClientStrategy.getPartitionIdAndRequestMap(toPartitionedMap))
      }
      assert(
        ex1.getMessage.contains(
          "An unspecified endpoint has been applied to the partitioning service"))

      // methodBuilder
      val ex2 = intercept[PartitioningStrategyException] {
        await(serviceWithMbStrategy.getPartitionIdAndRequestMap(toPartitionedMap))
      }
      assert(
        ex2.getMessage.contains(
          "MethodBuilder Strategy request type doesn't match with the actual request type"))
    }
  }

  test("fan-out request - partitionRequest") {
    val fanoutRequest = B.MergeableAdd.Args(List(1, 2, 3, 4))
    val serdeCtx1 = new ClientDeserializeCtx[Int](fanoutRequest, _ => Return(Int.MinValue))
    Contexts.local.let(ClientDeserializeCtx.Key, serdeCtx1) {
      serdeCtx1.rpcName("mergeable_add")
      assert(await(serviceWithClientStrategy.partitionRequest(fanoutRequest)).size == 3)
    }
  }

  test("fan-out request - the same request send to different dest as configured") {
    val sameRequestFanout = ClientCustomStrategy.noResharding(
      {
        case args: B.MergeableAdd.Args =>
          val idsAndRequests = (0 until 4).map { id =>
            id -> args
          }.toMap
          Future.value(idsAndRequests)
      },
      { shardId => Seq(shardId) }
    )
    val fanoutRequest = B.MergeableAdd.Args(List(1, 2, 3, 4))
    val serdeCtx1 = new ClientDeserializeCtx[Int](fanoutRequest, _ => Return(Int.MinValue))
    Contexts.local.let(ClientDeserializeCtx.Key, serdeCtx1) {
      serdeCtx1.rpcName("mergeable_add")
      val partitionRequestAndServices =
        testService(sameRequestFanout).partitionRequest(fanoutRequest)
      assert(await(partitionRequestAndServices).size == 1)
      assert(await(partitionRequestAndServices)(fanoutRequest).size == 4)
    }
  }

  test("response - mergeResponses") {
    val toBeMergedHeadIsOne = PartitioningService.PartitionedResults(
      successes = List((B.MergeableAdd.Args(List(1)), 1), (B.MergeableAdd.Args(List(2)), 2)),
      failures = List((B.MergeableAdd.Args(List(3)), new Exception))
    )
    val request = B.MergeableAdd.Args(List(2, 3, 4, 1))
    val serdeCtx = new ClientDeserializeCtx[Int](
      request,
      rep => Return(Buf.U32BE.unapply(Buf.ByteArray.Owned(rep)).get._1))
    val mergeResponses = PrivateMethod[Int]('mergeResponses)

    Contexts.local.let(ClientDeserializeCtx.Key, serdeCtx) {
      serdeCtx.rpcName("mergeable_add")
      val rep1 =
        serviceWithClientStrategy.invokePrivate(mergeResponses(request, toBeMergedHeadIsOne))
      assert(rep1 == thriftMarshallable.emptyResponse)
      val resultInCtx1 = ClientDeserializeCtx.get.deserialize(Array.emptyByteArray)
      assert(resultInCtx1 == Return(3))

      val toBeMergedHeadIsTwo = PartitioningService.PartitionedResults(
        successes = List((B.MergeableAdd.Args(List(2)), 2), (B.MergeableAdd.Args(List(1)), 1)),
        failures = List((B.MergeableAdd.Args(List(3)), new Exception))
      )
      // mergeResponses should be called once during one reqRep travel
      // mimic multiple requests in one local scope per testing
      val rep2 =
        serviceWithClientStrategy.invokePrivate(mergeResponses(request, toBeMergedHeadIsTwo))
      assert(rep2 == thriftMarshallable.emptyResponse)
      val resultInCtx2 = ClientDeserializeCtx.get.deserialize(Array.emptyByteArray)
      assert(resultInCtx2 == Return(3))

      //methodBuilder
      val rep3 =
        serviceWithMbStrategy.invokePrivate(mergeResponses(request, toBeMergedHeadIsTwo))
      assert(rep3 == thriftMarshallable.emptyResponse)
      val resultInCtx3 = ClientDeserializeCtx.get.deserialize(Array.emptyByteArray)
      assert(resultInCtx3 == Return(3))
    }
  }

}
