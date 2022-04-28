package com.twitter.finagle.thrift.exp.partitioning

import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.param.Label
import com.twitter.finagle.partitioning.PartitioningService
import com.twitter.finagle.thrift.ClientDeserializeCtx
import com.twitter.finagle.thrift.exp.partitioning.ThriftPartitioningService.PartitioningStrategyException
import com.twitter.finagle.thrift.exp.partitioning.ThriftPartitioningService.ReqRepMarshallable
import com.twitter.finagle.Address
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.Stack
import com.twitter.scrooge.ThriftStructIface
import com.twitter.util.Future
import com.twitter.util.Time
import com.twitter.finagle.loadbalancer.distributor.AddrLifecycle
import scala.collection.mutable
import scala.util.control.NonFatal

/**
 * This custom partitioning service integrates with the user supplied
 * [[CustomPartitioningStrategy]]. This provides users direct setup for their
 * partitioning topologies.
 * @see [[PartitioningService]].
 */
private[finagle] class ThriftCustomPartitioningService[Req, Rep](
  underlying: Stack[ServiceFactory[Req, Rep]],
  thriftMarshallable: ReqRepMarshallable[Req, Rep],
  params: Stack.Params,
  configuredStrategy: CustomPartitioningStrategy)
    extends PartitioningService[Req, Rep] {

  private[this] val customStrategy = configuredStrategy match {
    case strat: ClientClusterStrategy =>
      new ClientCustomStrategy[Set[Address]](
        strat.getPartitionIdAndRequestFn,
        strat.getLogicalPartitionIdFn,
        AddrLifecycle
          .varAddrToActivity(params[LoadBalancerFactory.Dest].va, params[Label].label))
    case _ => configuredStrategy
  }

  private[this] val nodeManager = customStrategy.newNodeManager(underlying, params)

  private[this] val serializer = new ThriftRequestSerializer(params)

  private[this] def rpcName: String = ClientDeserializeCtx.get.rpcName.getOrElse("N/A")

  final protected def noPartitionInformationHandler(req: Req): Future[Nothing] = {
    val ex = new PartitioningStrategyException(
      s"No Partitioning Ids for the thrift method: $rpcName")
    Future.exception(ex)
  }

  // for fan-out requests
  final protected[finagle] def partitionRequest(
    original: Req
  ): Future[Map[Req, Seq[Future[Service[Req, Rep]]]]] = {
    val snapPartitioner = nodeManager.snapshotSharder()

    val partitionIdAndRequest = getPartitionIdAndRequestMap(snapPartitioner.partitionFunction)

    partitionIdAndRequest.flatMap { idsAndRequests =>
      if (idsAndRequests.isEmpty) {
        noPartitionInformationHandler(original)
      } else if (idsAndRequests.size == 1) {
        // optimization: won't serialize request if it is a singleton partition
        Future.value(
          Map(original -> Seq(snapPartitioner.getServiceByPartitionId(idsAndRequests.head._1))))
      } else {
        val reqAndServices = mutable.Map[Req, Seq[Future[Service[Req, Rep]]]]()
        idsAndRequests.foreach {
          case (id, request) =>
            val thriftClientRequest =
              serializer.serialize(rpcName, request, thriftMarshallable.isOneway(original))
            val partitionedReq =
              thriftMarshallable.framePartitionedRequest(thriftClientRequest, original)

            if (reqAndServices.contains(partitionedReq)) {
              reqAndServices.update(
                partitionedReq,
                reqAndServices(partitionedReq) :+ snapPartitioner.getServiceByPartitionId(id))
            } else {
              reqAndServices += (partitionedReq -> Seq(snapPartitioner.getServiceByPartitionId(id)))
            }
        }
        Future.value(reqAndServices.toMap)
      }
    }
  }

  final protected def mergeResponses(
    originalReq: Req,
    results: PartitioningService.PartitionedResults[Req, Rep]
  ): Rep = {
    val mergerOption = customStrategy match {
      case clientCustomStrategy: ClientCustomStrategy[_] =>
        clientCustomStrategy.responseMergerRegistry.get(rpcName)
      case mbCustomStrategy: MethodBuilderCustomStrategy[_, _] =>
        mbCustomStrategy
        //upcasting, MethodBuilderCustomStrategy[Req <: ThriftStructIface, _]
          .asInstanceOf[MethodBuilderCustomStrategy[_, Any]]
          .responseMerger
      case clusterStrategy: ClientClusterStrategy =>
        throw new IllegalStateException(
          s"found a ClientClusterStrategy $clusterStrategy after it" +
            " should have been converted to a ClientCustomStrategy.  This state should be " +
            "impossible to reach. It indicates a serious bug.")
    }

    val responseMerger = mergerOption match {
      case Some(merger) => merger
      case None =>
        throw new IllegalArgumentException(
          s"cannot find the response merger for thrift method: $rpcName"
        )
    }

    val mergedResponse = ThriftPartitioningUtil.mergeResponses(
      results,
      responseMerger,
      thriftMarshallable.fromResponseToBytes)

    // set the merged response to the ClientDeserializeCtx field deserialized and
    // return an empty response.
    // Thrift client get the deserialized response from the field.
    ClientDeserializeCtx.get.mergedDeserializedResponse(mergedResponse)
    thriftMarshallable.emptyResponse
  }

  // note: this function should be only evaluate once per-request
  private[partitioning] def getPartitionIdAndRequestMap(
    pf: ClientCustomStrategy.ToPartitionedMap
  ): Future[Map[Int, ThriftStructIface]] = {
    val inputArg = ClientDeserializeCtx.get.request.asInstanceOf[ThriftStructIface]
    try {
      val getPartitionIdAndRequest = { ts: ThriftStructIface =>
        customStrategy match {
          case _: ClientCustomStrategy[_] =>
            pf.applyOrElse(ts, ClientCustomStrategy.defaultPartitionIdAndRequest)
          case mbCustomStrategy: MethodBuilderCustomStrategy[_, _] =>
            mbCustomStrategy
            //upcasting, MethodBuilderCustomStrategy[Req <: ThriftStructIface, _]
              .asInstanceOf[MethodBuilderCustomStrategy[ThriftStructIface, _]]
              .getPartitionIdAndRequest(ts)
          case clusterStrategy: ClientClusterStrategy =>
            throw new IllegalStateException(
              s"found a ClientClusterStrategy $clusterStrategy after it should have been " +
                "converted to a ClientCustomStrategy.  This state should be impossible" +
                " to reach. It indicates a serious bug.")
        }
      }
      // ClientCustomStrategy.defaultPartitionIdAndRequest throws a Future.exception
      // for undefined endpoints(methods) in PartitioningStrategy. It indicates
      // those requests for certain endpoint won't be served in PartitioningService.
      getPartitionIdAndRequest(inputArg)
    } catch {
      case castEx: ClassCastException =>
        // applied the wrong request type to getPartitionIdAndRequest
        Future.exception(
          new PartitioningStrategyException(
            "MethodBuilder Strategy request type doesn't match with the actual request type, " +
              "please check the MethodBuilderCustomStrategy type.",
            castEx))
      case NonFatal(e) => Future.exception(new PartitioningStrategyException(e))
    }
  }

  override def close(deadline: Time): Future[Unit] =
    Future.join(Seq(nodeManager.close(deadline), super.close(deadline)))
}
