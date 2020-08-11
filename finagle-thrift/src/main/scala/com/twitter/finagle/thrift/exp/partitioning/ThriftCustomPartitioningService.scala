package com.twitter.finagle.thrift.exp.partitioning

import com.twitter.finagle.partitioning.{PartitionNodeManager, PartitioningService}
import com.twitter.finagle.thrift.ClientDeserializeCtx
import com.twitter.finagle.thrift.exp.partitioning.ThriftPartitioningService.{
  PartitioningStrategyException,
  ReqRepMarshallable
}
import com.twitter.finagle.{Service, ServiceFactory, Stack}
import com.twitter.scrooge.ThriftStructIface
import com.twitter.util.Future
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
  customStrategy: CustomPartitioningStrategy)
    extends PartitioningService[Req, Rep] {

  private[this] val nodeManager =
    new PartitionNodeManager(underlying, customStrategy.getLogicalPartition, params)

  private[this] def rpcName: String = ClientDeserializeCtx.get.rpcName.getOrElse("N/A")

  final protected def noPartitionInformationHandler(req: Req): Future[Nothing] = {
    val ex = new PartitioningStrategyException(
      s"No Partitioning Ids for the thrift method: $rpcName")
    Future.exception(ex)
  }

  // for fan-out requests
  final protected def partitionRequest(
    original: Req
  ): Future[Map[Req, Future[Service[Req, Rep]]]] = {
    val serializer = new ThriftRequestSerializer(params)
    val partitionIdAndRequest = getPartitionIdAndRequestMap(original)
    partitionIdAndRequest.flatMap { idsAndRequests =>
      if (idsAndRequests.isEmpty) {
        noPartitionInformationHandler(original)
      } else if (idsAndRequests.size == 1) {
        // optimization: won't serialize request if it is a singleton partition
        Future.value(Map(original -> partitionServiceForPartitionId(idsAndRequests.head._1)))
      } else {
        Future.value(idsAndRequests.map {
          case (id, request) =>
            val thriftClientRequest =
              serializer.serialize(rpcName, request, thriftMarshallable.isOneway(original))

            val partitionedReq =
              thriftMarshallable.framePartitionedRequest(thriftClientRequest, original)

            // we assume NodeManager updates always happen before getPartitionIdAndRequestMap
            // updates. When updating the partitioning topology, it should do proper locking
            // before returning a lookup map.
            (partitionedReq, partitionServiceForPartitionId(id))
        })
      }
    }
  }

  final protected def mergeResponses(
    originalReq: Req,
    results: PartitioningService.PartitionedResults[Req, Rep]
  ): Rep = {
    val mergerOption = customStrategy match {
      case clientCustomStrategy: ClientCustomStrategy =>
        clientCustomStrategy.responseMergerRegistry.get(rpcName)
      case mbCustomStrategy: MethodBuilderCustomStrategy[_, _] =>
        mbCustomStrategy
        //upcasting, MethodBuilderCustomStrategy[Req <: ThriftStructIface, _]
          .asInstanceOf[MethodBuilderCustomStrategy[_, Any]]
          .responseMerger
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
  private[this] def getPartitionIdAndRequestMap(req: Req): Future[Map[Int, ThriftStructIface]] = {
    val inputArg = ClientDeserializeCtx.get.request.asInstanceOf[ThriftStructIface]
    try {
      val getPartitionIdAndRequest = { ts: ThriftStructIface =>
        customStrategy match {
          case clientCustomStrategy: ClientCustomStrategy =>
            clientCustomStrategy.getPartitionIdAndRequest
              .applyOrElse(ts, ClientCustomStrategy.defaultPartitionIdAndRequest)
          case mbCustomStrategy: MethodBuilderCustomStrategy[_, _] =>
            mbCustomStrategy
            //upcasting, MethodBuilderCustomStrategy[Req <: ThriftStructIface, _]
              .asInstanceOf[MethodBuilderCustomStrategy[ThriftStructIface, _]]
              .getPartitionIdAndRequest(ts)
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

  private[this] def partitionServiceForPartitionId(partitionId: Int): Future[Service[Req, Rep]] = {
    nodeManager.getServiceByPartitionId(partitionId)
  }
}
