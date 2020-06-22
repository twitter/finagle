package com.twitter.finagle.thrift.exp.partitioning

import com.twitter.finagle.partitioning.{PartitionNodeManager, PartitioningService}
import com.twitter.finagle.thrift.ClientDeserializeCtx
import com.twitter.finagle.thrift.exp.partitioning.ThriftPartitioningService.ReqRepMarshallable
import com.twitter.finagle.{FailureFlags, Service, ServiceFactory, Stack}
import com.twitter.logging.{HasLogLevel, Level}
import com.twitter.scrooge.{ThriftStruct, ThriftStructIface}
import com.twitter.util.Future
import scala.util.control.NonFatal

private object ThriftCustomPartitioningService {

  /**
   * Failed to get Partition Ids and Requests from [[CustomPartitioningStrategy]].
   */
  final class PartitioningStrategyException(
    message: String,
    cause: Throwable = null,
    val flags: Long = FailureFlags.Empty)
      extends Exception(message, cause)
      with FailureFlags[PartitioningStrategyException]
      with HasLogLevel {
    def this(cause: Throwable) = this(null, cause)
    def logLevel: Level = Level.ERROR

    protected def copyWithFlags(flags: Long): PartitioningStrategyException =
      new PartitioningStrategyException(message, cause, flags)
  }
}

/**
 * This custom partitioning service integrates with the user supplied
 * [[CustomPartitioningStrategy]]. This provides users direct setup for their
 * partitioning topologies.
 * @see [[PartitioningService]].
 */
class ThriftCustomPartitioningService[Req, Rep](
  underlying: Stack[ServiceFactory[Req, Rep]],
  thriftMarshallable: ReqRepMarshallable[Req, Rep],
  params: Stack.Params,
  customStrategy: CustomPartitioningStrategy)
    extends PartitioningService[Req, Rep] {

  import ThriftCustomPartitioningService._

  private[this] val nodeManager =
    new PartitionNodeManager(underlying, customStrategy.getLogicalPartition, params)

  // the precondition is that this request is all for a single partition
  final protected def getPartitionFor(partitionedRequest: Req): Future[Service[Req, Rep]] = {
    getPartitionIdAndRequestMap(partitionedRequest).flatMap { partitionMap =>
      if (partitionMap.isEmpty) {
        // this should not happen, isSingletonPartition guards against this
        noPartitionInformationHandler(partitionedRequest)
      } else {
        partitionServiceForPartitionId(partitionMap.head._1)
      }
    }
  }

  final protected def noPartitionInformationHandler(req: Req): Future[Nothing] = {
    val ex = new PartitioningStrategyException(
      s"No Partitioning Ids for the thrift method: ${ClientDeserializeCtx.get.rpcName
        .getOrElse("N/A")}")
    Future.exception(ex)
  }

  // for fan-out requests
  final protected def partitionRequest(
    original: Req
  ): Future[Map[Req, Future[Service[Req, Rep]]]] = {
    val serializer = new ThriftRequestSerializer(params)
    ClientDeserializeCtx.get.rpcName match {
      case Some(rpcName) =>
        getPartitionIdAndRequestMap(original).map { idsAndRequests =>
          idsAndRequests.map {
            case (id, request) =>
              val thriftClientRequest = serializer.serialize(
                rpcName,
                request.asInstanceOf[ThriftStruct],
                thriftMarshallable.isOneway(original))

              val partitionedReq =
                thriftMarshallable.framePartitionedRequest(thriftClientRequest, original)

              // we assume NodeManager updates always happen before getPartitionIdAndRequestMap
              // updates. When updating the partitioning topology, it should do proper locking
              // before returning a lookup map.
              (partitionedReq, partitionServiceForPartitionId(id))
          }
        }
      case None =>
        Future.exception(new IllegalArgumentException("cannot find the thrift method rpcName"))
    }
  }

  final protected def mergeResponses(
    originalReq: Req,
    results: PartitioningService.PartitionedResults[Req, Rep]
  ): Rep = {
    val responseMerger = customStrategy match {
      case clientCustomStrategy: ClientCustomStrategy =>
        ClientDeserializeCtx.get.rpcName.flatMap { rpcName =>
          clientCustomStrategy.responseMergerRegistry.get(rpcName)
        } match {
          case Some(merger) => merger
          case None =>
            throw new IllegalArgumentException(
              s"cannot find the response merger for thrift method: " +
                s"${ClientDeserializeCtx.get.rpcName.getOrElse("N/A")}"
            )
        }
    }

    val mergedResponse = ThriftPartitioningUtil.mergeResponses(
      originalReq,
      results,
      responseMerger,
      thriftMarshallable.fromResponseToBytes)

    // set the merged response to the ClientDeserializeCtx field deserialized and
    // return an empty response.
    // Thrift client get the deserialized response from the field.
    ClientDeserializeCtx.get.mergedDeserializedResponse(mergedResponse)
    thriftMarshallable.emptyResponse
  }

  final protected def isSinglePartition(request: Req): Future[Boolean] = {
    getPartitionIdAndRequestMap(request).flatMap { idsAndRequests =>
      val partitionIds = idsAndRequests.map(_._1).toSeq
      if (partitionIds.isEmpty) {
        noPartitionInformationHandler(request)
      } else {
        Future.value(partitionIds.size == 1)
      }
    }
  }

  private[this] def getPartitionIdAndRequestMap(req: Req): Future[Map[Int, ThriftStructIface]] = {
    val inputArg = ClientDeserializeCtx.get.request.asInstanceOf[ThriftStructIface]
    try {
      val getPartitionIdAndRequest = { ts: ThriftStructIface =>
        customStrategy match {
          case clientCustomStrategy: ClientCustomStrategy =>
            clientCustomStrategy.getPartitionIdAndRequest
              .applyOrElse(ts, ClientCustomStrategy.defaultPartitionIdAndRequest)
        }
      }
      // CustomPartitioningStrategy.defaultPartitionIdAndRequest set a Future.never
      // for undefined endpoints(methods) in PartitioningStrategy. It indicates
      // those requests for certain endpoint won't be served in PartitioningService.
      getPartitionIdAndRequest(inputArg)
    } catch {
      case NonFatal(e) => Future.exception(new PartitioningStrategyException(e))
    }
  }

  private[this] def partitionServiceForPartitionId(partitionId: Int): Future[Service[Req, Rep]] = {
    nodeManager.getServiceByPartitionId(partitionId)
  }
}
