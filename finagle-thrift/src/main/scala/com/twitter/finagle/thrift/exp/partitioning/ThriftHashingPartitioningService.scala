package com.twitter.finagle.thrift.exp.partitioning

import com.twitter.finagle.partitioning.ConsistentHashPartitioningService.{
  HashingStrategyException,
  NoPartitioningKeys
}
import com.twitter.finagle.partitioning.param.NumReps
import com.twitter.finagle.partitioning.{ConsistentHashPartitioningService, PartitioningService}
import com.twitter.finagle.thrift.ClientDeserializeCtx
import com.twitter.finagle.thrift.exp.partitioning.PartitioningStrategy.RequestMerger
import com.twitter.finagle.thrift.exp.partitioning.ThriftPartitioningService.{
  PartitioningStrategyException,
  ReqRepMarshallable
}
import com.twitter.finagle.{Service, ServiceFactory, Stack}
import com.twitter.hashing.KeyHasher
import com.twitter.io.Buf
import com.twitter.scrooge.ThriftStructIface
import com.twitter.util.Future
import scala.util.control.NonFatal

/**
 * A [[ConsistentHashPartitioningService]] for Thrift messages.
 * @see [[PartitioningService]].
 */
final private[partitioning] class ThriftHashingPartitioningService[Req, Rep](
  underlying: Stack[ServiceFactory[Req, Rep]],
  thriftMarshallable: ReqRepMarshallable[Req, Rep],
  params: Stack.Params,
  hashingStrategy: HashingPartitioningStrategy,
  keyHasher: KeyHasher = KeyHasher.MURMUR3,
  numReps: Int = NumReps.Default)
    extends ConsistentHashPartitioningService[Req, Rep, Any](
      underlying,
      params,
      keyHasher,
      numReps
    ) {

  private[this] val requestSerializer = new ThriftRequestSerializer(params)

  final protected def getKeyBytes(key: Any): Array[Byte] =
    Buf.ByteArray.Owned.extract(Buf.U32BE(key.hashCode()))

  final protected def noPartitionInformationHandler(req: Req): Future[Nothing] = {
    val ex = new NoPartitioningKeys(s"No Partitioning hashing keys for the thrift method: $rpcName")
    Future.exception(ex)
  }

  // unused
  final protected def getPartitionKeys(request: Req): Seq[Any] = Seq.empty
  // unused
  final protected def createPartitionRequestForKeys(original: Req, keys: Seq[Any]): Req = original

  // override the super function: Keep passing ThriftStructIface around instead of Req
  // until it gets the final request, so that it only serializes request once
  override protected def partitionRequest(
    request: Req
  ): Future[Map[Req, Future[Service[Req, Rep]]]] = {
    val keyAndRequest = getKeyAndRequestMap
    if (keyAndRequest.isEmpty || keyAndRequest.head._1 == None) {
      noPartitionInformationHandler(request)
    } else {
      val requestAndService = keyAndRequest
        .groupBy {
          case (key, _) => partitionServiceForKey(key)
        }.map {
          case (svc, kqMap) if kqMap.size == 1 =>
            (request, svc)
          case (svc, kqMap) =>
            (framePartitionedRequest(mergeRequest(rpcName)(kqMap.values.toSeq), request), svc)
        }
      Future.value(requestAndService)
    }
  }

  private[this] def framePartitionedRequest(requests: ThriftStructIface, original: Req) = {
    val serializedRequest = requestSerializer
      .serialize(rpcName, requests, thriftMarshallable.isOneway(original))
    thriftMarshallable.framePartitionedRequest(serializedRequest, original)
  }

  private[this] def mergeRequest(rpcName: String): RequestMerger[ThriftStructIface] = {
    val optMerger = hashingStrategy match {
      case clientHashingStrategy: ClientHashingStrategy =>
        clientHashingStrategy.requestMergerRegistry.get(rpcName)
    }
    optMerger match {
      case Some(merger) => merger
      case None =>
        throw new PartitioningStrategyException(
          s"cannot find the request merger for thrift method: $rpcName")
    }
  }

  private[this] def rpcName: String =
    ClientDeserializeCtx.get.rpcName.getOrElse("N/A")

  final protected def mergeResponses(
    originalReq: Req,
    results: PartitioningService.PartitionedResults[Req, Rep]
  ): Rep = {
    val responseMerger = hashingStrategy match {
      case clientHashingStrategy: ClientHashingStrategy =>
        clientHashingStrategy.responseMergerRegistry.get(rpcName) match {
          case Some(merger) => merger
          case None =>
            throw new PartitioningStrategyException(
              s"cannot find the response merger for thrift method: $rpcName")
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

  // apply the user provided getHashingKeyAndRequest to the original request,
  // get a map of hashing keys to sub-requests.
  // note: this function should be only evaluate once per-request
  private[this] def getKeyAndRequestMap: Map[Any, ThriftStructIface] = {
    val inputArg = ClientDeserializeCtx.get.request.asInstanceOf[ThriftStructIface]
    try {
      val getKeyAndRequest = { ts: ThriftStructIface =>
        hashingStrategy match {
          case clientHashingStrategy: ClientHashingStrategy =>
            clientHashingStrategy.getHashingKeyAndRequest
              .applyOrElse(ts, ClientHashingStrategy.defaultHashingKeyAndRequest)
        }
      }
      getKeyAndRequest(inputArg)
    } catch {
      case NonFatal(e) => throw new HashingStrategyException(e.getMessage)
    }
  }
}
