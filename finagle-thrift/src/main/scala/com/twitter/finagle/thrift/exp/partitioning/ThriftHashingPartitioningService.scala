package com.twitter.finagle.thrift.exp.partitioning

import com.twitter.finagle.partitioning.ConsistentHashPartitioningService.NoPartitioningKeys
import com.twitter.finagle.partitioning.param.NumReps
import com.twitter.finagle.partitioning.ConsistentHashPartitioningService
import com.twitter.finagle.partitioning.PartitioningService
import com.twitter.finagle.thrift.ClientDeserializeCtx
import com.twitter.finagle.thrift.exp.partitioning.PartitioningStrategy.RequestMerger
import com.twitter.finagle.thrift.exp.partitioning.ThriftPartitioningService.PartitioningStrategyException
import com.twitter.finagle.thrift.exp.partitioning.ThriftPartitioningService.ReqRepMarshallable
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.Stack
import com.twitter.hashing.KeyHasher
import com.twitter.io.Buf
import com.twitter.scrooge.ThriftStructIface
import com.twitter.util.Future
import scala.util.control.NonFatal

/**
 * A [[ConsistentHashPartitioningService]] for Thrift messages.
 * @see [[PartitioningService]].
 */
final private[finagle] class ThriftHashingPartitioningService[Req, Rep](
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

  private[this] def rpcName: String = ClientDeserializeCtx.get.rpcName.getOrElse("N/A")

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
  ): Future[Map[Req, Seq[Future[Service[Req, Rep]]]]] = {
    val keyAndRequest = getKeyAndRequestMap
    if (keyAndRequest.isEmpty || keyAndRequest.head._1 == None) {
      noPartitionInformationHandler(request)
    } else {
      val grouped = keyAndRequest
        .groupBy {
          case (key, _) => partitionServiceForKey(key)
        }
      // if everything is going to the same partition, we can avoid re-serializing
      Future.value(if (grouped.size == 1) {
        grouped.map { case (fsvc, _) => request -> Seq(fsvc) }
      } else {
        grouped.map {
          // if there's only one request going to a given partition, we don't need to remerge the requests
          case (svc, shardKeyAndRequestMap) if shardKeyAndRequestMap.size == 1 =>
            (framePartitionedRequest(shardKeyAndRequestMap.head._2, request), Seq(svc))
          case (svc, shardKeyAndRequestMap) =>
            (
              framePartitionedRequest(
                mergeRequest(rpcName)(shardKeyAndRequestMap.values.toSeq),
                request),
              Seq(svc))
        }
      })
    }
  }

  private[this] def framePartitionedRequest(requests: ThriftStructIface, original: Req): Req = {
    val serializedRequest = requestSerializer
      .serialize(rpcName, requests, thriftMarshallable.isOneway(original))
    thriftMarshallable.framePartitionedRequest(serializedRequest, original)
  }

  private[this] def mergeRequest(rpcName: String): RequestMerger[ThriftStructIface] = {
    val optMerger = hashingStrategy match {
      case clientHashingStrategy: ClientHashingStrategy =>
        clientHashingStrategy.requestMergerRegistry.get(rpcName)
      case mbHashingStrategy: MethodBuilderHashingStrategy[_, _] =>
        mbHashingStrategy
        //upcasting, MethodBuilderHashingStrategy[Req <: ThriftStructIface, _]
          .asInstanceOf[MethodBuilderHashingStrategy[ThriftStructIface, _]]
          .requestMerger
    }
    optMerger match {
      case Some(merger) => merger
      case None =>
        throw new PartitioningStrategyException(
          s"cannot find the request merger for thrift method: $rpcName")
    }
  }

  final protected def mergeResponses(
    originalReq: Req,
    results: PartitioningService.PartitionedResults[Req, Rep]
  ): Rep = {
    val mergerOption = hashingStrategy match {
      case clientHashingStrategy: ClientHashingStrategy =>
        clientHashingStrategy.responseMergerRegistry.get(rpcName)
      case mbCustomStrategy: MethodBuilderHashingStrategy[_, _] =>
        mbCustomStrategy
          .asInstanceOf[MethodBuilderHashingStrategy[_, Any]]
          .responseMerger
    }
    val responseMerger = mergerOption match {
      case Some(merger) => merger
      case None =>
        throw new PartitioningStrategyException(
          s"cannot find the response merger for thrift method: $rpcName")
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
          case mbHashingStrategy: MethodBuilderHashingStrategy[_, _] =>
            mbHashingStrategy
            //upcasting, MethodBuilderHashingStrategy[Req <: ThriftStructIface, _]
              .asInstanceOf[MethodBuilderHashingStrategy[ThriftStructIface, _]]
              .getHashingKeyAndRequest(ts)
        }
      }
      getKeyAndRequest(inputArg)
    } catch {
      case castEx: ClassCastException =>
        // applied the wrong request type to getHashingKeyAndRequest
        throw new PartitioningStrategyException(
          "MethodBuilder Strategy request type doesn't match with the actual request type, " +
            "please check the MethodBuilderHashingStrategy type.",
          castEx)
      case NonFatal(e) => throw new PartitioningStrategyException(e.getMessage)
    }
  }
}
