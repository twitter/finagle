package com.twitter.finagle.thrift.exp.partitioning

import com.twitter.finagle.partitioning.ConsistentHashPartitioningService.{
  HashingStrategyException,
  NoPartitioningKeys
}
import com.twitter.finagle.partitioning.{ConsistentHashPartitioningService, PartitioningService}
import com.twitter.finagle.thrift.ClientDeserializeCtx
import com.twitter.finagle.thrift.exp.partitioning.PartitioningStrategy.RequestMerger
import com.twitter.finagle.thrift.exp.partitioning.ThriftPartitioningService.ReqRepMarshallable
import com.twitter.finagle.{Service, ServiceFactory, Stack}
import com.twitter.hashing.KeyHasher
import com.twitter.io.Buf
import com.twitter.scrooge.{ThriftStruct, ThriftStructIface}
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
  numReps: Int = ConsistentHashPartitioningService.DefaultNumReps)
    extends ConsistentHashPartitioningService[Req, Rep, Any](
      underlying,
      params,
      keyHasher,
      numReps
    ) {

  final protected def getKeyBytes(key: Any): Array[Byte] =
    Buf.ByteArray.Owned.extract(Buf.U32BE(key.hashCode()))

  final override protected def getPartitionFor(
    partitionedRequest: Req
  ): Future[Service[Req, Rep]] = {
    val keyMap = getKeyAndRequestMap
    if (keyMap.isEmpty || keyMap.head._1 == None) {
      // HashingPartitioningStrategy.defaultHashingKeyAndRequest set the key as None for
      // undefined endpoints(methods) in PartitioningStrategy. It indicates those requests
      // for certain endpoint won't be served in PartitioningService.
      noPartitionInformationHandler(partitionedRequest)
    } else {
      // All keys in the request are assumed to belong to the same partition, so use the
      // first key to find the associated partition.
      partitionServiceForKey(keyMap.head._1)
    }
  }

  final protected def noPartitionInformationHandler(req: Req): Future[Nothing] = {
    val ex = new NoPartitioningKeys(
      s"No Partitioning hashing keys for the thrift method: ${ClientDeserializeCtx.get.rpcName.getOrElse("N/A")}")
    Future.exception(ex)
  }

  final override protected def getPartitionKeys(request: Req): Seq[Any] =
    getKeyAndRequestMap.map(_._1).toSeq

  final protected def createPartitionRequestForKeys(original: Req, keys: Seq[Any]): Req = {
    val requests = keys.flatMap(getKeyAndRequestMap.get)
    val requestSerializer = new ThriftRequestSerializer(params)

    val requestMerger: String => Option[RequestMerger[ThriftStructIface]] = { rpcName: String =>
      hashingStrategy match {
        case clientHashingStrategy: ClientHashingStrategy =>
          clientHashingStrategy.requestMergerRegistry().get(rpcName)
      }
    }

    val serializedRequest = for {
      rpcName <- ClientDeserializeCtx.get.rpcName
      merger <- requestMerger(rpcName)
    } yield {
      requestSerializer.serialize(
        rpcName,
        merger(requests).asInstanceOf[ThriftStruct],
        thriftMarshallable.isOneway(original))
    }

    serializedRequest match {
      case Some(r) => thriftMarshallable.framePartitionedRequest(r, original)
      case None =>
        throw new IllegalArgumentException(
          s"cannot find the request merger for thrift method: " +
            s"${ClientDeserializeCtx.get.rpcName.getOrElse("N/A")}")
    }
  }

  final protected def isSinglePartition(request: Req): Future[Boolean] =
    Future.value(allKeysForSinglePartition(request))

  final protected def mergeResponses(
    originalReq: Req,
    results: PartitioningService.PartitionedResults[Req, Rep]
  ): Rep = {
    val responseMerger = hashingStrategy match {
      case clientHashingStrategy: ClientHashingStrategy =>
        ClientDeserializeCtx.get.rpcName.flatMap { rpcName =>
          clientHashingStrategy.responseMergerRegistry().get(rpcName)
        } match {
          case Some(merger) => merger
          case None =>
            throw new IllegalArgumentException(
              s"cannot find the response merger for thrift method: " +
                s"${ClientDeserializeCtx.get.rpcName.getOrElse("N/A")}")
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
