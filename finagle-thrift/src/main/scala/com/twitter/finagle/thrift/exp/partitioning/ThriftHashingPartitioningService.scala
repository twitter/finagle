package com.twitter.finagle.thrift.exp.partitioning

import com.twitter.finagle.param.Logger
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
import com.twitter.logging.Level
import com.twitter.scrooge.{ThriftStruct, ThriftStructIface}
import com.twitter.util.{Future, Return, Throw}
import scala.collection.compat.immutable.ArraySeq
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

  private[this] val logger = params[Logger].log

  final protected def getKeyBytes(key: Any): Array[Byte] =
    Buf.ByteArray.Owned.extract(Buf.U32BE(key.hashCode()))

  final override protected def getPartitionFor(
    partitionedRequest: Req
  ): Future[Service[Req, Rep]] = {
    val keyMap = getKeyAndRequestMap
    if (keyMap.isEmpty || keyMap.head._1 == None) {
      // HashingPartitioningStrategy.defaultHashingKeyAndRequest set the key as None for
      // undefined endpoints(method) in PartitioningStrategy. It indicates those requests
      // for certain endpoint won't be served in PartitioningService.
      failedProcessRequest(partitionedRequest)
    } else {
      // All keys in the request are assumed to belong to the same partition, so use the
      // first key to find the associated partition.
      partitionServiceForKey(keyMap.head._1)
    }
  }

  final protected def failedProcessRequest(req: Req): Future[Nothing] = {
    val ex = new NoPartitioningKeys(
      s"NoPartitioningKeys in for the thrift method: ${ClientDeserializeCtx.get.rpcName.getOrElse(None)}")
    if (logger.isLoggable(Level.DEBUG))
      logger.log(Level.DEBUG, "partitionRequest failed: ", ex)
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
          clientHashingStrategy.requestMergerRegistry.get(rpcName)
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
    val successesRep = results.successes.map(_._2)
    val failuresRep = results.failures.map(_._2)

    val responseMerger = hashingStrategy match {
      case client: ClientHashingStrategy => {
        ClientDeserializeCtx.get.rpcName.flatMap { rpcName =>
          client.responseMergerRegistry.get(rpcName)
        } match {
          case Some(merger) => merger
          case None =>
            throw new IllegalArgumentException(
              s"cannot find the response merger for thrift method: " +
                s"${ClientDeserializeCtx.get.rpcName.getOrElse("N/A")}")
        }
      }
    }

    val deserializedSuccesses = ArraySeq.newBuilder[Any]
    val deserializedFailures = ArraySeq.newBuilder[Throwable]
    deserializedFailures ++= failuresRep

    successesRep.foreach { response =>
      ClientDeserializeCtx.get.deserializeFromBatched(
        thriftMarshallable.fromResponseToBytes(response)
      ) match {
        case Return(rep) => deserializedSuccesses += rep
        case Throw(t) => deserializedFailures += t
      }
    }

    val mergedResponse =
      responseMerger(deserializedSuccesses.result(), deserializedFailures.result())

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
              .applyOrElse(ts, HashingPartitioningStrategy.defaultHashingKeyAndRequest)
        }
      }
      getKeyAndRequest(inputArg)
    } catch {
      case NonFatal(e) => throw new HashingStrategyException(e.getMessage)
    }
  }
}
