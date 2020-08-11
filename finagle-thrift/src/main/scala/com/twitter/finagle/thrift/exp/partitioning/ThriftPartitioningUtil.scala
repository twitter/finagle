package com.twitter.finagle.thrift.exp.partitioning

import com.twitter.finagle.partitioning.PartitioningService
import com.twitter.finagle.thrift.ClientDeserializeCtx
import com.twitter.finagle.thrift.exp.partitioning.PartitioningStrategy.ResponseMerger
import com.twitter.util.Try

private[partitioning] object ThriftPartitioningUtil {

  def mergeResponses[Req, Rep](
    results: PartitioningService.PartitionedResults[Req, Rep],
    responseMerger: ResponseMerger[Any],
    fromResponseToBytes: Rep => Array[Byte]
  ): Try[Any] = {
    val (successesRep, failuresRep) = results.successes
      .map {
        case (_, response) =>
          ClientDeserializeCtx.get.deserializeFromBatched(fromResponseToBytes(response))
      }
      .partition(_.isReturn)

    responseMerger(
      successesRep.map(_.get()),
      results.failures.map(_._2) ++ failuresRep.map(_.throwable))
  }
}
