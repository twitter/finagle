package com.twitter.finagle.memcached.partitioning

import com.twitter.finagle.Stack.Params
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.partitioning.ConsistentHashPartitioningService.NoPartitioningKeys
import com.twitter.finagle.partitioning.PartitioningService.PartitionedResults
import com.twitter.finagle.partitioning.param.NumReps
import com.twitter.finagle.partitioning.{ConsistentHashPartitioningService, param}
import com.twitter.finagle.util.DefaultLogger
import com.twitter.finagle.{param => _, _}
import com.twitter.hashing.KeyHasher
import com.twitter.io.Buf
import com.twitter.logging.Level
import com.twitter.util.Future

/**
 * MemcachedPartitioningService provides Ketama consistent hashing based partitioning for the
 * Memcached protocol.
 */
private[finagle] object MemcachedPartitioningService {

  private[finagle] class UnsupportedCommand(msg: String) extends Exception
  private[finagle] class UnsupportedBatchCommand(msg: String) extends Exception
  private[finagle] class UnsupportedResponse(msg: String) extends Exception

  private[finagle] val role = Stack.Role("MemcachedKetamaPartitioning")
  private[finagle] val description =
    "Partitioning Service based on Ketama consistent hashing for memcached protocol"

  def module: Stackable[ServiceFactory[Command, Response]] =
    new ConsistentHashPartitioningService.Module[Command, Response, Buf] {

      override val role: Stack.Role = MemcachedPartitioningService.role
      override val description: String = MemcachedPartitioningService.description

      def newConsistentHashPartitioningService(
        underlying: Stack[ServiceFactory[Command, Response]],
        params: Params
      ): ConsistentHashPartitioningService[Command, Response, Buf] = {

        val param.KeyHasher(hasher) = params[param.KeyHasher]
        val param.NumReps(numReps) = params[param.NumReps]

        new MemcachedPartitioningService(underlying, params, hasher, numReps)
      }
    }
}

private[finagle] class MemcachedPartitioningService(
  underlying: Stack[ServiceFactory[Command, Response]],
  params: Stack.Params,
  keyHasher: KeyHasher = KeyHasher.KETAMA,
  numReps: Int = NumReps.Default)
    extends ConsistentHashPartitioningService[Command, Response, Buf](
      underlying,
      params,
      keyHasher,
      numReps
    ) {
  import MemcachedPartitioningService._

  private[this] val logger = DefaultLogger

  final override protected def getKeyBytes(key: Buf): Array[Byte] = {
    Buf.ByteArray.Owned.extract(key)
  }

  final override protected def getPartitionKeys(command: Command): Seq[Buf] = {
    command match {
      case rc: RetrievalCommand =>
        rc.keys
      case sc: StorageCommand =>
        Seq(sc.key)
      case ac: ArithmeticCommand =>
        Seq(ac.key)
      case delete: Delete =>
        Seq(delete.key)
      case _ =>
        if (logger.isLoggable(Level.DEBUG))
          logger.log(Level.DEBUG, s"UnsupportedCommand: $command")
        throw new UnsupportedCommand(s"Unsupported command: $command")
    }
  }

  /**
   * This method is expected to be invoked only for batched requests (get/getv/gets). It barfs
   * upon unexpected invocations.
   */
  final override protected def createPartitionRequestForKeys(
    command: Command,
    pKeys: Seq[Buf]
  ): Command = {
    command match {
      case get: Get =>
        get.copy(keys = pKeys)
      case gets: Gets =>
        gets.copy(keys = pKeys)
      case getv: Getv =>
        getv.copy(keys = pKeys)
      case _ =>
        if (logger.isLoggable(Level.DEBUG))
          logger.log(Level.DEBUG, s"UnsupportedBatchCommand: $command")
        throw new UnsupportedBatchCommand(s"Unsupported batch command: $command")
    }
  }

  /**
   * This method is expected to be invoked only for merging responses for batched requests
   * (get/getv/gets). It barfs upon unexpected invocations.
   */
  final override protected def mergeResponses(
    origReq: Command,
    pr: PartitionedResults[Command, Response]
  ): Response = {
    ValuesAndErrors(
      values = pr.successes.flatMap {
        case (_, Values(values)) => values
        case (_, nonValue) =>
          if (logger.isLoggable(Level.DEBUG))
            logger
              .log(Level.DEBUG, s"UnsupportedResponse: Expected Values, instead found $nonValue")
          throw new UnsupportedResponse(s"Expected Values, instead found $nonValue")
      },
      errors = pr.failures.flatMap {
        case (cmd, t) => getPartitionKeys(cmd).map(_ -> t)
      }.toMap
    )
  }

  final protected def noPartitionInformationHandler(req: Command): Future[Nothing] = {
    val ex = new NoPartitioningKeys(s"NoPartitioningKeys in for the thrift method: ${req.name}")
    if (logger.isLoggable(Level.DEBUG))
      logger.log(Level.DEBUG, "partitionRequest failed: ", ex)
    Future.exception(ex)
  }
}
