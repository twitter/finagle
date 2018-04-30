package com.twitter.finagle.memcached.partitioning

import com.twitter.finagle.Stack.Params
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.param.Logger
import com.twitter.finagle.{Memcached, _}
import com.twitter.hashing.KeyHasher
import com.twitter.io.Buf
import com.twitter.logging.Level

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
    new KetamaPartitioningService.Module[Command, Response, Buf] {

      override val role: Stack.Role = MemcachedPartitioningService.role
      override val description: String = MemcachedPartitioningService.description

      def newKetamaPartitioningService(
        underlying: Stack[ServiceFactory[Command, Response]],
        params: Params
      ): KetamaPartitioningService[Command, Response, Buf] = {

        val Memcached.param.KeyHasher(hasher) = params[Memcached.param.KeyHasher]
        val Memcached.param.NumReps(numReps) = params[Memcached.param.NumReps]

        new MemcachedPartitioningService(underlying, params, hasher, numReps)
      }
    }
}

private[finagle] class MemcachedPartitioningService(
  underlying: Stack[ServiceFactory[Command, Response]],
  params: Stack.Params,
  keyHasher: KeyHasher = KeyHasher.KETAMA,
  numReps: Int = KetamaPartitioningService.DefaultNumReps
) extends KetamaPartitioningService[Command, Response, Buf](
      underlying,
      params,
      keyHasher,
      numReps
    ) {
  import MemcachedPartitioningService._

  private[this] val logger = params[Logger].log

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
    successes: Seq[Response],
    failures: Map[Command, Throwable]
  ): Response = {
    ValuesAndErrors(
      successes.flatMap {
        case Values(values) =>
          values
        case nonValue =>
          if (logger.isLoggable(Level.DEBUG))
            logger.log(Level.DEBUG, s"UnsupportedResponse: Expected Values, instead found $nonValue")
          throw new UnsupportedResponse(s"Expected Values, instead found $nonValue")
      },
      failures.flatMap { case (command, t) =>
        getPartitionKeys(command).map(_ -> t)
      }
    )
  }

  protected def isSinglePartition(request: Command): Boolean = request match {
    case _: StorageCommand | _: ArithmeticCommand | _: Delete => true
    case _ => false
  }
}
