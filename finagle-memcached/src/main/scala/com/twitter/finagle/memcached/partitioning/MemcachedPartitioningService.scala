package com.twitter.finagle.memcached.partitioning

import com.twitter.finagle.Stack.Params
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.{Memcached, _}
import com.twitter.hashing.KeyHasher
import com.twitter.io.Buf
import org.apache.commons.io.Charsets

/**
 * MemcachedPartitioningService provides Ketama consistent hashing based partitioning for the
 * Memcached protocol.
 */
private[finagle] object MemcachedPartitioningService {

  private[finagle] val role = Stack.Role("MemcachedKetamaPartitioning")
  private[finagle] val description =
    "Partitioning Service based on Ketama consistent hashing for memcached protocol"

  def module: Stackable[ServiceFactory[Command, Response]] =
    new KetamaPartitioningService.Module[Command, Response, String] {

      override val role: Stack.Role = MemcachedPartitioningService.role
      override val description: String = MemcachedPartitioningService.description

      def newKetamaPartitioningService(
        underlying: Stack[ServiceFactory[Command, Response]],
        params: Params
      ): KetamaPartitioningService[Command, Response, String] = {

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
) extends KetamaPartitioningService[Command, Response, String](
      underlying,
      params,
      keyHasher,
      numReps
    ) {

  final override protected def getKeyBytes(key: String): Array[Byte] = {
    key.getBytes(Charsets.UTF_8)
  }

  final override protected def getPartitionKeys(command: Command): Seq[String] = {

    def extractKey(keyBuf: Buf): String = {
      val Buf.Utf8(key: String) = keyBuf
      key
    }

    command match {
      case rc: RetrievalCommand =>
        rc.keys.map(extractKey)
      case sc: StorageCommand =>
        Seq(extractKey(sc.key))
      case ac: ArithmeticCommand =>
        Seq(extractKey(ac.key))
      case delete: Delete =>
        Seq(extractKey(delete.key))
      case _ =>
        throw new IllegalStateException(s"Unexpected command: $command")
    }
  }

  /**
   * This method is expected to be invoked only for batched requests (get/getv/gets). It barfs
   * upon unexpected invocations.
   */
  final override protected def createPartitionRequestForKeys(
    command: Command,
    pKeys: Seq[String]
  ): Command = {
    command match {
      case get: Get =>
        get.copy(keys = pKeys.map(Buf.Utf8(_)))
      case gets: Gets =>
        gets.copy(keys = pKeys.map(Buf.Utf8(_)))
      case getv: Getv =>
        getv.copy(keys = pKeys.map(Buf.Utf8(_)))
      case _ =>
        throw new IllegalStateException(s"Unexpected invocation: $command")
    }
  }

  /**
   * This method is expected to be invoked only for merging responses for batched requests
   * (get/getv/gets). It barfs upon unexpected invocations.
   */
  final override protected def mergeResponses(responses: Seq[Response]): Response = {
    Values(responses.flatMap {
      case Values(values) => values
      case nonvalue =>
        throw new IllegalStateException(s"Expected Values, instead found $nonvalue")
    })
  }
}
