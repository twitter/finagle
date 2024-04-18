package com.twitter.finagle.memcached

import com.twitter.finagle._
import com.twitter.finagle.memcached.compressing.CompressionProvider
import com.twitter.finagle.memcached.compressing.param.CompressionParam
import com.twitter.finagle.memcached.compressing.scheme.CompressionScheme
import com.twitter.finagle.memcached.compressing.scheme.Lz4
import com.twitter.finagle.memcached.compressing.scheme.MemcachedCompression.FlagsAndBuf
import com.twitter.finagle.memcached.compressing.scheme.Uncompressed
import com.twitter.finagle.memcached.protocol.Add
import com.twitter.finagle.memcached.protocol.Cas
import com.twitter.finagle.memcached.protocol.Command
import com.twitter.finagle.memcached.protocol.NonStorageCommand
import com.twitter.finagle.memcached.protocol.Response
import com.twitter.finagle.memcached.protocol.RetrievalCommand
import com.twitter.finagle.memcached.protocol.Set
import com.twitter.finagle.memcached.protocol.StorageCommand
import com.twitter.finagle.memcached.protocol.Value
import com.twitter.finagle.memcached.protocol.Values
import com.twitter.finagle.memcached.protocol.ValuesAndErrors
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.io.Buf
import com.twitter.util.Future
import com.twitter.util.Return
import com.twitter.util.Throw
import scala.collection.mutable

private[finagle] object CompressingMemcachedFilter {

  /**
   * Apply the [[CompressingMemcachedFilter]] protocol specific annotations
   */
  def memcachedCompressingModule: Stackable[ServiceFactory[Command, Response]] =
    new Stack.Module2[CompressionParam, param.Stats, ServiceFactory[Command, Response]] {
      override val role: Stack.Role = Stack.Role("MemcachedCompressing")
      override val description: String = "Memcached filter with compression logic"

      override def make(
        _compressionParam: CompressionParam,
        _stats: param.Stats,
        next: ServiceFactory[Command, Response]
      ): ServiceFactory[Command, Response] = {
        _compressionParam.scheme match {
          case Uncompressed =>
            new CompressingMemcachedFilter(Uncompressed, _stats.statsReceiver).andThen(next)
          case Lz4 =>
            new CompressingMemcachedFilter(Lz4, _stats.statsReceiver).andThen(next)
        }
      }
    }
}

private[finagle] final class CompressingMemcachedFilter(
  compressionScheme: CompressionScheme,
  statsReceiver: StatsReceiver)
    extends SimpleFilter[Command, Response] {
  private final val compressionFactory =
    CompressionProvider(compressionScheme, statsReceiver)

  override def apply(command: Command, service: Service[Command, Response]): Future[Response] = {
    command match {
      case storageCommand: StorageCommand =>
        if (compressionScheme == Uncompressed) {
          service(storageCommand)
        } else { service(compress(storageCommand)) }
      case nonStorageCommand: NonStorageCommand =>
        decompress(nonStorageCommand, service)
      case otherCommand => service(otherCommand)
    }
  }

  private def compressedBufAndFlags(value: Buf, flags: Int): FlagsAndBuf = {
    val (compressionFlags, compressedBuf) = compressionFactory.compressor(value)
    (CompressionScheme.flagsWithCompression(flags, compressionFlags), compressedBuf)
  }

  def compress(command: StorageCommand): StorageCommand = {
    command match {
      case Set(key, flags, expiry, value) =>
        val (flagsWithCompression, compressedBuf) =
          compressedBufAndFlags(value, flags)

        Set(key, flagsWithCompression, expiry, compressedBuf)

      case Add(key, flags, expiry, value) =>
        val (flagsWithCompression, compressedBuf) =
          compressedBufAndFlags(value, flags)

        Add(key, flagsWithCompression, expiry, compressedBuf)

      case Cas(key, flags, expiry, value, casUnique) =>
        val (flagsWithCompression, compressedBuf) =
          compressedBufAndFlags(value, flags)

        Cas(key, flagsWithCompression, expiry, compressedBuf, casUnique)

      case unsupportedStorageCommand => unsupported(unsupportedStorageCommand.name)
    }
  }

  def decompress(
    command: NonStorageCommand,
    service: Service[Command, Response]
  ): Future[Response] = {
    command match {
      case retrievalCommand: RetrievalCommand =>
        service(retrievalCommand).map {
          case values: Values =>
            val decompressedValues =
              decompressValues(values.values)
            Values(decompressedValues)
          case valuesAndErrors: ValuesAndErrors =>
            val decompressedValuesAndErrors =
              decompressValuesAndErrors(valuesAndErrors)
            decompressedValuesAndErrors
          case retrievalResponse => retrievalResponse
        }
      case otherNonStorageCommand => service(otherNonStorageCommand)
    }
  }

  private[finagle] def decompressValues(
    values: Seq[Value],
  ): Seq[Value] = {
    val decompressedValuesList = mutable.ArrayBuffer[Value]()

    values.foreach { value =>
      val flagsInt = flagsFromValues(value)

      compressionFactory.decompressor((flagsInt, value.value)) match {
        case Throw(ex) => throw ex
        case Return(uncompressedValue) =>
          decompressedValuesList.append(value.copy(value = uncompressedValue))
      }
    }

    decompressedValuesList.toSeq
  }

  private[finagle] def decompressValuesAndErrors(
    valueAndErrors: ValuesAndErrors,
  ): ValuesAndErrors = {
    // Decompress values. If for some reason this fails, move the values to failures.
    val decompressedValuesList = mutable.ArrayBuffer[Value]()
    val failuresList = mutable.ListBuffer[(Buf, Throwable)]()

    val values = valueAndErrors.values

    values.foreach { value =>
      try {
        val flagsInt = flagsFromValues(value)

        compressionFactory.decompressor((flagsInt, value.value)) match {
          case Return(decompressedValue) =>
            decompressedValuesList.append(value.copy(value = decompressedValue))
          case Throw(ex) => failuresList.append(value.key -> ex)
        }
      } catch {
        case ex: Throwable => failuresList.append(value.key -> ex)
      }
    }

    valueAndErrors.copy(
      values = decompressedValuesList.toSeq,
      errors = valueAndErrors.errors ++ failuresList.toMap)
  }

  private def flagsFromValues(value: Value): Int = {
    // The flags value is stored as a literal string, from the memcache text protocol.
    value.flags.flatMap(Buf.Utf8.unapply(_)) match {
      case Some(s) => s.toInt
      case None => 0
    }
  }

  private def unsupported(command: String) = throw new UnsupportedOperationException(
    s"$command is unsupported for compressing cache")
}
