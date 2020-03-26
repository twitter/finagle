package com.twitter.finagle.thrift

import com.twitter.finagle.stats.{Counter, DefaultStatsReceiver, StatsReceiver}
import com.twitter.logging.Logger
import java.nio.{ByteBuffer, CharBuffer}
import java.nio.charset.{CharsetEncoder, CoderResult, CodingErrorAction, StandardCharsets}
import java.security.{AccessController, PrivilegedExceptionAction}
import org.apache.thrift.protocol.{
  TBinaryProtocol,
  TCompactProtocol,
  TMultiplexedProtocol,
  TProtocol,
  TProtocolFactory
}
import org.apache.thrift.transport.TTransport
import scala.util.control.NonFatal

object Protocols {

  // based on guava's UnsignedBytes.getUnsafe()
  private[this] def getUnsafe: sun.misc.Unsafe = {
    try {
      sun.misc.Unsafe.getUnsafe()
    } catch {
      case NonFatal(_) => // try reflection instead
        try {
          AccessController.doPrivileged(new PrivilegedExceptionAction[sun.misc.Unsafe]() {
            def run(): sun.misc.Unsafe = {
              val k = classOf[sun.misc.Unsafe]
              for (f <- k.getDeclaredFields) {
                f.setAccessible(true)
                val x = f.get(null)
                if (k.isInstance(x)) {
                  return k.cast(x)
                }
              }
              throw new NoSuchFieldException("the Unsafe") // fall through to the catch block below
            }
          })
        } catch {
          case NonFatal(_) =>
            Logger.get().info("%s unable to initialize sun.misc.Unsafe", getClass.getName)
            null
        }
    }
  }

  private val unsafe: Option[sun.misc.Unsafe] = Option(getUnsafe)

  // JDK9 Strings are no longer backed by Array[Char] - https://openjdk.java.net/jeps/254
  private val StringsBackedByCharArray =
    try {
      // Versioning changes between 8 and 9 - https://openjdk.java.net/jeps/223
      System.getProperty("java.specification.version").replace("1.", "").toInt < 9
    } catch {
      case _: Throwable => false
    }

  private[thrift] def limitToOption(limit: Long): Option[Long] =
    if (limit > NoLimit) Some(limit) else None

  private[thrift] def minLimit(a: Option[Long], b: Option[Long]): Option[Long] = {
    (a, b) match {
      case (Some(al), Some(bl)) => Some(al.min(bl))
      case _ => a.orElse(b)
    }
  }

  /**
   * These JVM properties limit the max number of characters allowed in any one String
   * or the max number of bytes in a binary found in a thrift object.
   *
   * The minimum value of the two is used, and only one needs to be specified.
   * org.apache.thrift.readLength is supported for backwards compatibility.
   */
  private[thrift] val SysPropStringLengthLimit: Option[Long] = {
    val stringLengthLimit =
      limitToOption(System.getProperty("com.twitter.finagle.thrift.stringLengthLimit", "-1").toLong)
    val readLimit = limitToOption(System.getProperty("org.apache.thrift.readLength", "-1").toLong)
    minLimit(stringLengthLimit, readLimit)
  }

  /**
   * This JVM property limits the max number of elements in a single collection in a thrift object.
   */
  private[thrift] val SysPropContainerLengthLimit: Option[Long] =
    limitToOption(
      System.getProperty("com.twitter.finagle.thrift.containerLengthLimit", "-1").toLong)

  /**
   * Represents no limit for the two limits above
   */
  private[thrift] val NoLimit: Long = -1

  private[this] def optimizedBinarySupported: Boolean = unsafe.isDefined && StringsBackedByCharArray

  /**
   * Returns a `TProtocolFactory` that creates `TProtocol`s that
   * are wire-compatible with `TBinaryProtocol`.
   */
  def binaryFactory(
    strictRead: Boolean = false,
    strictWrite: Boolean = true,
    stringLengthLimit: Long = NoLimit,
    containerLengthLimit: Long = NoLimit,
    statsReceiver: StatsReceiver = DefaultStatsReceiver
  ): TProtocolFactory = {
    val stringLengthLimitToUse: Long =
      minLimit(limitToOption(stringLengthLimit), SysPropStringLengthLimit).getOrElse(NoLimit)

    val containerLengthLimitToUse: Long =
      minLimit(limitToOption(containerLengthLimit), SysPropContainerLengthLimit).getOrElse(NoLimit)

    if (!optimizedBinarySupported) {
      new TBinaryProtocol.Factory(
        strictRead,
        strictWrite,
        stringLengthLimitToUse,
        containerLengthLimitToUse)
    } else {
      // Factories are created rarely while the creation of their TProtocol's
      // is a common event. Minimize counter creation to just once per Factory.
      val largerThanTlOutBuffer = statsReceiver.counter("larger_than_threadlocal_out_buffer")
      new TProtocolFactory {
        override def getProtocol(trans: TTransport): TProtocol = {
          new TFinagleBinaryProtocol(
            trans,
            largerThanTlOutBuffer,
            stringLengthLimitToUse,
            containerLengthLimitToUse,
            strictRead,
            strictWrite
          )
        }
      }
    }
  }

  /**
   * Returns a `TProtocolFactory` that creates `TProtocol`s that
   * are wire-compatible with `TCompactProtocol`.
   */
  def compactFactory(
    stringLengthLimit: Long = NoLimit,
    containerLengthLimit: Long = NoLimit
  ): TProtocolFactory = {

    val stringLengthLimitToUse: Long =
      minLimit(limitToOption(stringLengthLimit), SysPropStringLengthLimit).getOrElse(NoLimit)

    val containerLengthLimitToUse: Long =
      minLimit(limitToOption(containerLengthLimit), SysPropContainerLengthLimit).getOrElse(NoLimit)

    new TCompactProtocol.Factory(stringLengthLimitToUse, containerLengthLimitToUse)
  }

  def factory(statsReceiver: StatsReceiver = DefaultStatsReceiver): TProtocolFactory = {
    binaryFactory(statsReceiver = statsReceiver)
  }

  def multiplex(serviceName: String, protocolFactory: TProtocolFactory): TProtocolFactory = {
    new TProtocolFactory {
      def getProtocol(transport: TTransport): TMultiplexedProtocol = {
        new TMultiplexedProtocol(protocolFactory.getProtocol(transport), serviceName)
      }
    }
  }

  // Visible for testing purposes.
  private[thrift] object TFinagleBinaryProtocol {

    /**
     * JDK 9 introduced a change in how Strings are stored.
     * This is known as compact strings: https://openjdk.java.net/jeps/254
     * With that change, the optimization below no longer works.
     * A future effort could look into supporting something similar with compact strings.
     */
    val usesCompactStrings: Boolean = {
      // minimum JDK version is 8.
      // Java 8 has the format: 1.8.0_211
      // Java 9 and higher has the format: 9.0.1, 11.0.4, 12, 12.0.1
      val version = System.getProperty("java.version", "")
      !version.startsWith("1.8")
    }

    // zero-length strings are written to the wire as an i32 of its length, which is 0
    private val EmptyStringInBytes = Array[Byte](0, 0, 0, 0)

    // sun.nio.cs.UTF_8.Encoder.maxBytesPerChar because
    // ArrayEncoder.encode does not check for overflow
    private val MultiByteMultiplierEstimate = 3.0f

    /** Only valid if unsafe is defined */
    private val StringValueOffset: Long = unsafe
      .map {
        _.objectFieldOffset(classOf[String].getDeclaredField("value"))
      }
      .getOrElse(Long.MinValue)

    /**
     * Note, some versions of the JDK's define `String.offset`,
     * while others do not and always use 0.
     */
    private val OffsetValueOffset: Long = unsafe
      .map { u =>
        try {
          u.objectFieldOffset(classOf[String].getDeclaredField("offset"))
        } catch {
          case NonFatal(_) => Long.MinValue
        }
      }
      .getOrElse(Long.MinValue)

    /**
     * Note, some versions of the JDK's define `String.count`,
     * while others do not and always use `value.length`.
     */
    private val CountValueOffset: Long = unsafe
      .map { u =>
        try {
          u.objectFieldOffset(classOf[String].getDeclaredField("count"))
        } catch {
          case NonFatal(_) => Long.MinValue
        }
      }
      .getOrElse(Long.MinValue)

    private val charsetEncoder = new ThreadLocal[CharsetEncoder] {
      override def initialValue(): CharsetEncoder =
        StandardCharsets.UTF_8
          .newEncoder()
          .onMalformedInput(CodingErrorAction.REPLACE)
          .onUnmappableCharacter(CodingErrorAction.REPLACE)
    }

    // Visible for testing purposes
    private[thrift] val OutBufferSize = 16384

    private val outByteBuffer = new ThreadLocal[ByteBuffer] {
      override def initialValue(): ByteBuffer = ByteBuffer.allocate(OutBufferSize)
    }
  }

  /**
   * An implementation of TBinaryProtocol that optimizes `writeString`
   * to minimize object allocations.
   *
   * This specific speedup depends on sun.misc.Unsafe and will fall
   * back to standard TBinaryProtocol in the case when it is unavailable.
   *
   * Visible for testing purposes.
   */
  private[thrift] class TFinagleBinaryProtocol(
    trans: TTransport,
    largerThanTlOutBuffer: Counter,
    stringLengthLimit: Long,
    containerLengthLimit: Long,
    strictRead: Boolean,
    strictWrite: Boolean)
      extends TBinaryProtocol(
        trans,
        stringLengthLimit,
        containerLengthLimit,
        strictRead,
        strictWrite
      ) {
    import TFinagleBinaryProtocol._

    private[this] def writeNonCompactString(str: String): Unit = {
      // this is based on the CharsetEncoder code at:
      // https://psy-lob-saw.blogspot.co.nz/2013/04/writing-java-micro-benchmarks-with-jmh.html
      // we could probably do better than this via:
      // https://github.com/nitsanw/jmh-samples/blob/master/src/main/java/psy/lob/saw/utf8/CustomUtf8Encoder.java
      val u = unsafe.get
      val chars = u.getObject(str, StringValueOffset).asInstanceOf[Array[Char]]
      val offset =
        if (OffsetValueOffset == Long.MinValue) 0
        else {
          u.getInt(str, OffsetValueOffset)
        }
      val count =
        if (CountValueOffset == Long.MinValue) chars.length
        else {
          u.getInt(str, CountValueOffset)
        }

      val out = if (count * MultiByteMultiplierEstimate <= OutBufferSize) {
        val o = outByteBuffer.get()
        o.clear()
        o
      } else {
        largerThanTlOutBuffer.incr()
        ByteBuffer.allocate((count * MultiByteMultiplierEstimate).toInt)
      }

      val csEncoder = charsetEncoder.get()
      csEncoder.reset()

      csEncoder match {
        case arrayEncoder: sun.nio.cs.ArrayEncoder =>
          val blen = arrayEncoder.encode(chars, offset, count, out.array())
          out.position(blen)
        case _ =>
          val charBuffer = CharBuffer.wrap(chars, offset, count)
          csEncoder.encode(charBuffer, out, true /* endOfInput */ ) != CoderResult.UNDERFLOW
      }

      writeI32(out.position())
      trans.write(out.array(), 0, out.position())
    }

    override def writeString(str: String): Unit = {
      if (str.length == 0) {
        trans.write(EmptyStringInBytes)
        return
      }

      if (usesCompactStrings) {
        super.writeString(str)
      } else {
        writeNonCompactString(str)
      }
    }

    // Note: libthrift 0.5.0 has a bug when operating on ByteBuffer's with a non-zero arrayOffset.
    // We instead use the version from head that fixes this issue.
    override def writeBinary(bin: ByteBuffer): Unit = {
      if (bin.hasArray) {
        val length = bin.remaining()
        writeI32(length)
        trans.write(bin.array(), bin.position() + bin.arrayOffset(), length)
      } else {
        val array = new Array[Byte](bin.remaining())
        bin.duplicate().get(array)
        writeI32(array.length)
        trans.write(array, 0, array.length)
      }
    }
  }

}
