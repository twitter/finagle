package com.twitter.finagle.thrift

import com.twitter.finagle.stats.{Counter, DefaultStatsReceiver, StatsReceiver}
import com.twitter.logging.Logger
import java.nio.{ByteBuffer, CharBuffer}
import java.nio.charset.{CharsetEncoder, CoderResult, CodingErrorAction, StandardCharsets}
import java.security.{PrivilegedExceptionAction, AccessController}
import org.apache.thrift.protocol.{
  TMultiplexedProtocol,
  TProtocol,
  TProtocolFactory,
  TBinaryProtocol
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

  // JDK9 Strings are no longer backed by Array[Char] - http://openjdk.java.net/jeps/254
  private val StringsBackedByCharArray = try {
    // Versioning changes between 8 and 9 - http://openjdk.java.net/jeps/223
    System.getProperty("java.specification.version").replace("1.", "").toInt < 9
  } catch {
    case _: Throwable => false
  }

  /**
   * The JVM property of size limit on the incoming Thrift message
   */
  private[thrift] val SysPropReadLength: Long =
    System.getProperty("org.apache.thrift.readLength", "-1").toLong

  /**
   * No size limit on the incoming Thrift message, value is -1
   */
  private[thrift] val NoReadLimit: Int = -1

  private[this] def optimizedBinarySupported: Boolean = unsafe.isDefined && StringsBackedByCharArray

  /**
   * Returns a shorter read size limit when it is set both by system property and a method parameter
   *
   * @param readLength method parameter
   */
  private[thrift] def getReadLimit(readLength: Long): Long = {
    val readLengthOption = if (readLength <= NoReadLimit) None else Some(readLength)
    val sysPropReadLengthOption =
      if (SysPropReadLength <= NoReadLimit) None else Some(SysPropReadLength)

    (readLengthOption, sysPropReadLengthOption) match {
      case (Some(validatedReadLength), Some(validatedSystemLength)) =>
        validatedReadLength.min(validatedSystemLength)
      case (validatedReadLength, validatedSystemLength) =>
        validatedReadLength.orElse(validatedSystemLength).getOrElse(NoReadLimit)
    }
  }

  /**
   * Returns a `TProtocolFactory` that creates `TProtocol`s that
   * are wire-compatible with `TBinaryProtocol`.
   */
  def binaryFactory(
    strictRead: Boolean = false,
    strictWrite: Boolean = true,
    readLength: Long = -1,
    statsReceiver: StatsReceiver = DefaultStatsReceiver
  ): TProtocolFactory = {
    val readLengthLimit: Long = getReadLimit(readLength)
    if (!optimizedBinarySupported) {
      new TBinaryProtocol.Factory(strictRead, strictWrite, readLengthLimit, NoReadLimit)
    } else {
      // Factories are created rarely while the creation of their TProtocol's
      // is a common event. Minimize counter creation to just once per Factory.
      val largerThanTlOutBuffer = statsReceiver.counter("larger_than_threadlocal_out_buffer")
      new TProtocolFactory {
        override def getProtocol(trans: TTransport): TProtocol = {
          new TFinagleBinaryProtocol(
            trans,
            largerThanTlOutBuffer,
            readLengthLimit,
            NoReadLimit,
            strictRead,
            strictWrite
          )
        }
      }
    }
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
    stringLengthLimit: Long = -1,
    containerLengthLimit: Long = -1,
    strictRead: Boolean = false,
    strictWrite: Boolean = true
  ) extends TBinaryProtocol(
        trans,
        stringLengthLimit,
        containerLengthLimit,
        strictRead,
        strictWrite
      ) {
    import TFinagleBinaryProtocol._

    override def writeString(str: String) {
      if (str.length == 0) {
        trans.write(EmptyStringInBytes)
        return
      }
      // this is based on the CharsetEncoder code at:
      // http://psy-lob-saw.blogspot.co.nz/2013/04/writing-java-micro-benchmarks-with-jmh.html
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

    // Note: libthrift 0.5.0 has a bug when operating on ByteBuffer's with a non-zero arrayOffset.
    // We instead use the version from head that fixes this issue.
    override def writeBinary(bin: ByteBuffer) {
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
