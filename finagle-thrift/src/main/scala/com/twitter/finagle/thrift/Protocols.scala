package com.twitter.finagle.thrift

import com.google.common.base.Charsets
import com.twitter.finagle.stats.{NullStatsReceiver, Counter, DefaultStatsReceiver, StatsReceiver}
import com.twitter.logging.Logger
import com.twitter.util.NonFatal
import java.nio.{ByteBuffer, CharBuffer}
import java.nio.charset.{CoderResult, CharsetEncoder}
import java.security.{PrivilegedExceptionAction, AccessController}
import org.apache.thrift.protocol.{TProtocol, TProtocolFactory, TBinaryProtocol}
import org.apache.thrift.transport.TTransport

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
          case NonFatal(t) =>
            Logger.get().info("%s unable to initialize sun.misc.Unsafe", getClass.getName)
            null
        }
    }
  }

  private val unsafe: Option[sun.misc.Unsafe] = Option(getUnsafe)

  private[this] def optimizedBinarySupported: Boolean = unsafe.isDefined

  /**
   * Returns a `TProtocolFactory` that creates `TProtocol`s that
   * are wire-compatible with `TBinaryProtocol`.
   */
  def binaryFactory(
    strictRead: Boolean = false,
    strictWrite: Boolean = true,
    readLength: Int = 0,
    statsReceiver: StatsReceiver = DefaultStatsReceiver
  ): TProtocolFactory = {
    if (!optimizedBinarySupported) {
      new TBinaryProtocol.Factory(strictRead, strictWrite, readLength)
    } else {
      // Factories are created rarely while the creation of their TProtocol's
      // is a common event. Minimize counter creation to just once per Factory.
      val fastEncodeFailed = statsReceiver.counter("fast_encode_failed")
      val largerThanTlOutBuffer = statsReceiver.counter("larger_than_threadlocal_out_buffer")
      new TProtocolFactory {
        override def getProtocol(trans: TTransport): TProtocol = {
          val proto = new TFinagleBinaryProtocol(
            trans, fastEncodeFailed, largerThanTlOutBuffer, strictRead, strictWrite)
          if (readLength != 0) {
            proto.setReadLength(readLength)
          }
          proto
        }
      }
    }
  }

  def factory(statsReceiver: StatsReceiver = DefaultStatsReceiver): TProtocolFactory = {
    binaryFactory(statsReceiver = statsReceiver)
  }

  // Visible for testing purposes.
  private[thrift] object TFinagleBinaryProtocol {
    // zero-length strings are written to the wire as an i32 of its length, which is 0
    private val EmptyStringInBytes = Array[Byte](0, 0, 0, 0)

    // assume that most of our strings are mostly single byte utf8
    private val MultiByteMultiplierEstimate = 1.3f

    /** Only valid if unsafe is defined */
    private val StringValueOffset: Long = unsafe.map {
      _.objectFieldOffset(classOf[String].getDeclaredField("value"))
    }.getOrElse(Long.MinValue)

    /**
     * Note, some versions of the JDK's define `String.offset`,
     * while others do not and always use 0.
     */
    private val OffsetValueOffset: Long = unsafe.map { u =>
      try {
        u.objectFieldOffset(classOf[String].getDeclaredField("offset"))
      } catch {
        case NonFatal(_) => Long.MinValue
      }
    }.getOrElse(Long.MinValue)

    /**
     * Note, some versions of the JDK's define `String.count`,
     * while others do not and always use `value.length`.
     */
    private val CountValueOffset: Long = unsafe.map { u =>
      try {
        u.objectFieldOffset(classOf[String].getDeclaredField("count"))
      } catch {
        case NonFatal(_) => Long.MinValue
      }
    }.getOrElse(Long.MinValue)

    private val charsetEncoder = new ThreadLocal[CharsetEncoder] {
      override def initialValue() = Charsets.UTF_8.newEncoder()
    }

    // Visible for testing purposes
    private[thrift] val OutBufferSize = 4096

    private val outByteBuffer = new ThreadLocal[ByteBuffer] {
      override def initialValue() = ByteBuffer.allocate(OutBufferSize)
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
      fastEncodeFailed: Counter,
      largerThanTlOutBuffer: Counter,
      strictRead: Boolean = false,
      strictWrite: Boolean = true)
    extends TBinaryProtocol(
      trans,
      strictRead,
      strictWrite)
  {
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
      val offset = if (OffsetValueOffset == Long.MinValue) 0 else {
        u.getInt(str, OffsetValueOffset)
      }
      val count = if (CountValueOffset == Long.MinValue) chars.length else {
        u.getInt(str, CountValueOffset)
      }
      val charBuffer = CharBuffer.wrap(chars, offset, count)

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

      val result = csEncoder.encode(charBuffer, out, true)
      if (result != CoderResult.UNDERFLOW) {
        fastEncodeFailed.incr()
        super.writeString(str)
      } else {
        writeI32(out.position())
        trans.write(out.array(), 0, out.position())
      }
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
