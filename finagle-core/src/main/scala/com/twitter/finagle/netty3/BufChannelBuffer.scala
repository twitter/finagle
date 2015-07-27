package com.twitter.finagle.netty3

import com.twitter.io.Buf
import java.io.{InputStream, OutputStream}
import java.nio.{ByteBuffer, ByteOrder, ReadOnlyBufferException}
import java.nio.channels.{GatheringByteChannel, ScatteringByteChannel}
import org.jboss.netty.buffer.{AbstractChannelBuffer, ChannelBuffer, ChannelBuffers, ChannelBufferFactory}

/**
 * Class BufChannelBufferFactory is a Netty ChannelBufferFactory that
 * creates read-only ChannelBuffers based on [[com.twitter.util.Buf
 * Buf]]s. They are a thin API wrapper on top of
 * [[com.twitter.util.Buf Buf]]; no additional allocations are
 * performed.
 */
object BufChannelBufferFactory {
  private val beFactory = new BufChannelBufferFactory(ByteOrder.BIG_ENDIAN)
  private val leFactory = new BufChannelBufferFactory(ByteOrder.LITTLE_ENDIAN)

  /**
   * Get a ChannelBufferFactory with ByteOrder.BIG_ENDIAN
   */
  def apply(): ChannelBufferFactory = beFactory

  /**
   * Get a ChannelBufferFactory with `endianness` ByteOrder
   */
  def apply(endianness: ByteOrder): ChannelBufferFactory = endianness match {
    case ByteOrder.BIG_ENDIAN => beFactory
    case ByteOrder.LITTLE_ENDIAN => leFactory
  }
}

/**
 * A ChannelBufferFactory which produces read-only ChannelBuffers.
 */
private class BufChannelBufferFactory(defaultOrder: ByteOrder) extends ChannelBufferFactory {
  /**
   * Returns a read-only ChannelBuffer whose content is filled with
   * `capacity` zeros and ByteOrder.BIG_ENDIAN.
   */
  def getBuffer(capacity: Int): ChannelBuffer = getBuffer(defaultOrder, capacity)

  /**
   * Returns a read-only ChannelBuffer whose content is filled with
   * `capacity` zeros and `order` endianness.
   */
  def getBuffer(order: ByteOrder, capacity: Int): ChannelBuffer =
    new BufChannelBuffer(Buf.ByteArray.Owned(new Array[Byte](capacity)), order)

  /**
   * Returns a read-only ChannelBuffer whose content is equal to the
   * sub-region of the specified array with BytOrder.BIG_ENDIAN.
   */
  def getBuffer(array: Array[Byte], offset: Int, length: Int): ChannelBuffer =
    getBuffer(defaultOrder, array, offset, length)


  /**
   * Returns a read-only ChannelBuffer whose content is equal to the
   * sub-region of the specified array with `order` endianness.
   */
  def getBuffer(order: ByteOrder, array: Array[Byte], offset: Int, length: Int): ChannelBuffer = {
    ChannelBuffers.unmodifiableBuffer(
      ChannelBuffers.wrappedBuffer(order, array, offset, length))
  }

  /**
   * Returns a read-only ChannelBuffer whose content is equal to the sub-region
   * of the specified nioBuffer.
   */
  def getBuffer(nioBuffer: ByteBuffer): ChannelBuffer = {
    val bytes = new Array[Byte](nioBuffer.remaining())
    nioBuffer.get(bytes)
    getBuffer(bytes, 0, bytes.length)
  }

  def getDefaultOrder(): ByteOrder = defaultOrder
}

object BufChannelBuffer {

  /**
   * Creates a ChannelBuffer from `buf` with `endianness` ByteOrder.
   *
   * The returned ChannelBuffer should not be mutated.
   */
  def apply(buf: Buf, endianness: ByteOrder): ChannelBuffer = buf match {
    case empty if empty.isEmpty =>
      ChannelBuffers.EMPTY_BUFFER

    case ChannelBufferBuf.Owned(cb) if endianness == cb.order =>
      cb

    case Buf.ByteArray.Owned(bytes, begin, end) =>
      ChannelBuffers.wrappedBuffer(endianness, bytes, begin, end-begin)

    case Buf.ByteBuffer.Owned(bb) =>
      ChannelBuffers.wrappedBuffer(bb)

    case _ =>
      new BufChannelBuffer(buf, endianness)
  }

  /** Creates a ChannelBuffer from `buf` with big-endian ByteOrder. */
  def apply(buf: Buf): ChannelBuffer = apply(buf, ByteOrder.BIG_ENDIAN)

  /** Extract a Buf from a BufChannelBuffer. */
  def unapply(bcb: BufChannelBuffer): Option[Buf] = Some(bcb.buf)
}

/**
 * A [[org.jboss.netty.buffer.ChannelBuffer]] wrapper for
 * [[com.twitter.io.Buf Bufs]].
 *
 * @note Since `Buf`s are immutable, all `set` methods of this class throw
 * [[java.nio.ReadOnlyBufferException]]. These same semantics apply to `slice`s
 * taken from `BufChannelBuffer`s.
 *
 * @param buf The [[com.twitter.io.Buf]] to be wrapped in a
 * [[org.jboss.netty.buffer.ChannelBuffer]] interface.
 * @param endianness The endianness of `buf`, which will be reflected in the
 * `ChannelBuffer` wrapper.
 */
private class BufChannelBuffer(val buf: Buf, endianness: ByteOrder) extends AbstractChannelBuffer {
  writerIndex(buf.length)

  def this(buf: Buf) = this(buf, ByteOrder.BIG_ENDIAN)

  def factory(): ChannelBufferFactory = BufChannelBufferFactory(endianness)

  def order() = endianness

  def isDirect(): Boolean = false

  def hasArray() = false

  def array() = throw new ReadOnlyBufferException()

  def arrayOffset(): Int = throw new ReadOnlyBufferException()

  override def discardReadBytes() { throw new ReadOnlyBufferException() }

  def setByte(index: Int, value: Int) { throw new ReadOnlyBufferException() }

  def setBytes(index: Int, src: ChannelBuffer, srcIndex: Int, length: Int) {
    throw new ReadOnlyBufferException()
  }

  def setBytes(index: Int, src: Array[Byte], srcIndex: Int, length: Int) {
    throw new ReadOnlyBufferException()
  }

  def setBytes(index: Int, src: ByteBuffer) {
    throw new ReadOnlyBufferException()
  }

  def setShort(index: Int, value: Int) {
    throw new ReadOnlyBufferException()
  }

  def setMedium(index: Int, value: Int) {
    throw new ReadOnlyBufferException()
  }

  def setInt(index: Int, value: Int) {
    throw new ReadOnlyBufferException()
  }

  def setLong(index: Int, value: Long) {
    throw new ReadOnlyBufferException()
  }

  def setBytes(index: Int, in: InputStream, length: Int): Int = {
    throw new ReadOnlyBufferException()
  }

  def setBytes(index: Int, in: ScatteringByteChannel, length: Int): Int = {
    throw new ReadOnlyBufferException()
  }

  def getBytes(index: Int, out: GatheringByteChannel, length: Int): Int = {
    throw new UnsupportedOperationException()
  }

  def getBytes(index: Int, out: OutputStream, length: Int) {
    val s = buf.slice(index, index + length)
    val a = new Array[Byte](s.length)
    s.write(a, 0)
    out.write(a)
  }

  def getBytes(index: Int, dst: Array[Byte], dstIndex: Int, length: Int) {
    if (index < 0 || dstIndex < 0 || index + length > buf.length || dstIndex + length > dst.length)
      throw new IndexOutOfBoundsException()

    buf.slice(index, index + length).write(dst, dstIndex)
  }

  def getBytes(index: Int, dst: ChannelBuffer, dstIndex: Int, length: Int) {
    val s = buf.slice(index, index + length)
    val a = new Array[Byte](s.length)
    s.write(a, 0)
    dst.setBytes(dstIndex, a)
  }

  def getBytes(index: Int, dst: ByteBuffer) {
    if (index < 0)
      throw new IndexOutOfBoundsException()

    if (index >= buf.length)
      throw new IndexOutOfBoundsException()

    val c = (buf.length - index) min dst.remaining()
    val a = new Array[Byte](c)
    buf.slice(index, index + c).write(a, 0)
    dst.put(a)
  }

  def duplicate(): ChannelBuffer = {
    val dup = new BufChannelBuffer(buf, endianness)
    dup.setIndex(readerIndex(), writerIndex())
    dup
  }

  def copy(index: Int, length: Int): ChannelBuffer = {
    if (index < 0)
      throw new IndexOutOfBoundsException("index < 0")

    if (index > buf.length)
      throw new IndexOutOfBoundsException("index > buf.length")

    if (index + length > buf.length)
      throw new IndexOutOfBoundsException("index + length > buf.length")

    val bcb = new BufChannelBuffer(buf.slice(index, index + length), endianness)
    bcb.writerIndex(length)
    bcb
  }

  def slice(index: Int, length: Int): ChannelBuffer = {
    if (index < 0)
      throw new IndexOutOfBoundsException("index < 0")

    if (index > buf.length)
      throw new IndexOutOfBoundsException("index > buf.length")

    if (index + length > buf.length)
      throw new IndexOutOfBoundsException("index + length > buf.length")

    val bcb = new BufChannelBuffer(buf.slice(index, index + length), endianness)
    bcb.writerIndex(length)
    bcb
  }

  def getByte(index: Int): Byte = {
    if (index < 0)
      throw new IndexOutOfBoundsException()

    if (index >= buf.length)
      throw new IndexOutOfBoundsException()

    val one = new Array[Byte](1)
    buf.slice(index, index + 1).write(one, 0)
    one(0)
  }

  def getShort(index: Int): Short = {
    if (index < 0)
      throw new IndexOutOfBoundsException()

    if (index + 2 > buf.length)
      throw new IndexOutOfBoundsException()

    val bytes = new Array[Byte](2)
    buf.slice(index, index + 2).write(bytes, 0)
    endianness match {
      case ByteOrder.BIG_ENDIAN =>
        (((bytes(0) & 0xff) << 8) | (bytes(1) & 0xff)).toShort
      case ByteOrder.LITTLE_ENDIAN =>
        ((bytes(0) & 0xff) | ((bytes(1) & 0xff)  << 8)).toShort
    }
  }

  def getUnsignedMedium(index: Int): Int = {
    if (index < 0)
      throw new IndexOutOfBoundsException()

    if (index + 3 > buf.length)
      throw new IndexOutOfBoundsException()

    val bytes = new Array[Byte](3)
    buf.slice(index, index + 3).write(bytes, 0)
    endianness match {
      case ByteOrder.BIG_ENDIAN =>
        ((bytes(0) & 0xff) << 16) |
        ((bytes(1) & 0xff) << 8) |
         (bytes(2) & 0xff)
      case ByteOrder.LITTLE_ENDIAN =>
        (bytes(0) & 0xff) |
        ((bytes(1) & 0xff) << 8) |
        ((bytes(2) & 0xff) << 16)
    }
  }

  def getInt(index: Int): Int = {
    if (index < 0)
      throw new IndexOutOfBoundsException()

    if (index + 4 > buf.length)
      throw new IndexOutOfBoundsException()

    val bytes = new Array[Byte](4)
    buf.slice(index, index + 4).write(bytes, 0)
    endianness match {
      case ByteOrder.BIG_ENDIAN =>
        ((bytes(0) & 0xff) << 24) |
        ((bytes(1) & 0xff) << 16) |
        ((bytes(2) & 0xff) << 8) |
         (bytes(3) & 0xff)
      case ByteOrder.LITTLE_ENDIAN =>
        (bytes(0) & 0xff) |
        ((bytes(1) & 0xff) << 8) |
        ((bytes(2) & 0xff) << 16) |
        ((bytes(3) & 0xff) << 24)
    }
  }

  def getLong(index: Int): Long = {
    if (index < 0)
      throw new IndexOutOfBoundsException()

    if (index + 8 > buf.length)
      throw new IndexOutOfBoundsException()

    val bytes = new Array[Byte](8)
    buf.slice(index, index + 8).write(bytes, 0)
    endianness match {
      case ByteOrder.BIG_ENDIAN =>
        ((bytes(0) & 0xff).toLong << 56) |
        ((bytes(1) & 0xff).toLong << 48) |
        ((bytes(2) & 0xff).toLong << 40) |
        ((bytes(3) & 0xff).toLong << 32) |
        ((bytes(4) & 0xff).toLong << 24) |
        ((bytes(5) & 0xff).toLong << 16) |
        ((bytes(6) & 0xff).toLong << 8) |
         (bytes(7) & 0xff).toLong
      case ByteOrder.LITTLE_ENDIAN =>
        (bytes(0) & 0xff).toLong |
        ((bytes(1) & 0xff).toLong << 8) |
        ((bytes(2) & 0xff).toLong << 16) |
        ((bytes(3) & 0xff).toLong << 24)
        ((bytes(4) & 0xff).toLong << 32)
        ((bytes(5) & 0xff).toLong << 40)
        ((bytes(6) & 0xff).toLong << 48)
        ((bytes(7) & 0xff).toLong << 56)
    }
  }

  def toByteBuffer(index: Int, length: Int): ByteBuffer = {
    if (index + length > buf.length)
      throw new IndexOutOfBoundsException()

    val bytes = new Array[Byte](length)
    buf.slice(index, index + length).write(bytes, 0)
    ByteBuffer.wrap(bytes)
  }

  def capacity(): Int = buf.length

  override def readBytes(length: Int): ChannelBuffer = {
    checkReadableBytes(length)
    if (length == 0)
      return ChannelBuffers.EMPTY_BUFFER

    val offset = readerIndex()
    readerIndex(offset + length)
    val bcb = new BufChannelBuffer(buf.slice(offset, offset + length), endianness)
    bcb.writerIndex(length)
    bcb
  }
}
