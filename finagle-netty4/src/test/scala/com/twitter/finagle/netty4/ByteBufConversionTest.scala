package com.twitter.finagle.netty4

import com.twitter.io.Buf
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.util.CharsetUtil
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.{ByteBuffer, ReadOnlyBufferException}
import java.util._
import org.junit.Assert._
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class ByteBufConversionTest extends AnyFunSuite with BeforeAndAfter {
  private[this] val Capacity = 4096 // Must be even
  private[this] val BlockSize = 128
  private[this] var seed: Long = 0
  private[this] var random: Random = null
  private[this] var buffer: ByteBuf = null

  before {
    buffer =
      ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(Array.fill[Byte](Capacity)(0.toByte)))
    seed = System.currentTimeMillis()
    random = new Random(seed)
  }

  after {
    buffer = null
  }

  test("initial state") {
    assertEquals(Capacity, buffer.capacity)
    assertEquals(0, buffer.readerIndex)
  }

  test("reader index boundary check 1") {
    try {
      buffer.writerIndex(0)
    } catch {
      case e: IndexOutOfBoundsException =>
        fail()
    }
    intercept[IndexOutOfBoundsException] {
      buffer.readerIndex(-1)
    }
  }

  test("reader index boundary check 2") {
    try {
      buffer.writerIndex(buffer.capacity)
    } catch {
      case e: IndexOutOfBoundsException =>
        fail()
    }
    intercept[IndexOutOfBoundsException] {
      buffer.readerIndex(buffer.capacity + 1)
    }
  }

  test("reader index boundary check 3") {
    try {
      buffer.writerIndex(Capacity / 2)
    } catch {
      case e: IndexOutOfBoundsException =>
        fail()
    }
    intercept[IndexOutOfBoundsException] {
      buffer.readerIndex(Capacity * 3 / 2)
    }
  }

  test("reader index boundary check 4") {
    buffer.writerIndex(0)
    buffer.readerIndex(0)
    buffer.writerIndex(buffer.capacity)
    buffer.readerIndex(buffer.capacity)
  }

  test("writer index boundary check 1") {
    intercept[IndexOutOfBoundsException] {
      buffer.writerIndex(-1)
    }
  }

  test("writer index boundary check 2") {
    try {
      buffer.writerIndex(Capacity)
      buffer.readerIndex(Capacity)
    } catch {
      case e: IndexOutOfBoundsException =>
        fail()
    }
    intercept[IndexOutOfBoundsException] {
      buffer.writerIndex(buffer.capacity + 1)
    }
  }

  test("writer index boundary check 3") {
    try {
      buffer.writerIndex(Capacity)
      buffer.readerIndex(Capacity / 2)
    } catch {
      case e: IndexOutOfBoundsException =>
        fail()
    }
    intercept[IndexOutOfBoundsException] {
      buffer.writerIndex(Capacity / 4)
    }
  }

  test("writer index boundary check 4") {
    buffer.writerIndex(0)
    buffer.readerIndex(0)
    buffer.writerIndex(Capacity)
  }

  test("getByte boundary check 1") {
    intercept[IndexOutOfBoundsException] {
      buffer.getByte(-1)
    }
  }

  test("getByte boundary check 2") {
    intercept[IndexOutOfBoundsException] {
      buffer.getByte(buffer.capacity)
    }
  }

  test("getShort boundary check 1") {
    intercept[IndexOutOfBoundsException] {
      buffer.getShort(-1)
    }
  }

  test("getShort boundary check 2") {
    intercept[IndexOutOfBoundsException] {
      buffer.getShort(buffer.capacity - 1)
    }
  }

  test("getMedium boundary check 1") {
    intercept[IndexOutOfBoundsException] {
      buffer.getMedium(-1)
    }
  }

  test("getMedium boundary check 2") {
    intercept[IndexOutOfBoundsException] {
      buffer.getMedium(buffer.capacity - 2)
    }
  }

  test("getInt boundary check 1") {
    intercept[IndexOutOfBoundsException] {
      buffer.getInt(-1)
    }
  }

  test("getInt boundary check 2") {
    intercept[IndexOutOfBoundsException] {
      buffer.getInt(buffer.capacity - 3)
    }
  }

  test("getLong boundary check 1") {
    intercept[IndexOutOfBoundsException] {
      buffer.getLong(-1)
    }
  }

  test("getLong boundary check 2") {
    intercept[IndexOutOfBoundsException] {
      buffer.getLong(buffer.capacity - 7)
    }
  }

  test("getByteArray boundary check 1") {
    intercept[IndexOutOfBoundsException] {
      buffer.getBytes(-1, new Array[Byte](0))
    }
  }

  test("getByteArray boundary check 2") {
    intercept[IndexOutOfBoundsException] {
      buffer.getBytes(-1, new Array[Byte](0), 0, 0)
    }
  }

  test("getByteArray boundary check 3") {
    val dst = new Array[Byte](4)
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(Array[Byte](1, 2, 3, 4)))
    intercept[IndexOutOfBoundsException] {
      wrappedBuf.getBytes(0, dst, -1, 4)
    }

    // No partial copy is expected.
    assert(0 == dst(0))
    assert(0 == dst(1))
    assert(0 == dst(2))
    assert(0 == dst(3))
  }

  test("getByteArray boundary check 4") {
    val dst = new Array[Byte](4)
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(Array[Byte](1, 2, 3, 4)))
    intercept[IndexOutOfBoundsException] {
      wrappedBuf.getBytes(0, dst, 1, 4)
    }

    // No partial copy is expected.
    assert(0 == dst(0))
    assert(0 == dst(1))
    assert(0 == dst(2))
    assert(0 == dst(3))
  }

  test("getBytes ByteBuffer boundary check") {
    intercept[IndexOutOfBoundsException] {
      buffer.getBytes(-1, ByteBuffer.allocate(0))
    }
  }

  test("copy boundary check 1") {
    intercept[IndexOutOfBoundsException] {
      buffer.copy(-1, 0)
    }
  }

  test("copy boundary check 2") {
    intercept[IndexOutOfBoundsException] {
      buffer.copy(0, buffer.capacity + 1)
    }
  }

  test("copy boundary check 3") {
    intercept[IndexOutOfBoundsException] {
      buffer.copy(buffer.capacity + 1, 0)
    }
  }

  test("copy boundary check 4") {
    intercept[IndexOutOfBoundsException] {
      buffer.copy(buffer.capacity, 1)
    }
  }

  test("setIndex boundary check 1") {
    intercept[IndexOutOfBoundsException] {
      buffer.setIndex(-1, Capacity)
    }
  }

  test("setIndex boundary check 2") {
    intercept[IndexOutOfBoundsException] {
      buffer.setIndex(Capacity / 2, Capacity / 4)
    }
  }

  test("setIndex boundary check 3") {
    intercept[IndexOutOfBoundsException] {
      buffer.setIndex(0, Capacity + 1)
    }
  }

  test("getBytes ByteBuffer state") {
    val dst = ByteBuffer.allocate(4)
    dst.position(1)
    dst.limit(3)

    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(Array[Byte](1, 2, 3, 4)))
    wrappedBuf.getBytes(1, dst)

    assert(3 == dst.position())
    assert(3 == dst.limit())

    dst.clear()
    assert(0 == dst.get(0))
    assert(2 == dst.get(1))
    assert(3 == dst.get(2))
    assert(0 == dst.get(3))
  }

  test("getBytes DirectByteBuffer boundary check") {
    intercept[IndexOutOfBoundsException] {
      buffer.getBytes(-1, ByteBuffer.allocateDirect(0))
    }
  }

  test("getBytes DirectByteBuffer state") {
    val dst = ByteBuffer.allocateDirect(4)
    dst.position(1)
    dst.limit(3)

    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(Array[Byte](1, 2, 3, 4)))
    wrappedBuf.getBytes(1, dst)

    assert(3 == dst.position())
    assert(3 == dst.limit())

    dst.clear()
    assert(0 == dst.get(0))
    assert(2 == dst.get(1))
    assert(3 == dst.get(2))
    assert(0 == dst.get(3))
  }

  test("random byte access") {

    val buf = Buf.ByteArray.Owned(Array.fill(Capacity)(random.nextInt().toByte))
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(buf)

    random.setSeed(seed)
    0.until(Capacity).foreach { i =>
      val value = random.nextInt().asInstanceOf[Byte]
      assert(value == wrappedBuf.getByte(i))
    }
  }

  test("test random unsigned byte access") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    0.until(Capacity).foreach { i =>
      val value = random.nextInt().asInstanceOf[Byte]
      wrapped.setByte(i, value)
    }
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))

    random.setSeed(seed)
    0.until(Capacity).foreach { i =>
      val value = random.nextInt() & 0xff
      assert(value == wrappedBuf.getUnsignedByte(i))
    }
  }

  test("test random short access") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    0.until(Capacity, 2).foreach { i =>
      val value = random.nextInt().asInstanceOf[Short]
      wrapped.setShort(i, value)
    }
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))

    random.setSeed(seed)
    0.until(Capacity, 2).foreach { i =>
      val value = random.nextInt().asInstanceOf[Short]
      assert(value == wrappedBuf.getShort(i))
    }
  }

  test("test random unsigned short access") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    0.until(Capacity, 2).foreach { i =>
      val value = random.nextInt().asInstanceOf[Short]
      wrapped.setShort(i, value)
    }
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))

    random.setSeed(seed)
    0.until(Capacity, 2).foreach { i =>
      val value = random.nextInt() & 0xffff
      assert(value == wrappedBuf.getUnsignedShort(i))
    }
  }

  test("test random medium access") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    0.until(Capacity - 2, 3).foreach { i =>
      val value = random.nextInt()
      wrapped.setMedium(i, value)
    }
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))

    random.setSeed(seed)
    0.until(Capacity - 2, 3).foreach { i =>
      val value = random.nextInt() << 8 >> 8
      assert(value == wrappedBuf.getMedium(i))
    }
  }

  test("test random unsigned medium access") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    0.until(Capacity - 2, 3).foreach { i =>
      val value = random.nextInt()
      wrapped.setMedium(i, value)
    }
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))

    random.setSeed(seed)
    0.until(Capacity - 2, 3).foreach { i =>
      val value = random.nextInt() & 0x00ffffff
      assert(value == wrappedBuf.getUnsignedMedium(i))
    }
  }

  test("test random int access") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    0.until(Capacity - 3, 4).foreach { i =>
      val value = random.nextInt()
      wrapped.setInt(i, value)
    }
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))

    random.setSeed(seed)
    0.until(Capacity - 3, 4).foreach { i =>
      val value = random.nextInt()
      assert(value == wrappedBuf.getInt(i))
    }
  }

  test("test random unsigned int access") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    0.until(Capacity - 3, 4).foreach { i =>
      val value = random.nextInt()
      wrapped.setInt(i, value)
    }
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))

    random.setSeed(seed)
    0.until(Capacity - 3, 4).foreach { i =>
      val value = random.nextInt() & 0xffffffffL
      assert(value == wrappedBuf.getUnsignedInt(i))
    }
  }

  test("test random long access") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    0.until(Capacity - 7, 8).foreach { i =>
      val value = random.nextLong()
      wrapped.setLong(i, value)
    }
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))

    random.setSeed(seed)
    0.until(Capacity - 7, 8).foreach { i =>
      val value = random.nextLong()
      assert(value == wrappedBuf.getLong(i))
    }
  }

  test("setZero") {
    intercept[ReadOnlyBufferException] {
      buffer.setZero(0, 1)
    }
  }

  test("sequential byte access") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    wrapped.writerIndex(0)
    0.until(Capacity).foreach { i =>
      val value = random.nextInt().asInstanceOf[Byte]
      wrapped.writeByte(value)
    }
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))
    assert(0 == wrappedBuf.readerIndex)

    random.setSeed(seed)
    0.until(Capacity).foreach { i =>
      val value = random.nextInt().asInstanceOf[Byte]
      assert(i == wrappedBuf.readerIndex)
      assert(wrappedBuf.isReadable)
      assert(value == wrappedBuf.readByte())
    }

    assert(wrappedBuf.maxCapacity == wrappedBuf.readerIndex)
    assert(!wrappedBuf.isReadable)
  }

  test("sequential unsigned byte access") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    wrapped.writerIndex(0)
    0.until(Capacity).foreach { i =>
      val value = random.nextInt().asInstanceOf[Byte]
      wrapped.writeByte(value)
    }
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))
    assert(0 == wrappedBuf.readerIndex)

    random.setSeed(seed)
    0.until(Capacity).foreach { i =>
      val value = random.nextInt() & 0xff
      assert(i == wrappedBuf.readerIndex)
      assert(wrappedBuf.isReadable)
      assert(value == wrappedBuf.readUnsignedByte())
    }

    assert(wrappedBuf.capacity == wrappedBuf.readerIndex)
    assert(!wrappedBuf.isReadable)
  }

  test("sequential short access") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    wrapped.writerIndex(0)
    0.until(Capacity, 2).foreach { i =>
      val value = random.nextInt().asInstanceOf[Short]
      wrapped.writeShort(value)
    }
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))
    assert(0 == wrappedBuf.readerIndex)

    random.setSeed(seed)
    0.until(Capacity, 2).foreach { i =>
      val value = random.nextInt().asInstanceOf[Short]
      assert(i == wrappedBuf.readerIndex)
      assert(wrappedBuf.isReadable)
      assert(value == wrappedBuf.readShort())
    }

    assert(wrappedBuf.capacity == wrappedBuf.readerIndex)
    assert(!wrappedBuf.isReadable)
  }

  test("sequential unsigned short access") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    wrapped.writerIndex(0)
    0.until(Capacity, 2).foreach { i =>
      val value = random.nextInt().asInstanceOf[Short]
      wrapped.writeShort(value)
    }
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))
    assert(0 == wrappedBuf.readerIndex)

    random.setSeed(seed)
    0.until(Capacity, 2).foreach { i =>
      val value = random.nextInt() & 0xffff
      assert(i == wrappedBuf.readerIndex)
      assert(wrappedBuf.isReadable)
      assert(value == wrappedBuf.readUnsignedShort())
    }

    assert(wrappedBuf.capacity == wrappedBuf.readerIndex)
    assert(!wrappedBuf.isReadable)
  }

  test("sequential medium access") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    wrapped.writerIndex(0)
    val limit = Capacity / 3 * 3
    0.until(limit, 3).foreach { i =>
      val value = random.nextInt()
      wrapped.writeMedium(value)
    }
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))
    assert(0 == wrappedBuf.readerIndex)

    random.setSeed(seed)
    0.until(limit, 3).foreach { i =>
      val value = random.nextInt() << 8 >> 8
      assert(i == wrappedBuf.readerIndex)
      assert(wrappedBuf.isReadable)
      assert(value == wrappedBuf.readMedium())
    }

    assert(limit == wrappedBuf.readerIndex)
  }

  test("sequential unsigned medium access") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    wrapped.writerIndex(0)
    val limit = Capacity / 3 * 3
    0.until(limit, 3).foreach { i =>
      val value = random.nextInt()
      wrapped.writeMedium(value)
    }
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))
    assert(0 == wrappedBuf.readerIndex)

    random.setSeed(seed)
    0.until(limit, 3).foreach { i =>
      val value = random.nextInt() & 0x00ffffff
      assert(i == wrappedBuf.readerIndex)
      assert(wrappedBuf.isReadable)
      assert(value == wrappedBuf.readUnsignedMedium())
    }

    assert(limit == wrappedBuf.readerIndex)
  }

  test("sequential int access") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    wrapped.writerIndex(0)
    0.until(Capacity, 4).foreach { i =>
      val value = random.nextInt()
      wrapped.writeInt(value)
    }
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))
    assert(0 == wrappedBuf.readerIndex)

    random.setSeed(seed)
    0.until(Capacity, 4).foreach { i =>
      val value = random.nextInt()
      assert(i == wrappedBuf.readerIndex)
      assert(wrappedBuf.isReadable)
      assert(value == wrappedBuf.readInt())
    }

    assert(Capacity == wrappedBuf.readerIndex)
  }

  test("sequential unsigned int access") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    wrapped.writerIndex(0)
    0.until(Capacity, 4).foreach { i =>
      val value = random.nextInt()
      wrapped.writeInt(value)
    }
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))
    assert(0 == wrappedBuf.readerIndex)

    random.setSeed(seed)
    0.until(Capacity, 4).foreach { i =>
      val value = random.nextInt() & 0xffffffffL
      assert(i == wrappedBuf.readerIndex)
      assert(wrappedBuf.isReadable)
      assert(value == wrappedBuf.readUnsignedInt())
    }

    assert(Capacity == wrappedBuf.readerIndex)
  }

  test("sequential long access") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    wrapped.writerIndex(0)
    0.until(Capacity, 8).foreach { i =>
      val value = random.nextLong()
      wrapped.writeLong(value)
    }
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))
    assert(0 == wrappedBuf.readerIndex)

    random.setSeed(seed)
    0.until(Capacity, 8).foreach { i =>
      val value = random.nextLong()
      assert(i == wrappedBuf.readerIndex)
      assert(wrappedBuf.isReadable)
      assert(value == wrappedBuf.readLong())
    }

    assert(Capacity == wrappedBuf.readerIndex)
  }

  test("byte array transfer") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    val value = new Array[Byte](BlockSize * 2)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(value)
      wrapped.setBytes(i, value, random.nextInt(BlockSize), BlockSize)
    }
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))

    random.setSeed(seed)
    val expectedValue = new Array[Byte](BlockSize * 2)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(expectedValue)
      val valueOffset = random.nextInt(BlockSize)
      wrappedBuf.getBytes(i, value, valueOffset, BlockSize)
      valueOffset.until(valueOffset + BlockSize).foreach { j =>
        assertEquals(expectedValue(j), value(j))
      }
    }
  }

  test("random byte array transfer 1") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    val value = new Array[Byte](BlockSize)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(value)
      wrapped.setBytes(i, value)
    }
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))

    random.setSeed(seed)
    val expectedValueContent = new Array[Byte](BlockSize)
    val expectedValue = Unpooled.wrappedBuffer(expectedValueContent)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(expectedValueContent)
      wrappedBuf.getBytes(i, value)
      0.until(BlockSize).foreach { j => assertEquals(expectedValue.getByte(j), value(j)) }
    }
  }

  test("random byte array transfer 2") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    val value = new Array[Byte](BlockSize * 2)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(value)
      wrapped.setBytes(i, value, random.nextInt(BlockSize), BlockSize)
    }
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))

    random.setSeed(seed)
    val expectedValueContent = new Array[Byte](BlockSize * 2)
    val expectedValue = Unpooled.wrappedBuffer(expectedValueContent)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(expectedValueContent)
      val valueOffset = random.nextInt(BlockSize)
      wrappedBuf.getBytes(i, value, valueOffset, BlockSize)
      valueOffset.until(valueOffset + BlockSize).foreach { j =>
        assertEquals(expectedValue.getByte(j), value(j))
      }
    }
  }

  test("random heap buffer transfer 1") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    val valueContent = new Array[Byte](BlockSize)
    val value = Unpooled.wrappedBuffer(valueContent)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(valueContent)
      value.setIndex(0, BlockSize)
      wrapped.setBytes(i, value)
      assertEquals(BlockSize, value.readerIndex)
      assertEquals(BlockSize, value.writerIndex())
    }
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))

    random.setSeed(seed)
    val expectedValueContent = new Array[Byte](BlockSize)
    val expectedValue = Unpooled.wrappedBuffer(expectedValueContent)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(expectedValueContent)
      value.clear()
      wrappedBuf.getBytes(i, value)
      assertEquals(0, value.readerIndex)
      assertEquals(BlockSize, value.writerIndex())
      0.until(BlockSize).foreach { j => assertEquals(expectedValue.getByte(j), value.getByte(j)) }
    }
  }

  test("random heap buffer transfer 2") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    val valueContent = new Array[Byte](BlockSize * 2)
    val value = Unpooled.wrappedBuffer(valueContent)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(valueContent)
      wrapped.setBytes(i, value, random.nextInt(BlockSize), BlockSize)
    }
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))

    random.setSeed(seed)
    val expectedValueContent = new Array[Byte](BlockSize * 2)
    val expectedValue = Unpooled.wrappedBuffer(expectedValueContent)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(expectedValueContent)
      val valueOffset = random.nextInt(BlockSize)
      wrappedBuf.getBytes(i, value, valueOffset, BlockSize)
      valueOffset.until(valueOffset + BlockSize).foreach { j =>
        assertEquals(expectedValue.getByte(j), value.getByte(j))
      }
    }
  }

  test("random direct buffer transfer") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    val tmp = new Array[Byte](BlockSize * 2)
    val value = Unpooled.directBuffer(BlockSize * 2)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(tmp)
      value.setBytes(0, tmp, 0, value.capacity)
      wrapped.setBytes(i, value, random.nextInt(BlockSize), BlockSize)
    }
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))

    random.setSeed(seed)
    val expectedValue = Unpooled.directBuffer(BlockSize * 2)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(tmp)
      expectedValue.setBytes(0, tmp, 0, expectedValue.capacity)
      val valueOffset = random.nextInt(BlockSize)
      wrappedBuf.getBytes(i, value, valueOffset, BlockSize)
      valueOffset.until(valueOffset + BlockSize).foreach { j =>
        assertEquals(expectedValue.getByte(j), value.getByte(j))
      }
    }
  }

  test("random ByteBuffer transfer") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    val value = ByteBuffer.allocate(BlockSize * 2)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(value.array())
      value.clear().position(random.nextInt(BlockSize))
      value.limit(value.position() + BlockSize)
      wrapped.setBytes(i, value)
    }
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))

    random.setSeed(seed)
    val expectedValue = ByteBuffer.allocate(BlockSize * 2)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(expectedValue.array())
      val valueOffset = random.nextInt(BlockSize)
      value.clear().position(valueOffset).limit(valueOffset + BlockSize)
      wrappedBuf.getBytes(i, value)
      assertEquals(valueOffset + BlockSize, value.position())
      valueOffset.until(valueOffset + BlockSize).foreach { j =>
        assertEquals(expectedValue.get(j), value.get(j))
      }
    }
  }

  test("sequential Array[Byte] transfer 1") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))
    val value = new Array[Byte](BlockSize)
    wrapped.writerIndex(0)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(value)
      assertEquals(0, wrappedBuf.readerIndex)
      wrapped.writeBytes(value)
    }

    random.setSeed(seed)
    val expectedValue = new Array[Byte](BlockSize)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(expectedValue)
      assert(i == wrappedBuf.readerIndex)
      assertEquals(Capacity, wrappedBuf.writerIndex())
      wrappedBuf.readBytes(value)
      0.until(BlockSize).foreach { j => assertEquals(expectedValue(j), value(j)) }
    }
  }

  test("sequential Array[Byte] transfer 2") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))
    val value = new Array[Byte](BlockSize * 2)
    wrapped.writerIndex(0)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(value)
      assertEquals(0, wrappedBuf.readerIndex)
      val readerIndex = random.nextInt(BlockSize)
      wrapped.writeBytes(value, readerIndex, BlockSize)
    }

    random.setSeed(seed)
    val expectedValue = new Array[Byte](BlockSize * 2)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(expectedValue)
      val valueOffset = random.nextInt(BlockSize)
      assert(i == wrappedBuf.readerIndex)
      assertEquals(Capacity, wrappedBuf.writerIndex())
      wrappedBuf.readBytes(value, valueOffset, BlockSize)
      valueOffset.until(valueOffset + BlockSize).foreach { j =>
        assertEquals(expectedValue(j), value(j))
      }
    }
  }

  test("sequential heap buffer transfer 1") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))
    val valueContent = new Array[Byte](BlockSize * 2)
    val value = Unpooled.wrappedBuffer(valueContent)
    wrapped.writerIndex(0)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(valueContent)
      assertEquals(0, wrappedBuf.readerIndex)
      wrapped.writeBytes(value, random.nextInt(BlockSize), BlockSize)
      assertEquals(0, value.readerIndex)
      assertEquals(valueContent.length, value.writerIndex())
    }

    random.setSeed(seed)
    val expectedValueContent = new Array[Byte](BlockSize * 2)
    val expectedValue = Unpooled.wrappedBuffer(expectedValueContent)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(expectedValueContent)
      val valueOffset = random.nextInt(BlockSize)
      assert(i == wrappedBuf.readerIndex)
      assertEquals(Capacity, wrappedBuf.writerIndex())
      wrappedBuf.readBytes(value, valueOffset, BlockSize)
      valueOffset.until(valueOffset + BlockSize).foreach { j =>
        assertEquals(expectedValue.getByte(j), value.getByte(j))
      }
      assertEquals(0, value.readerIndex)
      assertEquals(valueContent.length, value.writerIndex())
    }
  }

  test("sequential heap buffer transfer 2") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))
    val valueContent = new Array[Byte](BlockSize * 2)
    val value = Unpooled.wrappedBuffer(valueContent)
    wrapped.writerIndex(0)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(valueContent)
      assertEquals(0, wrappedBuf.readerIndex)
      val readerIndex = random.nextInt(BlockSize)
      value.readerIndex(readerIndex)
      value.writerIndex(readerIndex + BlockSize)
      wrapped.writeBytes(value)
      assertEquals(readerIndex + BlockSize, value.writerIndex())
      assertEquals(value.writerIndex(), value.readerIndex)
    }

    random.setSeed(seed)
    val expectedValueContent = new Array[Byte](BlockSize * 2)
    val expectedValue = Unpooled.wrappedBuffer(expectedValueContent)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(expectedValueContent)
      val valueOffset = random.nextInt(BlockSize)
      assert(i == wrappedBuf.readerIndex)
      assertEquals(Capacity, wrappedBuf.writerIndex())
      value.readerIndex(valueOffset)
      value.writerIndex(valueOffset)
      wrappedBuf.readBytes(value, BlockSize)
      valueOffset.until(valueOffset + BlockSize).foreach { j =>
        assertEquals(expectedValue.getByte(j), value.getByte(j))
      }
      assertEquals(valueOffset, value.readerIndex)
      assertEquals(valueOffset + BlockSize, value.writerIndex())
    }
  }

  test("sequential direct buffer transfer 1") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))
    val valueContent = new Array[Byte](BlockSize * 2)
    val value = Unpooled.directBuffer(BlockSize * 2)
    wrapped.writerIndex(0)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(valueContent)
      value.setBytes(0, valueContent)
      assertEquals(0, wrappedBuf.readerIndex)
      wrapped.writeBytes(value, random.nextInt(BlockSize), BlockSize)
      assertEquals(0, value.readerIndex)
      assertEquals(0, value.writerIndex())
    }

    random.setSeed(seed)
    val expectedValueContent = new Array[Byte](BlockSize * 2)
    val expectedValue = Unpooled.wrappedBuffer(expectedValueContent)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(expectedValueContent)
      val valueOffset = random.nextInt(BlockSize)
      value.setBytes(0, valueContent)
      assert(i == wrappedBuf.readerIndex)
      assertEquals(Capacity, wrappedBuf.writerIndex())
      wrappedBuf.readBytes(value, valueOffset, BlockSize)
      valueOffset.until(valueOffset + BlockSize).foreach { j =>
        assertEquals(expectedValue.getByte(j), value.getByte(j))
      }
      assertEquals(0, value.readerIndex)
      assertEquals(0, value.writerIndex())
    }
  }

  test("sequential direct buffer transfer 2") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))
    val valueContent = new Array[Byte](BlockSize * 2)
    val value = Unpooled.directBuffer(BlockSize * 2)
    wrapped.writerIndex(0)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(valueContent)
      value.setBytes(0, valueContent)
      assertEquals(0, wrappedBuf.readerIndex)
      val readerIndex = random.nextInt(BlockSize)
      value.readerIndex(0)
      value.writerIndex(readerIndex + BlockSize)
      value.readerIndex(readerIndex)
      wrapped.writeBytes(value)
      assertEquals(readerIndex + BlockSize, value.writerIndex())
      assertEquals(value.writerIndex(), value.readerIndex)
    }

    random.setSeed(seed)
    val expectedValueContent = new Array[Byte](BlockSize * 2)
    val expectedValue = Unpooled.wrappedBuffer(expectedValueContent)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(expectedValueContent)
      value.setBytes(0, valueContent)
      val valueOffset = random.nextInt(BlockSize)
      assert(i == wrappedBuf.readerIndex)
      assertEquals(Capacity, wrappedBuf.writerIndex())
      value.readerIndex(valueOffset)
      value.writerIndex(valueOffset)
      wrappedBuf.readBytes(value, BlockSize)
      valueOffset.until(valueOffset + BlockSize).foreach { j =>
        assertEquals(expectedValue.getByte(j), value.getByte(j))
      }
      assertEquals(valueOffset, value.readerIndex)
      assertEquals(valueOffset + BlockSize, value.writerIndex())
    }
  }

  test("sequential ByteBuffer-backed heap buffer transfer 1") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))
    val valueContent = new Array[Byte](BlockSize * 2)
    val value = Unpooled.wrappedBuffer(ByteBuffer.allocate(BlockSize * 2))
    value.writerIndex(0)
    wrapped.writerIndex(0)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(valueContent)
      value.setBytes(0, valueContent)
      assertEquals(0, wrappedBuf.readerIndex)
      wrapped.writeBytes(value, random.nextInt(BlockSize), BlockSize)
      assertEquals(0, value.readerIndex)
      assertEquals(0, value.writerIndex())
    }

    random.setSeed(seed)
    val expectedValueContent = new Array[Byte](BlockSize * 2)
    val expectedValue = Unpooled.wrappedBuffer(expectedValueContent)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(expectedValueContent)
      val valueOffset = random.nextInt(BlockSize)
      value.setBytes(0, valueContent)
      assert(i == wrappedBuf.readerIndex)
      assertEquals(Capacity, wrappedBuf.writerIndex())
      wrappedBuf.readBytes(value, valueOffset, BlockSize)
      valueOffset.until(valueOffset + BlockSize).foreach { j =>
        assertEquals(expectedValue.getByte(j), value.getByte(j))
      }
      assertEquals(0, value.readerIndex)
      assertEquals(0, value.writerIndex())
    }
  }

  test("sequential ByteBuffer-backed heap buffer transfer 2") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))
    val valueContent = new Array[Byte](BlockSize * 2)
    val value = Unpooled.wrappedBuffer(ByteBuffer.allocate(BlockSize * 2))
    value.writerIndex(0)
    wrapped.writerIndex(0)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(valueContent)
      value.setBytes(0, valueContent)
      assertEquals(0, wrappedBuf.readerIndex)
      val readerIndex = random.nextInt(BlockSize)
      value.readerIndex(0)
      value.writerIndex(readerIndex + BlockSize)
      value.readerIndex(readerIndex)
      wrapped.writeBytes(value)
      assertEquals(readerIndex + BlockSize, value.writerIndex())
      assertEquals(value.writerIndex(), value.readerIndex)
    }

    random.setSeed(seed)
    val expectedValueContent = new Array[Byte](BlockSize * 2)
    val expectedValue = Unpooled.wrappedBuffer(expectedValueContent)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(expectedValueContent)
      value.setBytes(0, valueContent)
      val valueOffset = random.nextInt(BlockSize)
      assert(i == wrappedBuf.readerIndex)
      assertEquals(Capacity, wrappedBuf.writerIndex())
      value.readerIndex(valueOffset)
      value.writerIndex(valueOffset)
      wrappedBuf.readBytes(value, BlockSize)
      valueOffset.until(valueOffset + BlockSize).foreach { j =>
        assertEquals(expectedValue.getByte(j), value.getByte(j))
      }
      assertEquals(valueOffset, value.readerIndex)
      assertEquals(valueOffset + BlockSize, value.writerIndex())
    }
  }

  test("sequential ByteBuffer transfer") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))
    wrapped.writerIndex(0)
    val value = ByteBuffer.allocate(BlockSize * 2)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(value.array())
      value.clear().position(random.nextInt(BlockSize))
      value.limit(value.position() + BlockSize)
      wrapped.writeBytes(value)
    }
    random.setSeed(seed)
    val expectedValue = ByteBuffer.allocate(BlockSize * 2)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(expectedValue.array())
      val valueOffset = random.nextInt(BlockSize)
      value.clear().position(valueOffset).limit(valueOffset + BlockSize)
      wrappedBuf.readBytes(value)
      assertEquals(valueOffset + BlockSize, value.position())
      valueOffset.until(valueOffset + BlockSize).foreach { j =>
        assertEquals(expectedValue.get(j), value.get(j))
      }
    }
  }

  test("sequential copied buffer transfer") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))
    wrapped.writerIndex(0)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      val value = new Array[Byte](BlockSize)
      random.nextBytes(value)
      assertEquals(0, wrappedBuf.readerIndex)
      wrapped.writeBytes(value)
    }

    random.setSeed(seed)
    val expectedValue = new Array[Byte](BlockSize)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(expectedValue)
      assert(i == wrappedBuf.readerIndex)
      assertEquals(Capacity, wrappedBuf.writerIndex())
      val actualValue = wrappedBuf.readBytes(BlockSize)
      assertEquals(Unpooled.wrappedBuffer(expectedValue), actualValue)
    }
  }

  test("sequential slice") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))
    wrapped.writerIndex(0)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      val value = new Array[Byte](BlockSize)
      random.nextBytes(value)
      assertEquals(0, wrappedBuf.readerIndex)
      wrapped.writeBytes(value)
    }

    random.setSeed(seed)
    val expectedValue = new Array[Byte](BlockSize)
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      random.nextBytes(expectedValue)
      assert(i == wrappedBuf.readerIndex)
      assertEquals(Capacity, wrappedBuf.writerIndex())
      val actualValue = wrappedBuf.readSlice(BlockSize)
      assert(Unpooled.wrappedBuffer(expectedValue) == actualValue)

      // Make sure if it is a read-only buffer.
      intercept[ReadOnlyBufferException] {
        actualValue.setByte(0, 0)
      }
    }
  }

  test("write zero") {
    intercept[ReadOnlyBufferException] {
      buffer.writeZero(-1)
    }

    buffer.clear()

    intercept[ReadOnlyBufferException] {
      buffer.writeZero(Capacity)
    }
  }

  test("discardReadBytes") {
    val bytes = new Array[Byte](Capacity)
    val wrapped = Unpooled.wrappedBuffer(bytes)
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))
    wrapped.writerIndex(0)
    0.until(Capacity, 4).foreach { i => wrapped.writeInt(i) }
    wrappedBuf.readByte()
    intercept[ReadOnlyBufferException] {
      wrappedBuf.discardReadBytes()
    }
  }

  test("stream transfer 1") {
    val bytes = new Array[Byte](Capacity)
    random.nextBytes(bytes)
    val in = new ByteArrayInputStream(bytes, 0, Capacity)
    intercept[ReadOnlyBufferException] {
      buffer.setBytes(0, in, Capacity)
    }

    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))
    val out = new ByteArrayOutputStream()
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      wrappedBuf.getBytes(i, out, BlockSize)
    }

    assert(Arrays.equals(bytes, out.toByteArray()))
  }

  test("stream transfer 2") {
    val bytes = new Array[Byte](Capacity)
    random.nextBytes(bytes)
    val in = new ByteArrayInputStream(bytes, 0, Capacity)
    buffer.clear()
    intercept[ReadOnlyBufferException] {
      buffer.writeBytes(in, Capacity)
    }
    assert(buffer.writerIndex() == 0)

    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))
    val out = new ByteArrayOutputStream()
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      assert(i == wrappedBuf.readerIndex)
      wrappedBuf.readBytes(out, BlockSize)
      assertEquals(i + BlockSize, wrappedBuf.readerIndex)
    }

    assert(Arrays.equals(bytes, out.toByteArray()))
  }

  test("copy") {
    val bytes = new Array[Byte](Capacity)
    random.nextBytes(bytes)
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))

    val readerIndex = Capacity / 3
    val writerIndex = Capacity * 2 / 3
    wrappedBuf.setIndex(readerIndex, writerIndex)

    // Make sure all properties are copied.
    val copy = wrappedBuf.copy()
    assertEquals(0, copy.readerIndex)
    assertEquals(wrappedBuf.readableBytes, copy.writerIndex())
    assertEquals(wrappedBuf.readableBytes, copy.capacity)
    0.until(copy.capacity).foreach { i =>
      assertEquals(wrappedBuf.getByte(i + readerIndex), copy.getByte(i))
    }
  }

  test("duplicate") {
    val bytes = new Array[Byte](Capacity)
    random.nextBytes(bytes)
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))

    val readerIndex = Capacity / 3
    val writerIndex = Capacity * 2 / 3
    wrappedBuf.setIndex(readerIndex, writerIndex)

    // Make sure all properties are copied.
    val duplicate = wrappedBuf.duplicate()
    assertEquals(wrappedBuf.readerIndex, duplicate.readerIndex)
    assertEquals(wrappedBuf.writerIndex(), duplicate.writerIndex())
    assertEquals(wrappedBuf.capacity, duplicate.capacity)
    0.until(duplicate.capacity).foreach { i =>
      assertEquals(wrappedBuf.getByte(i), duplicate.getByte(i))
    }

    // Make sure the duplicate is read-only
    intercept[ReadOnlyBufferException] {
      duplicate.setByte(1, 1)
    }
  }

  test("slice index") {
    assertEquals(0, buffer.slice(0, buffer.capacity).readerIndex)
    assertEquals(0, buffer.slice(0, buffer.capacity - 1).readerIndex)
    assertEquals(0, buffer.slice(1, buffer.capacity - 1).readerIndex)
    assertEquals(0, buffer.slice(1, buffer.capacity - 2).readerIndex)

    assertEquals(buffer.capacity, buffer.slice(0, buffer.capacity).writerIndex())
    assertEquals(buffer.capacity - 1, buffer.slice(0, buffer.capacity - 1).writerIndex())
    assertEquals(buffer.capacity - 1, buffer.slice(1, buffer.capacity - 1).writerIndex())
    assertEquals(buffer.capacity - 2, buffer.slice(1, buffer.capacity - 2).writerIndex())
  }

  test("equals") {
    assert(!buffer.equals(null))
    assert(!buffer.equals(new Object()))

    val value = new Array[Byte](32)
    random.nextBytes(value)
    val bytes = Arrays.copyOf(value, Capacity)
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))
    wrappedBuf.setIndex(0, value.length)

    assertEquals(wrappedBuf, Unpooled.wrappedBuffer(value))

    value(0) = (value(0) + 1).asInstanceOf[Byte]
    assert(!wrappedBuf.equals(Unpooled.wrappedBuffer(value)))
  }

  test("compareTo") {
    intercept[NullPointerException] {
      buffer.compareTo(null)
    }

    // Fill the random stuff
    val value = new Array[Byte](32)
    random.nextBytes(value)
    // Prevent overflow / underflow
    if (value(0) == 0) {
      value(0) = (value(0) + 1).asInstanceOf[Byte]
    } else if (value(0) == -1) {
      value(0) = (value(0) - 1).asInstanceOf[Byte]
    }

    val bytes = Arrays.copyOf(value, Capacity)
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(bytes))
    wrappedBuf.setIndex(0, value.length)

    assertEquals(0, wrappedBuf.compareTo(Unpooled.wrappedBuffer(value)))

    value(0) = (value(0) + 1).asInstanceOf[Byte]
    assert(wrappedBuf.compareTo(Unpooled.wrappedBuffer(value)) < 0)
    value(0) = (value(0) - 2).asInstanceOf[Byte]
    assert(wrappedBuf.compareTo(Unpooled.wrappedBuffer(value)) > 0)
    value(0) = (value(0) + 1).asInstanceOf[Byte]

    assert(wrappedBuf.compareTo(Unpooled.wrappedBuffer(value, 0, 31)) > 0)
    assert(wrappedBuf.slice(0, 31).compareTo(Unpooled.wrappedBuffer(value)) < 0)
  }

  test("toString") {
    val msg = "Hello, World!"
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.Utf8(msg))
    assert("Hello, World!" == wrappedBuf.toString(CharsetUtil.UTF_8))
  }

  test("indexOf") {
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(Array[Byte](1, 2, 3, 2, 1)))

    assertEquals(-1, wrappedBuf.indexOf(1, 4, 1: Byte))
    assertEquals(-1, wrappedBuf.indexOf(4, 1, 1: Byte))
    assertEquals(1, wrappedBuf.indexOf(1, 4, 2: Byte))
    assertEquals(3, wrappedBuf.indexOf(4, 1, 2: Byte))
  }

  test("nioBuffer 1") {
    val value = new Array[Byte](Capacity)
    random.nextBytes(value)
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(value))
    assertEquals(ByteBuffer.wrap(value), wrappedBuf.nioBuffer())
  }

  test("nioBuffer 2") {
    val value = new Array[Byte](Capacity)
    random.nextBytes(value)
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(value))
    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      assertEquals(ByteBuffer.wrap(value, i, BlockSize), wrappedBuf.nioBuffer(i, BlockSize))
    }
  }

  test("nioBuffers 1") {
    val value = new Array[Byte](Capacity)
    random.nextBytes(value)
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(value))

    val nioBuffers = wrappedBuf.nioBuffers()
    var length = 0
    for (b <- nioBuffers) {
      length = length + b.remaining
    }

    val nioBuffer = ByteBuffer.allocate(length)
    for (b <- nioBuffers) {
      nioBuffer.put(b)
    }
    nioBuffer.flip()

    assertEquals(ByteBuffer.wrap(value), nioBuffer)
  }

  test("nioBuffers 2") {
    val value = new Array[Byte](Capacity)
    random.nextBytes(value)
    val wrappedBuf = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(value))

    0.until(Capacity - BlockSize + 1, BlockSize).foreach { i =>
      val nioBuffers = wrappedBuf.nioBuffers(i, BlockSize)
      val nioBuffer = ByteBuffer.allocate(BlockSize)
      for (b <- nioBuffers) {
        nioBuffer.put(b)
      }
      nioBuffer.flip()

      assertEquals(ByteBuffer.wrap(value, i, BlockSize), nioBuffer)
    }
  }

  test("skipBytes") {
    buffer.setIndex(Capacity / 4, Capacity / 2)

    buffer.skipBytes(Capacity / 4)
    assertEquals(Capacity / 4 * 2, buffer.readerIndex)

    intercept[IndexOutOfBoundsException] {
      buffer.skipBytes(Capacity / 4 + 1)
    }

    // Should remain unchanged.
    assertEquals(Capacity / 4 * 2, buffer.readerIndex)
  }

  test("hashCode") {
    val a = Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5)
    val b = Array[Byte](6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9)
    val cbA = Unpooled.buffer(15)
    val cbB = Unpooled.directBuffer(15)
    cbA.writeBytes(a)
    cbB.writeBytes(b)

    val set = new HashSet[ByteBuf]()
    set.add(cbA)
    set.add(cbB)

    assertEquals(2, set.size())
    assert(set.contains(cbA.copy()))
    assert(set.contains(cbB.copy()))

    val wrappedBufA = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(a))
    assert(set.remove(wrappedBufA))
    assert(!set.contains(cbA))
    assertEquals(1, set.size())

    val wrappedBufB = ByteBufConversion.bufAsByteBuf(Buf.ByteArray.Owned(b))
    assert(set.remove(wrappedBufB))
    assert(!set.contains(cbB))
    assertEquals(0, set.size())
  }

  def testConstructAndExtract(name: String, buf: Buf): Unit = {
    test(s"construct and extract BufAsByteBuf.bufAsByteBuf from $name") {
      val bytes = Buf.ByteArray.Owned.extract(buf)
      val bb = ByteBufConversion.bufAsByteBuf(buf)
      assert(bb.readableBytes == bytes.length)
      val wrappedBytes = new Array[Byte](buf.length)
      bb.getBytes(0, wrappedBytes)
      assert(wrappedBytes.toSeq == bytes.toSeq)
    }
  }

  testConstructAndExtract("empty Buf", Buf.Empty)

  testConstructAndExtract(
    "Buf.ByteArray",
    Buf.ByteArray.Owned(Array[Byte](0, 1, 2, 3, 4))
  )

  testConstructAndExtract(
    "Buf.ByteArray with begin and end",
    Buf.ByteArray.Owned(Array[Byte](0, 1, 2, 3, 4), 3, 4)
  )

  testConstructAndExtract(
    "Buf.ByteBuffer",
    Buf.ByteBuffer.Owned(
      ByteBuffer.wrap(Array[Byte](0, 1, 2, 3, 4))
    )
  )
}
