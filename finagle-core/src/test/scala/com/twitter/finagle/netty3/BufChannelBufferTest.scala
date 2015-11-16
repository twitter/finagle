package com.twitter.finagle.netty3

import com.twitter.io.Buf
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.{ByteBuffer, ByteOrder, ReadOnlyBufferException}
import java.util._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.util.CharsetUtil
import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class BufChannelBufferTest extends FunSuite with BeforeAndAfter {
  private[this] val CAPACITY = 4096 // Must be even
  private[this] val BLOCK_SIZE = 128
  private[this] var seed: Long = 0
  private[this] var random: Random = null
  private[this] var buffer: ChannelBuffer = null

  before {
    buffer = BufChannelBufferFactory(ByteOrder.BIG_ENDIAN).getBuffer(CAPACITY)
    seed = System.currentTimeMillis()
    random = new Random(seed)
  }

  after {
    buffer = null
  }

  test("initial state") {
    assertEquals(CAPACITY, buffer.capacity())
    assertEquals(0, buffer.readerIndex())
  }

  test("reader index boundary check 1") {
    try {
      buffer.writerIndex(0)
    } catch { case e: IndexOutOfBoundsException =>
      fail()
    }
    intercept[IndexOutOfBoundsException] {
      buffer.readerIndex(-1)
    }
  }

  test("reader index boundary check 2") {
    try {
      buffer.writerIndex(buffer.capacity())
    } catch { case e: IndexOutOfBoundsException =>
      fail()
    }
    intercept[IndexOutOfBoundsException] {
      buffer.readerIndex(buffer.capacity() + 1)
    }
  }

  test("reader index boundary check 3") {
    try {
      buffer.writerIndex(CAPACITY / 2)
    } catch { case e: IndexOutOfBoundsException =>
      fail()
    }
    intercept[IndexOutOfBoundsException] {
      buffer.readerIndex(CAPACITY * 3 / 2)
    }
  }

  test("reader index boundary check 4") {
    buffer.writerIndex(0)
    buffer.readerIndex(0)
    buffer.writerIndex(buffer.capacity())
    buffer.readerIndex(buffer.capacity())
  }

  test("writer index boundary check 1") {
    intercept[IndexOutOfBoundsException] {
      buffer.writerIndex(-1)
    }
  }

  test("writer index boundary check 2") {
    try {
      buffer.writerIndex(CAPACITY)
      buffer.readerIndex(CAPACITY)
    } catch { case e: IndexOutOfBoundsException =>
      fail()
    }
    intercept[IndexOutOfBoundsException] {
      buffer.writerIndex(buffer.capacity() + 1)
    }
  }

  test("writer index boundary check 3") {
    try {
      buffer.writerIndex(CAPACITY)
      buffer.readerIndex(CAPACITY / 2)
    } catch { case e: IndexOutOfBoundsException =>
      fail()
    }
    intercept[IndexOutOfBoundsException] {
      buffer.writerIndex(CAPACITY / 4)
    }
  }

  test("writer index boundary check 4") {
    buffer.writerIndex(0)
    buffer.readerIndex(0)
    buffer.writerIndex(CAPACITY)
  }

  test("getByte boundary check 1") {
    intercept[IndexOutOfBoundsException] {
      buffer.getByte(-1)
    }
  }

  test("getByte boundary check 2") {
    intercept[IndexOutOfBoundsException] {
      buffer.getByte(buffer.capacity())
    }
  }

  test("getShort boundary check 1") {
    intercept[IndexOutOfBoundsException] {
      buffer.getShort(-1)
    }
  }

  test("getShort boundary check 2") {
    intercept[IndexOutOfBoundsException] {
      buffer.getShort(buffer.capacity() - 1)
    }
  }

  test("getMedium boundary check 1") {
    intercept[IndexOutOfBoundsException] {
      buffer.getMedium(-1)
    }
  }

  test("getMedium boundary check 2") {
    intercept[IndexOutOfBoundsException] {
      buffer.getMedium(buffer.capacity() - 2)
    }
  }

  test("getInt boundary check 1") {
    intercept[IndexOutOfBoundsException] {
      buffer.getInt(-1)
    }
  }

  test("getInt boundary check 2") {
    intercept[IndexOutOfBoundsException] {
      buffer.getInt(buffer.capacity() - 3)
    }
  }

  test("getLong boundary check 1") {
    intercept[IndexOutOfBoundsException] {
      buffer.getLong(-1)
    }
  }

  test("getLong boundary check 2") {
    intercept[IndexOutOfBoundsException] {
      buffer.getLong(buffer.capacity() - 7)
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
    val bcb = new BufChannelBuffer(Buf.ByteArray(1,2,3,4))
    intercept[IndexOutOfBoundsException] {
      bcb.getBytes(0, dst, -1, 4)
    }

    // No partial copy is expected.
    assert(0 == dst(0))
    assert(0 == dst(1))
    assert(0 == dst(2))
    assert(0 == dst(3))
  }

  test("getByteArray boundary check 4") {
    val dst = new Array[Byte](4)
    val bcb = new BufChannelBuffer(Buf.ByteArray(1,2,3,4))
    intercept[IndexOutOfBoundsException] {
      bcb.getBytes(0, dst, 1, 4)
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
      buffer.copy(0, buffer.capacity() + 1)
    }
  }

  test("copy boundary check 3") {
    intercept[IndexOutOfBoundsException] {
      buffer.copy(buffer.capacity() + 1, 0)
    }
  }

  test("copy boundary check 4") {
    intercept[IndexOutOfBoundsException] {
      buffer.copy(buffer.capacity(), 1)
    }
  }

  test("setIndex boundary check 1") {
    intercept[IndexOutOfBoundsException] {
      buffer.setIndex(-1, CAPACITY)
    }
  }

  test("setIndex boundary check 2") {
    intercept[IndexOutOfBoundsException] {
      buffer.setIndex(CAPACITY / 2, CAPACITY / 4)
    }
  }

  test("setIndex boundary check 3") {
    intercept[IndexOutOfBoundsException] {
      buffer.setIndex(0, CAPACITY + 1)
    }
  }

  test("getBytes ByteBuffer state") {
    val dst = ByteBuffer.allocate(4)
    dst.position(1)
    dst.limit(3)

    val bcb = new BufChannelBuffer(Buf.ByteArray(1,2,3,4))
    bcb.getBytes(1, dst)

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

    val bcb = new BufChannelBuffer(Buf.ByteArray(1,2,3,4))
    bcb.getBytes(1, dst)

    assert(3 == dst.position())
    assert(3 == dst.limit())

    dst.clear()
    assert(0 == dst.get(0))
    assert(2 == dst.get(1))
    assert(3 == dst.get(2))
    assert(0 == dst.get(3))
  }


  test("random byte access") {
    val buf = Buf.ByteArray.Owned(0.until(CAPACITY).map(_ =>
      random.nextInt().asInstanceOf[Byte]).toArray)
    val bcb = new BufChannelBuffer(buf)

    random.setSeed(seed)
    0.until(CAPACITY) foreach { i =>
      val value = random.nextInt().asInstanceOf[Byte]
      assert(value == bcb.getByte(i))
    }
  }

  test("test random unsigned byte access") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    0.until(CAPACITY) foreach { i =>
      val value = random.nextInt().asInstanceOf[Byte]
      wrapped.setByte(i, value)
    }
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))

    random.setSeed(seed)
    0.until(CAPACITY) foreach { i =>
      val value = random.nextInt() & 0xFF
      assert(value == bcb.getUnsignedByte(i))
    }
  }

  test("test random short access") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    0.until(CAPACITY, 2) foreach { i =>
      val value = random.nextInt().asInstanceOf[Short]
      wrapped.setShort(i, value)
    }
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))

    random.setSeed(seed)
    0.until(CAPACITY, 2) foreach { i =>
      val value = random.nextInt().asInstanceOf[Short]
      assert(value == bcb.getShort(i))
    }
  }

  test("test random unsigned short access") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    0.until(CAPACITY, 2) foreach { i =>
      val value = random.nextInt().asInstanceOf[Short]
      wrapped.setShort(i, value)
    }
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))

    random.setSeed(seed)
    0.until(CAPACITY, 2) foreach { i =>
      val value = random.nextInt() & 0xFFFF
      assert(value == bcb.getUnsignedShort(i))
    }
  }

  test("test random medium access") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    0.until(CAPACITY - 2, 3) foreach { i =>
      val value = random.nextInt()
      wrapped.setMedium(i, value)
    }
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))

    random.setSeed(seed)
    0.until(CAPACITY - 2, 3) foreach { i =>
      val value = random.nextInt() << 8 >> 8
      assert(value == bcb.getMedium(i))
    }
  }

  test("test random unsigned medium access") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    0.until(CAPACITY - 2, 3) foreach { i =>
      val value = random.nextInt()
      wrapped.setMedium(i, value)
    }
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))

    random.setSeed(seed)
    0.until(CAPACITY - 2, 3) foreach { i =>
      val value = random.nextInt() & 0x00FFFFFF
      assert(value == bcb.getUnsignedMedium(i))
    }
  }

  test("test random int access") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    0.until(CAPACITY - 3, 4) foreach { i =>
      val value = random.nextInt()
      wrapped.setInt(i, value)
    }
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))

    random.setSeed(seed)
    0.until(CAPACITY - 3, 4) foreach { i =>
      val value = random.nextInt()
      assert(value == bcb.getInt(i))
    }
  }

  test("test random unsigned int access") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    0.until(CAPACITY - 3, 4) foreach { i =>
      val value = random.nextInt()
      wrapped.setInt(i, value)
    }
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))

    random.setSeed(seed)
    0.until(CAPACITY - 3, 4) foreach { i =>
      val value = random.nextInt() & 0xFFFFFFFFL
      assert(value == bcb.getUnsignedInt(i))
    }
  }

  test("test random long access") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    0.until(CAPACITY - 7, 8) foreach { i =>
      val value = random.nextLong()
      wrapped.setLong(i, value)
    }
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))

    random.setSeed(seed)
    0.until(CAPACITY - 7, 8) foreach { i =>
      val value = random.nextLong()
      assert(value == bcb.getLong(i))
    }
  }

  test("setZero") {
    intercept[ReadOnlyBufferException] {
      buffer.setZero(0, 1)
    }
  }

  test("sequential byte access") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    wrapped.writerIndex(0)
    0.until(CAPACITY) foreach { i =>
      val value = random.nextInt().asInstanceOf[Byte]
      wrapped.writeByte(value)
    }
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))
    assert(0 == bcb.readerIndex())

    random.setSeed(seed)
    0.until(CAPACITY) foreach { i =>
      val value = random.nextInt().asInstanceOf[Byte]
      assert(i == bcb.readerIndex())
      assert(bcb.readable())
      assert(value == bcb.readByte())
    }

    assert(bcb.capacity() == bcb.readerIndex())
    assert(!bcb.readable())
  }

  test("sequential unsigned byte access") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    wrapped.writerIndex(0)
    0.until(CAPACITY) foreach { i =>
      val value = random.nextInt().asInstanceOf[Byte]
      wrapped.writeByte(value)
    }
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))
    assert(0 == bcb.readerIndex())

    random.setSeed(seed)
    0.until(CAPACITY) foreach { i =>
      val value = random.nextInt() & 0xFF
      assert(i == bcb.readerIndex())
      assert(bcb.readable())
      assert(value == bcb.readUnsignedByte())
    }

    assert(bcb.capacity() == bcb.readerIndex())
    assert(!bcb.readable())
  }

  test("sequential short access") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    wrapped.writerIndex(0)
    0.until(CAPACITY, 2) foreach { i =>
      val value = random.nextInt().asInstanceOf[Short]
      wrapped.writeShort(value)
    }
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))
    assert(0 == bcb.readerIndex())

    random.setSeed(seed)
    0.until(CAPACITY, 2) foreach { i =>
      val value = random.nextInt().asInstanceOf[Short]
      assert(i == bcb.readerIndex())
      assert(bcb.readable())
      assert(value == bcb.readShort())
    }

    assert(bcb.capacity() == bcb.readerIndex())
    assert(!bcb.readable())
  }

  test("sequential unsigned short access") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    wrapped.writerIndex(0)
    0.until(CAPACITY, 2) foreach { i =>
      val value = random.nextInt().asInstanceOf[Short]
      wrapped.writeShort(value)
    }
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))
    assert(0 == bcb.readerIndex())

    random.setSeed(seed)
    0.until(CAPACITY, 2) foreach { i =>
      val value = random.nextInt() & 0xFFFF
      assert(i == bcb.readerIndex())
      assert(bcb.readable())
      assert(value == bcb.readUnsignedShort())
    }

    assert(bcb.capacity() == bcb.readerIndex())
    assert(!bcb.readable())
  }

  test("sequential medium access") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    wrapped.writerIndex(0)
    val limit = CAPACITY / 3 * 3
    0.until(limit, 3) foreach { i =>
      val value = random.nextInt()
      wrapped.writeMedium(value)
    }
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))
    assert(0 == bcb.readerIndex())

    random.setSeed(seed)
    0.until(limit, 3) foreach { i =>
      val value = random.nextInt() << 8 >> 8
      assert(i == bcb.readerIndex())
      assert(bcb.readable())
      assert(value == bcb.readMedium())
    }

    assert(limit == bcb.readerIndex())
  }

  test("sequential unsigned medium access") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    wrapped.writerIndex(0)
    val limit = CAPACITY / 3 * 3
    0.until(limit, 3) foreach { i =>
      val value = random.nextInt()
      wrapped.writeMedium(value)
    }
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))
    assert(0 == bcb.readerIndex())

    random.setSeed(seed)
    0.until(limit, 3) foreach { i =>
      val value = random.nextInt() & 0x00FFFFFF
      assert(i == bcb.readerIndex())
      assert(bcb.readable())
      assert(value == bcb.readUnsignedMedium())
    }

    assert(limit == bcb.readerIndex())
  }

  test("sequential int access") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    wrapped.writerIndex(0)
    0.until(CAPACITY, 4) foreach { i =>
      val value = random.nextInt()
      wrapped.writeInt(value)
    }
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))
    assert(0 == bcb.readerIndex())

    random.setSeed(seed)
    0.until(CAPACITY, 4) foreach { i =>
      val value = random.nextInt()
      assert(i == bcb.readerIndex())
      assert(bcb.readable())
      assert(value == bcb.readInt())
    }

    assert(CAPACITY == bcb.readerIndex())
  }

  test("sequential unsigned int access") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    wrapped.writerIndex(0)
    0.until(CAPACITY, 4) foreach { i =>
      val value = random.nextInt()
      wrapped.writeInt(value)
    }
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))
    assert(0 == bcb.readerIndex())

    random.setSeed(seed)
    0.until(CAPACITY, 4) foreach { i =>
      val value = random.nextInt() & 0xFFFFFFFFL
      assert(i == bcb.readerIndex())
      assert(bcb.readable())
      assert(value == bcb.readUnsignedInt())
    }

    assert(CAPACITY == bcb.readerIndex())
  }

  test("sequential long access") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    wrapped.writerIndex(0)
    0.until(CAPACITY, 8) foreach { i =>
      val value = random.nextLong()
      wrapped.writeLong(value)
    }
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))
    assert(0 == bcb.readerIndex())

    random.setSeed(seed)
    0.until(CAPACITY, 8) foreach { i =>
      val value = random.nextLong()
      assert(i == bcb.readerIndex())
      assert(bcb.readable())
      assert(value == bcb.readLong())
    }

    assert(CAPACITY == bcb.readerIndex())
  }

  test("byte array transfer") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    val value = new Array[Byte](BLOCK_SIZE * 2)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(value)
      wrapped.setBytes(i, value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE)
    }
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))

    random.setSeed(seed)
    val expectedValue = new Array[Byte](BLOCK_SIZE * 2)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(expectedValue)
      val valueOffset = random.nextInt(BLOCK_SIZE)
      bcb.getBytes(i, value, valueOffset, BLOCK_SIZE)
      valueOffset.until(valueOffset + BLOCK_SIZE) foreach { j =>
        assertEquals(expectedValue(j), value(j))
      }
    }
  }

  test("random byte array transfer 1") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    val value = new Array[Byte](BLOCK_SIZE)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(value)
      wrapped.setBytes(i, value)
    }
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))

    random.setSeed(seed)
    val expectedValueContent = new Array[Byte](BLOCK_SIZE)
    val expectedValue = ChannelBuffers.wrappedBuffer(expectedValueContent)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(expectedValueContent)
      bcb.getBytes(i, value)
      0.until(BLOCK_SIZE) foreach { j =>
        assertEquals(expectedValue.getByte(j), value(j))
      }
    }
  }

  test("random byte array transfer 2") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    val value = new Array[Byte](BLOCK_SIZE * 2)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(value)
      wrapped.setBytes(i, value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE)
    }
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))

    random.setSeed(seed)
    val expectedValueContent = new Array[Byte](BLOCK_SIZE * 2)
    val expectedValue = ChannelBuffers.wrappedBuffer(expectedValueContent)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(expectedValueContent)
      val valueOffset = random.nextInt(BLOCK_SIZE)
      bcb.getBytes(i, value, valueOffset, BLOCK_SIZE)
      valueOffset.until(valueOffset + BLOCK_SIZE) foreach { j =>
        assertEquals(expectedValue.getByte(j), value(j))
      }
    }
  }

  test("random heap buffer transfer 1") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    val valueContent = new Array[Byte](BLOCK_SIZE)
    val value = ChannelBuffers.wrappedBuffer(valueContent)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(valueContent)
      value.setIndex(0, BLOCK_SIZE)
      wrapped.setBytes(i, value)
      assertEquals(BLOCK_SIZE, value.readerIndex())
      assertEquals(BLOCK_SIZE, value.writerIndex())
    }
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))

    random.setSeed(seed)
    val expectedValueContent = new Array[Byte](BLOCK_SIZE)
    val expectedValue = ChannelBuffers.wrappedBuffer(expectedValueContent)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(expectedValueContent)
      value.clear()
      bcb.getBytes(i, value)
      assertEquals(0, value.readerIndex())
      assertEquals(BLOCK_SIZE, value.writerIndex())
      0.until(BLOCK_SIZE) foreach { j =>
        assertEquals(expectedValue.getByte(j), value.getByte(j))
      }
    }
  }

  test("random heap buffer transfer 2") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    val valueContent = new Array[Byte](BLOCK_SIZE * 2)
    val value = ChannelBuffers.wrappedBuffer(valueContent)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(valueContent)
      wrapped.setBytes(i, value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE)
    }
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))

    random.setSeed(seed)
    val expectedValueContent = new Array[Byte](BLOCK_SIZE * 2)
    val expectedValue = ChannelBuffers.wrappedBuffer(expectedValueContent)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(expectedValueContent)
      val valueOffset = random.nextInt(BLOCK_SIZE)
      bcb.getBytes(i, value, valueOffset, BLOCK_SIZE)
      valueOffset.until(valueOffset + BLOCK_SIZE) foreach { j =>
        assertEquals(expectedValue.getByte(j), value.getByte(j))
      }
    }
  }

  test("random direct buffer transfer") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    val tmp = new Array[Byte](BLOCK_SIZE * 2)
    val value = ChannelBuffers.directBuffer(BLOCK_SIZE * 2)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(tmp)
      value.setBytes(0, tmp, 0, value.capacity())
      wrapped.setBytes(i, value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE)
    }
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))

    random.setSeed(seed)
    val expectedValue = ChannelBuffers.directBuffer(BLOCK_SIZE * 2)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(tmp)
      expectedValue.setBytes(0, tmp, 0, expectedValue.capacity())
      val valueOffset = random.nextInt(BLOCK_SIZE)
      bcb.getBytes(i, value, valueOffset, BLOCK_SIZE)
      valueOffset.until(valueOffset + BLOCK_SIZE) foreach { j =>
        assertEquals(expectedValue.getByte(j), value.getByte(j))
      }
    }
  }

  test("random ByteBuffer transfer") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    val value = ByteBuffer.allocate(BLOCK_SIZE * 2)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(value.array())
      value.clear().position(random.nextInt(BLOCK_SIZE))
      value.limit(value.position() + BLOCK_SIZE)
      wrapped.setBytes(i, value)
    }
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))

    random.setSeed(seed)
    val expectedValue = ByteBuffer.allocate(BLOCK_SIZE * 2)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(expectedValue.array())
      val valueOffset = random.nextInt(BLOCK_SIZE)
      value.clear().position(valueOffset).limit(valueOffset + BLOCK_SIZE)
      bcb.getBytes(i, value)
      assertEquals(valueOffset + BLOCK_SIZE, value.position())
      valueOffset.until(valueOffset + BLOCK_SIZE) foreach { j =>
        assertEquals(expectedValue.get(j), value.get(j))
      }
    }
  }

  test("sequential Array[Byte] transfer 1") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))
    val value = new Array[Byte](BLOCK_SIZE)
    wrapped.writerIndex(0)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(value)
      assertEquals(0, bcb.readerIndex())
      wrapped.writeBytes(value)
    }

    random.setSeed(seed)
    val expectedValue = new Array[Byte](BLOCK_SIZE)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(expectedValue)
      assert(i == bcb.readerIndex())
      assertEquals(CAPACITY, bcb.writerIndex())
      bcb.readBytes(value)
      0.until(BLOCK_SIZE) foreach { j =>
        assertEquals(expectedValue(j), value(j))
      }
    }
  }

  test("sequential Array[Byte] transfer 2") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))
    val value = new Array[Byte](BLOCK_SIZE * 2)
    wrapped.writerIndex(0)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(value)
      assertEquals(0, bcb.readerIndex())
      val readerIndex = random.nextInt(BLOCK_SIZE)
      wrapped.writeBytes(value, readerIndex, BLOCK_SIZE)
    }

    random.setSeed(seed)
    val expectedValue = new Array[Byte](BLOCK_SIZE * 2)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(expectedValue)
      val valueOffset = random.nextInt(BLOCK_SIZE)
      assert(i == bcb.readerIndex())
      assertEquals(CAPACITY, bcb.writerIndex())
      bcb.readBytes(value, valueOffset, BLOCK_SIZE)
      valueOffset.until(valueOffset + BLOCK_SIZE) foreach { j =>
        assertEquals(expectedValue(j), value(j))
      }
    }
  }

  test("sequential heap buffer transfer 1") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))
    val valueContent = new Array[Byte](BLOCK_SIZE * 2)
    val value = ChannelBuffers.wrappedBuffer(valueContent)
    wrapped.writerIndex(0)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(valueContent)
      assertEquals(0, bcb.readerIndex())
      wrapped.writeBytes(value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE)
      assertEquals(0, value.readerIndex())
      assertEquals(valueContent.length, value.writerIndex())
    }

    random.setSeed(seed)
    val expectedValueContent = new Array[Byte](BLOCK_SIZE * 2)
    val expectedValue = ChannelBuffers.wrappedBuffer(expectedValueContent)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(expectedValueContent)
      val valueOffset = random.nextInt(BLOCK_SIZE)
      assert(i == bcb.readerIndex())
      assertEquals(CAPACITY, bcb.writerIndex())
      bcb.readBytes(value, valueOffset, BLOCK_SIZE)
      valueOffset.until(valueOffset + BLOCK_SIZE) foreach { j =>
        assertEquals(expectedValue.getByte(j), value.getByte(j))
      }
      assertEquals(0, value.readerIndex())
      assertEquals(valueContent.length, value.writerIndex())
    }
  }

  test("sequential heap buffer transfer 2") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))
    val valueContent = new Array[Byte](BLOCK_SIZE * 2)
    val value = ChannelBuffers.wrappedBuffer(valueContent)
    wrapped.writerIndex(0)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(valueContent)
      assertEquals(0, bcb.readerIndex())
      val readerIndex = random.nextInt(BLOCK_SIZE)
      value.readerIndex(readerIndex)
      value.writerIndex(readerIndex + BLOCK_SIZE)
      wrapped.writeBytes(value)
      assertEquals(readerIndex + BLOCK_SIZE, value.writerIndex())
      assertEquals(value.writerIndex(), value.readerIndex())
    }

    random.setSeed(seed)
    val expectedValueContent = new Array[Byte](BLOCK_SIZE * 2)
    val expectedValue = ChannelBuffers.wrappedBuffer(expectedValueContent)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(expectedValueContent)
      val valueOffset = random.nextInt(BLOCK_SIZE)
      assert(i == bcb.readerIndex())
      assertEquals(CAPACITY, bcb.writerIndex())
      value.readerIndex(valueOffset)
      value.writerIndex(valueOffset)
      bcb.readBytes(value, BLOCK_SIZE)
      valueOffset.until(valueOffset + BLOCK_SIZE) foreach { j =>
        assertEquals(expectedValue.getByte(j), value.getByte(j))
      }
      assertEquals(valueOffset, value.readerIndex())
      assertEquals(valueOffset + BLOCK_SIZE, value.writerIndex())
    }
  }

  test("sequential direct buffer transfer 1") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))
    val valueContent = new Array[Byte](BLOCK_SIZE * 2)
    val value = ChannelBuffers.directBuffer(BLOCK_SIZE * 2)
    wrapped.writerIndex(0)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(valueContent)
      value.setBytes(0, valueContent)
      assertEquals(0, bcb.readerIndex())
      wrapped.writeBytes(value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE)
      assertEquals(0, value.readerIndex())
      assertEquals(0, value.writerIndex())
    }

    random.setSeed(seed)
    val expectedValueContent = new Array[Byte](BLOCK_SIZE * 2)
    val expectedValue = ChannelBuffers.wrappedBuffer(expectedValueContent)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(expectedValueContent)
      val valueOffset = random.nextInt(BLOCK_SIZE)
      value.setBytes(0, valueContent)
      assert(i == bcb.readerIndex())
      assertEquals(CAPACITY, bcb.writerIndex())
      bcb.readBytes(value, valueOffset, BLOCK_SIZE)
      valueOffset.until(valueOffset + BLOCK_SIZE) foreach { j =>
        assertEquals(expectedValue.getByte(j), value.getByte(j))
      }
      assertEquals(0, value.readerIndex())
      assertEquals(0, value.writerIndex())
    }
  }

  test("sequential direct buffer transfer 2") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))
    val valueContent = new Array[Byte](BLOCK_SIZE * 2)
    val value = ChannelBuffers.directBuffer(BLOCK_SIZE * 2)
    wrapped.writerIndex(0)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(valueContent)
      value.setBytes(0, valueContent)
      assertEquals(0, bcb.readerIndex())
      val readerIndex = random.nextInt(BLOCK_SIZE)
      value.readerIndex(0)
      value.writerIndex(readerIndex + BLOCK_SIZE)
      value.readerIndex(readerIndex)
      wrapped.writeBytes(value)
      assertEquals(readerIndex + BLOCK_SIZE, value.writerIndex())
      assertEquals(value.writerIndex(), value.readerIndex())
    }

    random.setSeed(seed)
    val expectedValueContent = new Array[Byte](BLOCK_SIZE * 2)
    val expectedValue = ChannelBuffers.wrappedBuffer(expectedValueContent)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(expectedValueContent)
      value.setBytes(0, valueContent)
      val valueOffset = random.nextInt(BLOCK_SIZE)
      assert(i == bcb.readerIndex())
      assertEquals(CAPACITY, bcb.writerIndex())
      value.readerIndex(valueOffset)
      value.writerIndex(valueOffset)
      bcb.readBytes(value, BLOCK_SIZE)
      valueOffset.until(valueOffset + BLOCK_SIZE) foreach { j =>
        assertEquals(expectedValue.getByte(j), value.getByte(j))
      }
      assertEquals(valueOffset, value.readerIndex())
      assertEquals(valueOffset + BLOCK_SIZE, value.writerIndex())
    }
  }

  test("sequential ByteBuffer-backed heap buffer transfer 1") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))
    val valueContent = new Array[Byte](BLOCK_SIZE * 2)
    val value = ChannelBuffers.wrappedBuffer(ByteBuffer.allocate(BLOCK_SIZE * 2))
    value.writerIndex(0)
    wrapped.writerIndex(0)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(valueContent)
      value.setBytes(0, valueContent)
      assertEquals(0, bcb.readerIndex())
      wrapped.writeBytes(value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE)
      assertEquals(0, value.readerIndex())
      assertEquals(0, value.writerIndex())
    }

    random.setSeed(seed)
    val expectedValueContent = new Array[Byte](BLOCK_SIZE * 2)
    val expectedValue = ChannelBuffers.wrappedBuffer(expectedValueContent)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(expectedValueContent)
      val valueOffset = random.nextInt(BLOCK_SIZE)
      value.setBytes(0, valueContent)
      assert(i == bcb.readerIndex())
      assertEquals(CAPACITY, bcb.writerIndex())
      bcb.readBytes(value, valueOffset, BLOCK_SIZE)
      valueOffset.until(valueOffset + BLOCK_SIZE) foreach { j =>
        assertEquals(expectedValue.getByte(j), value.getByte(j))
      }
      assertEquals(0, value.readerIndex())
      assertEquals(0, value.writerIndex())
    }
  }

  test("sequential ByteBuffer-backed heap buffer transfer 2") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))
    val valueContent = new Array[Byte](BLOCK_SIZE * 2)
    val value = ChannelBuffers.wrappedBuffer(ByteBuffer.allocate(BLOCK_SIZE * 2))
    value.writerIndex(0)
    wrapped.writerIndex(0)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(valueContent)
      value.setBytes(0, valueContent)
      assertEquals(0, bcb.readerIndex())
      val readerIndex = random.nextInt(BLOCK_SIZE)
      value.readerIndex(0)
      value.writerIndex(readerIndex + BLOCK_SIZE)
      value.readerIndex(readerIndex)
      wrapped.writeBytes(value)
      assertEquals(readerIndex + BLOCK_SIZE, value.writerIndex())
      assertEquals(value.writerIndex(), value.readerIndex())
    }

    random.setSeed(seed)
    val expectedValueContent = new Array[Byte](BLOCK_SIZE * 2)
    val expectedValue = ChannelBuffers.wrappedBuffer(expectedValueContent)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(expectedValueContent)
      value.setBytes(0, valueContent)
      val valueOffset = random.nextInt(BLOCK_SIZE)
      assert(i == bcb.readerIndex())
      assertEquals(CAPACITY, bcb.writerIndex())
      value.readerIndex(valueOffset)
      value.writerIndex(valueOffset)
      bcb.readBytes(value, BLOCK_SIZE)
      valueOffset.until(valueOffset + BLOCK_SIZE) foreach { j =>
        assertEquals(expectedValue.getByte(j), value.getByte(j))
      }
      assertEquals(valueOffset, value.readerIndex())
      assertEquals(valueOffset + BLOCK_SIZE, value.writerIndex())
    }
  }

  test("sequential ByteBuffer transfer") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))
    wrapped.writerIndex(0)
    val value = ByteBuffer.allocate(BLOCK_SIZE * 2)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(value.array())
      value.clear().position(random.nextInt(BLOCK_SIZE))
      value.limit(value.position() + BLOCK_SIZE)
      wrapped.writeBytes(value)
    }
    random.setSeed(seed)
    val expectedValue = ByteBuffer.allocate(BLOCK_SIZE * 2)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(expectedValue.array())
      val valueOffset = random.nextInt(BLOCK_SIZE)
      value.clear().position(valueOffset).limit(valueOffset + BLOCK_SIZE)
      bcb.readBytes(value)
      assertEquals(valueOffset + BLOCK_SIZE, value.position())
      valueOffset.until(valueOffset + BLOCK_SIZE) foreach { j =>
        assertEquals(expectedValue.get(j), value.get(j))
      }
    }
  }

  test("sequential copied buffer transfer") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))
    wrapped.writerIndex(0)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      val value = new Array[Byte](BLOCK_SIZE)
      random.nextBytes(value)
      assertEquals(0, bcb.readerIndex())
      wrapped.writeBytes(value)
    }

    random.setSeed(seed)
    val expectedValue = new Array[Byte](BLOCK_SIZE)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(expectedValue)
      assert(i == bcb.readerIndex())
      assertEquals(CAPACITY, bcb.writerIndex())
      val actualValue = bcb.readBytes(BLOCK_SIZE)
      assertEquals(ChannelBuffers.wrappedBuffer(expectedValue), actualValue)

      // Make sure the copy is also read-only
      intercept[ReadOnlyBufferException] {
        actualValue.setByte(0, 0)
      }
    }
  }

  test("sequential slice") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))
    wrapped.writerIndex(0)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      val value = new Array[Byte](BLOCK_SIZE)
      random.nextBytes(value)
      assertEquals(0, bcb.readerIndex())
      wrapped.writeBytes(value)
    }

    random.setSeed(seed)
    val expectedValue = new Array[Byte](BLOCK_SIZE)
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      random.nextBytes(expectedValue)
      assert(i == bcb.readerIndex())
      assertEquals(CAPACITY, bcb.writerIndex())
      val actualValue = bcb.readSlice(BLOCK_SIZE)
      assert(ChannelBuffers.wrappedBuffer(expectedValue) == actualValue)

      // Make sure if it is a read-only buffer.
      intercept[ReadOnlyBufferException] {
        actualValue.setByte(0, 0)
      }
    }
  }

  test("write zero") {
    intercept[IllegalArgumentException] {
      buffer.writeZero(-1)
    }

    buffer.clear()

    intercept[ReadOnlyBufferException] {
      buffer.writeZero(CAPACITY)
    }
  }

  test("discardReadBytes") {
    val bytes = new Array[Byte](CAPACITY)
    val wrapped = ChannelBuffers.wrappedBuffer(bytes)
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))
    wrapped.writerIndex(0)
    0.until(CAPACITY, 4) foreach { i =>
      wrapped.writeInt(i)
    }
    bcb.readByte()
    intercept[ReadOnlyBufferException] {
      bcb.discardReadBytes()
    }
  }

  test("stream transfer 1") {
    val bytes = new Array[Byte](CAPACITY)
    random.nextBytes(bytes)
    val in = new ByteArrayInputStream(bytes, 0, CAPACITY)
    intercept[ReadOnlyBufferException] {
      buffer.setBytes(0, in, CAPACITY)
    }

    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))
    val out = new ByteArrayOutputStream()
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      bcb.getBytes(i, out, BLOCK_SIZE)
    }

    assert(Arrays.equals(bytes, out.toByteArray()))
  }

  test("stream transfer 2") {
    val bytes = new Array[Byte](CAPACITY)
    random.nextBytes(bytes)
    val in = new ByteArrayInputStream(bytes, 0, CAPACITY)
    buffer.clear()
    intercept[ReadOnlyBufferException] {
      buffer.writeBytes(in, CAPACITY)
    }
    assert(buffer.writerIndex() == 0)

    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))
    val out = new ByteArrayOutputStream()
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      assert(i == bcb.readerIndex())
      bcb.readBytes(out, BLOCK_SIZE)
      assertEquals(i + BLOCK_SIZE, bcb.readerIndex())
    }

    assert(Arrays.equals(bytes, out.toByteArray()))
  }

  test("copy") {
    val bytes = new Array[Byte](CAPACITY)
    random.nextBytes(bytes)
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))

    val readerIndex = CAPACITY / 3
    val writerIndex = CAPACITY * 2 / 3
    bcb.setIndex(readerIndex, writerIndex)

    // Make sure all properties are copied.
    val copy = bcb.copy()
    assertEquals(0, copy.readerIndex())
    assertEquals(bcb.readableBytes(), copy.writerIndex())
    assertEquals(bcb.readableBytes(), copy.capacity())
    assertSame(bcb.order(), copy.order())
    0.until(copy.capacity()) foreach { i =>
      assertEquals(bcb.getByte(i + readerIndex), copy.getByte(i))
    }

    // Make sure the copy is read-only
    intercept[ReadOnlyBufferException] {
      copy.setByte(1, 1)
    }
  }

  test("duplicate") {
    val bytes = new Array[Byte](CAPACITY)
    random.nextBytes(bytes)
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))

    val readerIndex = CAPACITY / 3
    val writerIndex = CAPACITY * 2 / 3
    bcb.setIndex(readerIndex, writerIndex)

    // Make sure all properties are copied.
    val duplicate = bcb.duplicate()
    assertEquals(bcb.readerIndex(), duplicate.readerIndex())
    assertEquals(bcb.writerIndex(), duplicate.writerIndex())
    assertEquals(bcb.capacity(), duplicate.capacity())
    assertSame(bcb.order(), duplicate.order())
    0.until(duplicate.capacity()) foreach { i =>
      assertEquals(bcb.getByte(i), duplicate.getByte(i))
    }

    // Make sure the duplicate is read-only
    intercept[ReadOnlyBufferException] {
      duplicate.setByte(1, 1)
    }
  }

  test("slice endianness") {
    assertEquals(buffer.order(), buffer.slice(0, buffer.capacity()).order())
    assertEquals(buffer.order(), buffer.slice(0, buffer.capacity() - 1).order())
    assertEquals(buffer.order(), buffer.slice(1, buffer.capacity() - 1).order())
    assertEquals(buffer.order(), buffer.slice(1, buffer.capacity() - 2).order())
  }

  test("slice index") {
    assertEquals(0, buffer.slice(0, buffer.capacity()).readerIndex())
    assertEquals(0, buffer.slice(0, buffer.capacity() - 1).readerIndex())
    assertEquals(0, buffer.slice(1, buffer.capacity() - 1).readerIndex())
    assertEquals(0, buffer.slice(1, buffer.capacity() - 2).readerIndex())

    assertEquals(buffer.capacity(), buffer.slice(0, buffer.capacity()).writerIndex())
    assertEquals(buffer.capacity() - 1, buffer.slice(0, buffer.capacity() - 1).writerIndex())
    assertEquals(buffer.capacity() - 1, buffer.slice(1, buffer.capacity() - 1).writerIndex())
    assertEquals(buffer.capacity() - 2, buffer.slice(1, buffer.capacity() - 2).writerIndex())
  }

  test("equals") {
    assert(!buffer.equals(null))
    assert(!buffer.equals(new Object()))

    val value = new Array[Byte](32)
    random.nextBytes(value)
    val bytes = Arrays.copyOf(value, CAPACITY)
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))
    bcb.setIndex(0, value.length)

    assertEquals(bcb, ChannelBuffers.wrappedBuffer(ByteOrder.BIG_ENDIAN, value))
    assertEquals(bcb, ChannelBuffers.wrappedBuffer(ByteOrder.LITTLE_ENDIAN, value))

    value(0) = (value(0) + 1).asInstanceOf[Byte]
    assert(!bcb.equals(ChannelBuffers.wrappedBuffer(ByteOrder.BIG_ENDIAN, value)))
    assert(!bcb.equals(ChannelBuffers.wrappedBuffer(ByteOrder.LITTLE_ENDIAN, value)))
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

    val bytes = Arrays.copyOf(value, CAPACITY)
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(bytes))
    bcb.setIndex(0, value.length)

    assertEquals(0, bcb.compareTo(ChannelBuffers.wrappedBuffer(ByteOrder.BIG_ENDIAN, value)))
    assertEquals(0, bcb.compareTo(ChannelBuffers.wrappedBuffer(ByteOrder.LITTLE_ENDIAN, value)))

    value(0) = (value(0) + 1).asInstanceOf[Byte]
    assert(bcb.compareTo(ChannelBuffers.wrappedBuffer(ByteOrder.BIG_ENDIAN, value)) < 0)
    assert(bcb.compareTo(ChannelBuffers.wrappedBuffer(ByteOrder.LITTLE_ENDIAN, value)) < 0)
    value(0) = (value(0) - 2).asInstanceOf[Byte]
    assert(bcb.compareTo(ChannelBuffers.wrappedBuffer(ByteOrder.BIG_ENDIAN, value)) > 0)
    assert(bcb.compareTo(ChannelBuffers.wrappedBuffer(ByteOrder.LITTLE_ENDIAN, value)) > 0)
    value(0) = (value(0) + 1).asInstanceOf[Byte]

    assert(bcb.compareTo(ChannelBuffers.wrappedBuffer(ByteOrder.BIG_ENDIAN, value, 0, 31)) > 0)
    assert(bcb.compareTo(ChannelBuffers.wrappedBuffer(ByteOrder.LITTLE_ENDIAN, value, 0, 31)) > 0)
    assert(bcb.slice(0, 31).compareTo(ChannelBuffers.wrappedBuffer(ByteOrder.BIG_ENDIAN, value)) < 0)
    assert(bcb.slice(0, 31).compareTo(ChannelBuffers.wrappedBuffer(ByteOrder.LITTLE_ENDIAN, value)) < 0)
  }

  test("toString") {
    val msg = "Hello, World!"
    val bcb = new BufChannelBuffer(Buf.Utf8(msg))
    assert("Hello, World!" == bcb.toString(CharsetUtil.UTF_8))
  }

  test("indexOf") {
    val bcb = new BufChannelBuffer(Buf.ByteArray(1,2,3,2,1))

    assertEquals(-1, bcb.indexOf(1, 4, 1: Byte))
    assertEquals(-1, bcb.indexOf(4, 1, 1: Byte))
    assertEquals(1, bcb.indexOf(1, 4, 2: Byte))
    assertEquals(3, bcb.indexOf(4, 1, 2: Byte))
  }

  test("toByteBuffer 1") {
    val value = new Array[Byte](CAPACITY)
    random.nextBytes(value)
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(value))
    assertEquals(ByteBuffer.wrap(value), bcb.toByteBuffer())
  }

  test("toByteBuffer 2") {
    val value = new Array[Byte](CAPACITY)
    random.nextBytes(value)
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(value))
    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      assertEquals(ByteBuffer.wrap(value, i, BLOCK_SIZE), bcb.toByteBuffer(i, BLOCK_SIZE))
    }
  }

  test("toByteBuffer") {
    assertEquals(buffer.order(), buffer.toByteBuffer().order())
  }

  test("toByteBuffers 1") {
    val value = new Array[Byte](CAPACITY)
    random.nextBytes(value)
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(value))

    val nioBuffers = bcb.toByteBuffers()
    var length = 0
    for (b <- nioBuffers) {
      length = length + b.remaining()
    }

    val nioBuffer = ByteBuffer.allocate(length)
    for (b <- nioBuffers) {
      nioBuffer.put(b)
    }
    nioBuffer.flip()

    assertEquals(ByteBuffer.wrap(value), nioBuffer)
  }

  test("toByteBuffers 2") {
    val value = new Array[Byte](CAPACITY)
    random.nextBytes(value)
    val bcb = new BufChannelBuffer(Buf.ByteArray.Owned(value))

    0.until(CAPACITY - BLOCK_SIZE + 1, BLOCK_SIZE) foreach { i =>
      val nioBuffers = bcb.toByteBuffers(i, BLOCK_SIZE)
      val nioBuffer = ByteBuffer.allocate(BLOCK_SIZE)
      for (b <- nioBuffers) {
        nioBuffer.put(b)
      }
      nioBuffer.flip()

      assertEquals(ByteBuffer.wrap(value, i, BLOCK_SIZE), nioBuffer)
    }
  }

  test("skipBytes") {
    buffer.setIndex(CAPACITY / 4, CAPACITY / 2)

    buffer.skipBytes(CAPACITY / 4)
    assertEquals(CAPACITY / 4 * 2, buffer.readerIndex())

    intercept[IndexOutOfBoundsException] {
      buffer.skipBytes(CAPACITY / 4 + 1)
    }

    // Should remain unchanged.
    assertEquals(CAPACITY / 4 * 2, buffer.readerIndex())
  }

  test("hashCode") {
    val a = Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5)
    val b = Array[Byte](6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9)
    val cbA = ChannelBuffers.buffer(15)
    val cbB = ChannelBuffers.directBuffer(15)
    cbA.writeBytes(a)
    cbB.writeBytes(b)

    val set = new HashSet[ChannelBuffer]()
    set.add(cbA)
    set.add(cbB)

    assertEquals(2, set.size())
    assert(set.contains(cbA.copy()))
    assert(set.contains(cbB.copy()))

    val bcbA = new BufChannelBuffer(Buf.ByteArray.Owned(a))
    assert(set.remove(bcbA))
    assert(!set.contains(cbA))
    assertEquals(1, set.size())

    val bcbB = new BufChannelBuffer(Buf.ByteArray.Owned(b))
    assert(set.remove(bcbB))
    assert(!set.contains(cbB))
    assertEquals(0, set.size())
  }

  test("BufChannelBuffer.apply from empty Buf") {
    val cb = BufChannelBuffer(Buf.Empty)
    assert(cb.readableBytes() == 0)
    assert(cb.capacity() == 0)
  }

  test("BufChannelBuffer.apply from Buf.ByteArray.Owned") {
    val arr = Array[Byte](0, 1, 2, 3, 4)
    val baBuf = Buf.ByteArray.Owned(arr, 1, 4)
    val bcb = BufChannelBuffer(baBuf)
    assert(bcb.readableBytes() == 3)
    assert(bcb.capacity() == 3)
    assert(bcb.getByte(0) == 1)
    assert(bcb.getByte(1) == 2)
    assert(bcb.getByte(2) == 3)
  }

  test("BufChannelBuffer.apply from Buf.ByteBuffer.Owned") {
    val arr = Array[Byte](0, 1, 2)
    val baBuf = Buf.ByteBuffer.Owned(ByteBuffer.wrap(arr))
    val bcb = BufChannelBuffer(baBuf)
    assert(bcb.readableBytes() == 3)
    assert(bcb.capacity() == 3)
    assert(bcb.getByte(0) == 0)
    assert(bcb.getByte(1) == 1)
    assert(bcb.getByte(2) == 2)
  }

}
