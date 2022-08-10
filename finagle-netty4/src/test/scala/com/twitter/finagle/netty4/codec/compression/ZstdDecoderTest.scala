package com.twitter.finagle.netty4.codec.compression

import com.github.luben.zstd.Zstd
import com.twitter.finagle.netty4.codec.compression.CompressionTestUtils.rand
import com.twitter.finagle.netty4.codec.compression.zstd.ZstdConstants.DEFAULT_COMPRESSION_LEVEL
import com.twitter.finagle.netty4.codec.compression.zstd.ZstdDecoder
import com.twitter.finagle.netty4.codec.compression.zstd.ZstdStreamingEncoder
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufUtil
import io.netty.buffer.CompositeByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandler
import io.netty.channel.embedded.EmbeddedChannel
import java.util
import org.junit.Assert.assertArrayEquals
import org.scalatest.OneInstancePerTest
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class ZstdDecoderTest
    extends AnyFunSuite
    with ScalaCheckDrivenPropertyChecks
    with OneInstancePerTest {
  protected val verySmallBytes = CompressionTestUtils.verySmallRandBytes()
  protected var wrappedVerySmallBytes: ByteBuf = Unpooled.wrappedBuffer(verySmallBytes)
  protected val smallBytes = CompressionTestUtils.smallRandBytes()
  protected var wrappedSmallBytes: ByteBuf = Unpooled.wrappedBuffer(smallBytes)
  protected val largeBytes = CompressionTestUtils.largeRandBytes()
  protected var wrappedLargeBytes: ByteBuf = Unpooled.wrappedBuffer(largeBytes)
  protected val veryLargeBytes = CompressionTestUtils.veryLargeRandBytes()
  protected var wrappedVeryLargeBytes: ByteBuf = Unpooled.wrappedBuffer(veryLargeBytes)

  protected var channel: EmbeddedChannel = createChannel()
  protected val zstdCompressChannel = createChannel(
    new ZstdStreamingEncoder(DEFAULT_COMPRESSION_LEVEL))

  protected var compressedBytesVerySmall: Array[Byte] = compressStatic(verySmallBytes)
  protected var compressedBytesSmall: Array[Byte] = compressStatic(smallBytes)
  protected var compressedBytesLarge: Array[Byte] = compressStatic(largeBytes)
  protected var compressedBytesVeryLarge: Array[Byte] = compressStatic(veryLargeBytes)

  protected def compressStream(channel: EmbeddedChannel, data: Array[Byte]): ByteBuf = {
    channel.writeOutbound(Unpooled.wrappedBuffer(data));
    readCompressed(channel)
  }

  protected def compressStatic(data: Array[Byte]): Array[Byte] = {
    Zstd.compress(data, DEFAULT_COMPRESSION_LEVEL)
  }

  protected def createChannel(handler: ChannelHandler = new ZstdDecoder()): EmbeddedChannel = {
    new EmbeddedChannel(handler)
  }

  private def makeHeap(compressedData: Array[Byte]): ByteBuf = {
    Unpooled.wrappedBuffer(compressedData)
  }

  private def makeDirect(compressedData: Array[Byte]): ByteBuf = {
    val direct = Unpooled.directBuffer(compressedData.length)
    direct.writeBytes(compressedData)
  }

  def verySmallHeapData: ByteBuf = makeHeap(compressedBytesVerySmall)
  def verySmallDirectData: ByteBuf = makeDirect(compressedBytesVerySmall)

  def smallHeapData: ByteBuf = makeHeap(compressedBytesSmall)
  def smallDirectData: ByteBuf = makeDirect(compressedBytesSmall)

  def largeHeapData: ByteBuf = makeHeap(compressedBytesLarge)
  def largeDirectData: ByteBuf = makeDirect(compressedBytesLarge)

  def veryLargeHeapData: ByteBuf = makeHeap(compressedBytesVeryLarge)
  def veryLargeDirectData: ByteBuf = makeDirect(compressedBytesVeryLarge)

  test("Decompression of very small chunk of Heap Data") {
    testDecompression(wrappedVerySmallBytes, verySmallHeapData)
  }

  test("Decompression of very small chunk of Direct Data") {
    testDecompression(wrappedVerySmallBytes, verySmallDirectData)
  }

  test("Decompression of small chunk of Heap Data") {
    testDecompression(wrappedSmallBytes, smallHeapData)
  }

  test("Decompression of small chunk of Direct Data") {
    testDecompression(wrappedSmallBytes, smallDirectData)
  }

  test("Decompression of large chunk of Heap Data") {
    testDecompression(wrappedLargeBytes, largeHeapData)
  }

  test("Decompression of large chunk of Direct Data") {
    testDecompression(wrappedLargeBytes, largeDirectData)
  }

  test("Decompression of very large chunk of Heap Data") {
    testDecompression(wrappedVeryLargeBytes, veryLargeHeapData)
  }

  test("Decompression of very large chunk of Direct Data") {
    testDecompression(wrappedVeryLargeBytes, veryLargeDirectData)
  }

  test("Decompression of batched flow for small chunk of Heap Data") {
    testDecompressionOfBatchedFlow(wrappedSmallBytes, smallHeapData)
  }

  test("Decompression of batched flow for small chunk of Direct Data") {
    testDecompressionOfBatchedFlow(wrappedSmallBytes, smallDirectData)
  }

  test("Decompression of batched flow for large chunk of Heap Data") {
    testDecompressionOfBatchedFlow(wrappedLargeBytes, largeHeapData)
  }

  test("Decompression of batched flow for large chunk of Direct Data") {
    testDecompressionOfBatchedFlow(wrappedLargeBytes, largeDirectData)
  }

  test("Decompression of batched flow for very large chunk of Heap Data") {
    testDecompressionOfBatchedFlow(wrappedVeryLargeBytes, veryLargeHeapData)
  }

  test("Decompression of batched flow for very large chunk of Direct Data") {
    testDecompressionOfBatchedFlow(wrappedVeryLargeBytes, veryLargeDirectData)
  }

  /* reuse same decompression context */
  test("Decompression of multiple very small chunk of Heap Data") {
    testDecompression(wrappedVerySmallBytes, verySmallHeapData)
    testDecompression(wrappedVerySmallBytes, verySmallHeapData)
  }

  test("Decompression of multiple very small chunks of Direct Data") {
    testDecompression(wrappedVerySmallBytes, verySmallDirectData)
    testDecompression(wrappedVerySmallBytes, verySmallDirectData)
  }

  test("Decompression of multiple small chunk of Heap Data") {
    testDecompression(wrappedSmallBytes, smallHeapData)
    testDecompression(wrappedSmallBytes, smallHeapData)
  }

  test("Decompression of multiple small chunk of Direct Data") {
    testDecompression(wrappedSmallBytes, smallDirectData)
    testDecompression(wrappedSmallBytes, smallDirectData)
  }

  test("Decompression of multiple large chunk of Heap Data") {
    testDecompression(wrappedLargeBytes, largeHeapData)
    testDecompression(wrappedLargeBytes, largeHeapData)
  }

  test("Decompression of multiple large chunk of Direct Data") {
    testDecompression(wrappedLargeBytes, largeDirectData)
    testDecompression(wrappedLargeBytes, largeDirectData)
  }

  test("Decompression of multiple very large chunk of Heap Data") {
    testDecompression(wrappedVeryLargeBytes, veryLargeHeapData)
    testDecompression(wrappedVeryLargeBytes, veryLargeHeapData)
  }

  test("Decompression of multiple very large chunk of Direct Data") {
    testDecompression(wrappedVeryLargeBytes, veryLargeDirectData)
    testDecompression(wrappedVeryLargeBytes, veryLargeDirectData)
  }

  test("Decompression of batched flow for multiple small chunk of Heap Data") {
    testDecompressionOfBatchedFlow(wrappedSmallBytes, smallHeapData)
    testDecompressionOfBatchedFlow(wrappedSmallBytes, smallHeapData)
  }

  test("Decompression of batched flow for multiple small chunk of Direct Data") {
    testDecompressionOfBatchedFlow(wrappedSmallBytes, smallDirectData)
    testDecompressionOfBatchedFlow(wrappedSmallBytes, smallDirectData)
  }

  test("Decompression of batched flow for multiple large chunk of Heap Data") {
    testDecompressionOfBatchedFlow(wrappedLargeBytes, largeHeapData)
    testDecompressionOfBatchedFlow(wrappedLargeBytes, largeHeapData)
  }

  test("Decompression of batched flow for multiple large chunk of Direct Data") {
    testDecompressionOfBatchedFlow(wrappedLargeBytes, largeDirectData)
    testDecompressionOfBatchedFlow(wrappedLargeBytes, largeDirectData)
  }

  test("Decompression of batched flow for multiple very large chunk of Heap Data") {
    testDecompressionOfBatchedFlow(wrappedVeryLargeBytes, veryLargeHeapData)
    testDecompressionOfBatchedFlow(wrappedVeryLargeBytes, veryLargeHeapData)
  }

  test("Decompression of batched flow for multiple very large chunk of Direct Data") {
    testDecompressionOfBatchedFlow(wrappedVeryLargeBytes, veryLargeDirectData)
    testDecompressionOfBatchedFlow(wrappedVeryLargeBytes, veryLargeDirectData)
  }

  /* reuse decompression context and mix buffer types */

  test("Decompression of multiple very small chunk of mixed data, Heap first") {
    testDecompression(wrappedVerySmallBytes, verySmallHeapData)
    testDecompression(wrappedVerySmallBytes, verySmallDirectData)
  }

  test("Decompression of multiple very small chunks of mixed data, Direct first") {
    testDecompression(wrappedVerySmallBytes, verySmallDirectData)
    testDecompression(wrappedVerySmallBytes, verySmallHeapData)
  }

  test("Decompression of multiple small chunk of mixed data, Heap first") {
    testDecompression(wrappedSmallBytes, smallHeapData)
    testDecompression(wrappedSmallBytes, smallDirectData)
  }

  test("Decompression of multiple small chunk of mixed data, Direct first") {
    testDecompression(wrappedSmallBytes, smallDirectData)
    testDecompression(wrappedSmallBytes, smallHeapData)
  }

  test("Decompression of multiple large chunk of mixed data, Heap first") {
    testDecompression(wrappedLargeBytes, largeHeapData)
    testDecompression(wrappedLargeBytes, largeDirectData)
  }

  test("Decompression of multiple large chunk of mixed data, Direct first") {
    testDecompression(wrappedLargeBytes, largeDirectData)
    testDecompression(wrappedLargeBytes, largeHeapData)
  }

  test("Decompression of multiple very large chunk of mixed data, Heap first") {
    testDecompression(wrappedVeryLargeBytes, veryLargeHeapData)
    testDecompression(wrappedVeryLargeBytes, veryLargeDirectData)
  }

  test("Decompression of multiple very large chunk of mixed data, Direct first") {
    testDecompression(wrappedVeryLargeBytes, veryLargeDirectData)
    testDecompression(wrappedVeryLargeBytes, veryLargeHeapData)
  }

  test("Decompression of batched flow for multiple small chunk of mixed data, Heap first") {
    testDecompressionOfBatchedFlow(wrappedSmallBytes, smallHeapData)
    testDecompressionOfBatchedFlow(wrappedSmallBytes, smallDirectData)
  }

  test("Decompression of batched flow for multiple small chunk of mixed data, Direct first") {
    testDecompressionOfBatchedFlow(wrappedSmallBytes, smallDirectData)
    testDecompressionOfBatchedFlow(wrappedSmallBytes, smallHeapData)
  }

  test("Decompression of batched flow for multiple large chunk of mixed data, Heap first") {
    testDecompressionOfBatchedFlow(wrappedLargeBytes, largeHeapData)
    testDecompressionOfBatchedFlow(wrappedLargeBytes, largeDirectData)
  }

  test("Decompression of batched flow for multiple large chunk of mixed data, Direct first") {
    testDecompressionOfBatchedFlow(wrappedLargeBytes, largeDirectData)
    testDecompressionOfBatchedFlow(wrappedLargeBytes, largeHeapData)
  }

  test("Decompression of batched flow for multiple very large chunk of mixed data, Heap first") {
    testDecompressionOfBatchedFlow(wrappedVeryLargeBytes, veryLargeHeapData)
    testDecompressionOfBatchedFlow(wrappedVeryLargeBytes, veryLargeDirectData)
  }

  test("Decompression of batched flow for multiple very large chunk of mixed data, Direct first") {
    testDecompressionOfBatchedFlow(wrappedVeryLargeBytes, veryLargeDirectData)
    testDecompressionOfBatchedFlow(wrappedVeryLargeBytes, veryLargeHeapData)
  }

  test("Decompression of several different sized buffers, passed in micro batches") {
    testDecompressionOfFixedBatchSize(wrappedVerySmallBytes, verySmallHeapData, 1)
    testDecompressionOfFixedBatchSize(wrappedSmallBytes, smallHeapData, 1)
    testDecompressionOfFixedBatchSize(wrappedLargeBytes, largeHeapData, 1)
  }

  test("Decompression of multiple frames, fed in with spill over across frames") {
    // This probably looks weird. This tests specifically if somehow we get some bytes bleed over
    // from one complete frame to another frame; the decompressor shouldn't throw those away
    // and correctly continue to process the frame transparently to the caller. Internally
    // we try to reset state/context after each frame and this verifies that we can do that
    // non destructively even in odd circumstances
    val doubleLarge = compressedBytesLarge ++ compressedBytesLarge
    val chunkSize = firstNonDivisingInteger(doubleLarge.length)
    testDecompressionOfFixedBatchSize(
      Unpooled.wrappedBuffer(largeBytes ++ largeBytes),
      makeHeap(doubleLarge),
      chunkSize)
  }

  test("Decompression of several different sized buffers through a stream") {
    val zstdCompressChannel = createChannel(new ZstdStreamingEncoder(DEFAULT_COMPRESSION_LEVEL))
    val verySmallData = compressStream(zstdCompressChannel, verySmallBytes)
    val smallData = compressStream(zstdCompressChannel, smallBytes)
    val largeData = compressStream(zstdCompressChannel, largeBytes)
    val veryLargeData = compressStream(zstdCompressChannel, veryLargeBytes)
    testDecompression(wrappedVerySmallBytes, verySmallData)
    testDecompression(wrappedSmallBytes, smallData)
    testDecompression(wrappedLargeBytes, largeData)
    testDecompression(wrappedVeryLargeBytes, veryLargeData)
  }

  test("Decompression of lots of buffers through a stream") {
    val zstdCompressChannel = createChannel(new ZstdStreamingEncoder(DEFAULT_COMPRESSION_LEVEL))
    for { _ <- 0 to 1000 } {
      testDecompression(wrappedLargeBytes, compressStream(zstdCompressChannel, largeBytes))
    }
  }

  @throws[Exception]
  protected def testDecompression(
    expected: ByteBuf,
    data: ByteBuf,
    c: EmbeddedChannel = channel
  ): Unit = {
    assert(c.writeInbound(data))
    val decompressed = readDecompressed(c)
    val exp = convertToArray(expected)
    val act = convertToArray(decompressed)
    assertArrayEquals(exp, act)
    decompressed.release
  }

  protected def testDecompressionOfBatchedFlow(expected: ByteBuf, data: ByteBuf): Unit = {

    val compressedLength = data.readableBytes
    var written = 0
    var length = rand.nextInt(100)
    while ({
      written + length < compressedLength
    }) {
      val compressedBuf = data.retainedSlice(written, length)
      channel.writeInbound(compressedBuf)
      written += length
      length = rand.nextInt(100)
    }
    val compressedBuf = data.slice(written, compressedLength - written)
    assert(channel.writeInbound(compressedBuf.retain))
    val decompressedBuf = readDecompressed(channel)
    val expectedArr = convertToArray(expected)
    val decompressedArr = convertToArray(decompressedBuf)
    assertArrayEquals(expectedArr, decompressedArr)
    decompressedBuf.release
    data.release
  }

  protected def testDecompressionOfFixedBatchSize(
    expected: ByteBuf,
    data: ByteBuf,
    batchSize: Int
  ): Unit = {

    val compressedLength = data.readableBytes
    var written = 0
    try {
      while ({
        written + batchSize < compressedLength
      }) {
        val compressedBuf = data.retainedSlice(written, batchSize)
        channel.writeInbound(compressedBuf)
        written += batchSize
      }
      val compressedBuf = data.slice(written, compressedLength - written)
      assert(channel.writeInbound(compressedBuf.retain))
      val decompressedBuf = readDecompressed(channel)
      val expectedArr = convertToArray(expected)
      val decompressedArr = convertToArray(decompressedBuf)
      assertArrayEquals(expectedArr, decompressedArr)
      decompressedBuf.release
      data.release
    } catch {
      case e: Exception =>
        throw e
    }
  }

  private def readCompressed(channel: EmbeddedChannel): ByteBuf = {
    val compressed = Unpooled.compositeBuffer
    var msg: ByteBuf = null
    while ({
      msg = channel.readOutbound.asInstanceOf[ByteBuf]; msg != null
    }) compressed.addComponent(true, msg)
    compressed
  }

  private def readDecompressed(channel: EmbeddedChannel): ByteBuf = {
    val decompressed = Unpooled.compositeBuffer
    var msg: ByteBuf = null
    while ({
      msg = channel.readInbound.asInstanceOf[ByteBuf]; msg != null
    }) decompressed.addComponent(true, msg)
    decompressed
  }

  private def convertToArray(buf: ByteBuf): Array[Byte] = {
    if (buf.hasArray) buf.array
    else if (buf.isInstanceOf[CompositeByteBuf]) {
      val cBuf = buf.asInstanceOf[CompositeByteBuf]
      var totalSize = 0
      for (i <- 0 until cBuf.numComponents()) {
        val c = cBuf.component(i)
        totalSize += c.readableBytes
      }
      val dst = new Array[Byte](totalSize)
      var offset = 0
      cBuf
        .nioBuffers().foreach(b => {
          val offsetDelta = b.remaining
          b.get(dst, offset, b.remaining)
          offset += offsetDelta
        })

      util.Arrays.copyOf(dst, offset)
    } else ByteBufUtil.getBytes(buf, buf.readerIndex, buf.readableBytes)
  }

  private def firstNonDivisingInteger(target: Int): Int = {
    for { i <- 2 until target } {
      if (target % i != 0) {
        return i
      }
    }
    throw new Exception(
      s"Impossible math: $target was divisible by every number through ${target - 1}")
  }
}
