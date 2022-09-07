package com.twitter.finagle.thrift

import com.twitter.io.Buf
import org.apache.thrift.transport.TTransport

/**
 * A TTransport that's backed by a Buf.
 *
 * We assume that the `input` is "owned", and will directly access
 * the underlying byte array if possible for performance. If the
 * `input` is needded intact, please instead copy the `Buf` and then
 * provide it.
 *
 * Note that this class is not threadsafe.  If you wish to use it across
 * threads, you must provide your own synchronization.
 */
final class TBufInputTransport(input: Buf) extends TTransport {
  private[this] var pos: Int = 0

  def isOpen: Boolean = true

  def open: Unit = ()

  def close(): Unit = ()

  def read(buf: Array[Byte], offset: Int, length: Int): Int = {
    val sliced = input.slice(pos, pos + length)
    sliced.write(buf, offset)
    val bytesRead = sliced.length
    pos += bytesRead
    bytesRead
  }

  def write(buf: Array[Byte], offset: Int, length: Int): Unit = {
    throw new UnsupportedOperationException("No writing allowed!");
  }

  /**
   * Depending on the Buf implementation, may entail copying
   */
  override def getBuffer: Array[Byte] = {
    Buf.ByteArray.Owned.extract(input)
  }

  override def getBufferPosition(): Int = {
    pos
  }

  override def getBytesRemainingInBuffer(): Int = {
    input.length - pos
  }

  override def consumeBuffer(len: Int): Unit = {
    pos += len
  }
}
