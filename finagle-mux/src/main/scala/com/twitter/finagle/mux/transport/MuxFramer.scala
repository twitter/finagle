package com.twitter.finagle.mux.transport

import com.twitter.io.{Buf, BufByteWriter, ByteReader}

/**
 * Defines a [[com.twitter.finagle.transport.Transport]] which allows a
 * mux session to be shared between multiple tag streams. The transport splits
 * mux messages into fragments with a size defined by a parameter. Writes are
 * then interleaved to achieve equity and goodput over the entire stream.
 * Fragments are aggregated into complete mux messages when read. The fragment size
 * is negotiated when a mux session is initialized.
 *
 * @see [[com.twitter.finagle.mux.Handshake]] for usage details.
 *
 * @note Our current implementation does not offer any mechanism to resize
 * the window after a session is established. However, it is possible to
 * compose a flow control algorithm over this which can dynamically control
 * the size of `window`.
 */
private[finagle] object MuxFramer {

  /**
   * Defines mux framer keys and values exchanged as part of a
   * mux session header during initialization.
   */
  object Header {
    val KeyBuf: Buf = Buf.Utf8("mux-framer")

    /**
     * Returns a header value with the given frame `size` encoded.
     */
    def encodeFrameSize(size: Int): Buf = {
      require(size > 0)
      val bw = BufByteWriter.fixed(4)
      bw.writeIntBE(size)
      bw.owned()
    }

    /**
     * Extracts frame size from the `buf`.
     */
    def decodeFrameSize(buf: Buf): Int = {
      val br = ByteReader(buf)
      try {
        val size = ByteReader(buf).readIntBE()
        require(size > 0)
        size
      } finally br.close()
    }
  }
}
