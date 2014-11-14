package com.twitter.finagle.spdy

import org.jboss.netty.handler.codec.spdy.{
  SpdyFrameCodec, SpdyHeaderBlockRawDecoder, SpdyHeaderBlockRawEncoder, SpdyVersion
}

private[finagle] class SpdyRawFrameCodec(version: SpdyVersion, maxChunkSize: Int, maxHeaderSize: Int)
  extends SpdyFrameCodec(version, maxChunkSize,
    new SpdyHeaderBlockRawDecoder(version, maxHeaderSize),
    new SpdyHeaderBlockRawEncoder(version))
