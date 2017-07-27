package com.twitter.finagle.netty3.codec

import com.twitter.finagle.Failure
import com.twitter.io.Buf
import java.nio.charset.StandardCharsets.UTF_8
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.embedder.{
  CodecEmbedderException,
  DecoderEmbedder,
  EncoderEmbedder
}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BufCodecTest extends FunSuite {
  test("decode") {
    val de = new DecoderEmbedder[Buf](new BufCodec)

    de.offer(ChannelBuffers.wrappedBuffer("hello".getBytes(UTF_8)))
    assert(de.size() == 1)
    assert(de.poll() == Buf.Utf8("hello"))
    assert(de.size() == 0)

    val exc = intercept[CodecEmbedderException] { de.offer(new Object) }
    assert(exc.getCause.isInstanceOf[Failure])
  }

  test("encode") {
    val ee = new EncoderEmbedder[ChannelBuffer](new BufCodec)
    ee.offer(Buf.Utf8("hello"))
    assert(ee.size == 1)
    val cb = ee.poll()
    assert(cb.toString(UTF_8) == "hello")
    assert(ee.size == 0)

    val cf = ee.getPipeline.getChannel.write(new Object)
    assert(cf.getCause.isInstanceOf[Failure])
  }
}
