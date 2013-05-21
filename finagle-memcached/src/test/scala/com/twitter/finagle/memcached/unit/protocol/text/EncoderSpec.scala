package com.twitter.finagle.memcached.protocol.text

import com.twitter.finagle.memcached.util.ChannelBufferUtils.{
  stringToChannelBuffer,
  channelBufferToString
}
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel.{ChannelHandlerContext, Channel}
import org.specs.mock.Mockito
import org.specs.SpecificationWithJUnit

class EncoderSpec extends SpecificationWithJUnit with Mockito {
  "Encoder" should {
    val channel = smartMock[Channel]
    val context = smartMock[ChannelHandlerContext]
    val addr = smartMock[java.net.SocketAddress]
    context.getChannel returns channel
    channel.getLocalAddress returns addr
    val encoder = new Encoder

    def encode(x: AnyRef) = {
      val encoded = encoder.encode(context, channel, x).asInstanceOf[ChannelBuffer]
      channelBufferToString(encoded)
    }

    def encodeIsPure(x: AnyRef) = {
      val buf1 = encode(x)
      val buf2 = encode(x)

      buf1 mustEqual buf2
    }

    "not alter the tokens it is serializing" in {
      "tokens" in encodeIsPure(Tokens(Seq("tok")))
      "tokens with data" in encodeIsPure(TokensWithData(Seq("foo"), "bar", None))
      "tokens with data and cas" in encodeIsPure(TokensWithData(Seq("foo"), "baz", Some("quux")))
      "stat lines" in encodeIsPure(
        StatLines(
          Seq(
            Tokens(Seq("tok1")),
            Tokens(Seq("tok2"))
          )
        )
      )
      "value lines" in encodeIsPure(
        ValueLines(Seq(TokensWithData(Seq("foo"), "bar", Some("quux"))))
      )
    }
  }
}
