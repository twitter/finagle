package com.twitter.finagle.memcached.protocol.text

import com.twitter.io.Buf
import org.jboss.netty.channel.{ChannelHandlerContext, Channel}
import org.junit.runner.RunWith
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mockito.MockitoSugar

@RunWith(classOf[JUnitRunner])
class EncoderTest extends FunSuite with MockitoSugar {

  test("not alter the tokens it is serializing") {
    val channel = mock[Channel]
    val context = mock[ChannelHandlerContext]
    val addr = mock[java.net.SocketAddress]
    when(context.getChannel) thenReturn channel
    when(channel.getLocalAddress) thenReturn addr
    val encoder = new Encoder

    def encode(x: AnyRef) = {
      val encoded = encoder.encode(context, channel, x)
      Buf.Utf8.unapply(encoded).get
    }

    def encodeIsPure(x: AnyRef) = {
      val buf1 = encode(x)
      val buf2 = encode(x)
      assert(buf1 == buf2)
    }

    info("tokens")
    encodeIsPure(Tokens(Seq(Buf.Utf8("tok"))))

    info("tokens with data")
    encodeIsPure(TokensWithData(Seq(Buf.Utf8("foo")), Buf.Utf8("bar"), None))

    info("tokens with data and cas")
    encodeIsPure(TokensWithData(Seq(Buf.Utf8("foo")), Buf.Utf8("baz"), Some(Buf.Utf8("quux"))))

    info("stat lines")
    encodeIsPure(
      StatLines(
        Seq(
          Tokens(Seq(Buf.Utf8("tok1"))),
          Tokens(Seq(Buf.Utf8("tok2")))
        )
      )
    )

    info("value lines")
    encodeIsPure(
      ValueLines(Seq(TokensWithData(Seq(Buf.Utf8("foo")), Buf.Utf8("bar"), Some(Buf.Utf8("quux")))))
    )
  }
}

