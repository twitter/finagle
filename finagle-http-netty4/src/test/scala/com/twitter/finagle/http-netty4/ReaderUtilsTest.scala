package com.twitter.finagle.http4

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.time._
import com.twitter.finagle.Status
import com.twitter.finagle.transport.{Transport, QueueTransport}
import com.twitter.io.Reader.ReaderDiscarded
import com.twitter.io.{Reader, Buf}
import com.twitter.util.{Time, Await, Future}
import io.netty.buffer.Unpooled
import io.netty.handler.codec.http.DefaultHttpContent
import io.netty.handler.codec.{http => NettyHttp}
import java.net.SocketAddress
import java.nio.charset.StandardCharsets.UTF_8
import java.security.cert.Certificate
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReaderUtilsTest extends FunSuite {
  import ReaderUtils._

  test("readChunk: returned bufs have same content as http chunk") {
    val input = Array[Byte](1,2,3)
    val output = readChunk(new DefaultHttpContent(Unpooled.wrappedBuffer(input)))
    assert(Await.result(output, 2.seconds) == Some(Buf.ByteArray.Owned(input)))
  }

  test("readChunk: interprets last http chunk as Future.None") {
    val output = readChunk(new NettyHttp.DefaultLastHttpContent)
    assert(output == Future.None)
  }

  test("chunkOfBuf: wraps buf in http chunk") {
    val input = Array[Byte](1,2,3)
    val chunk = chunkOfBuf(Buf.ByteArray.Owned(input))

    val output = new Array[Byte](chunk.content.readableBytes)
    chunk.content.readBytes(output)
    assert(input.toSeq == output.toSeq)
  }

  test("streamChunks: streams http chunks into transport") {
    val rw = Reader.writable()

    val (write, read) = (new AsyncQueue[Any], new AsyncQueue[Any])
    val tr = new QueueTransport[Any,Any](write, read)

    rw.write(Buf.Utf8("msg1"))

    val chunk1F = write.poll()

    streamChunks(tr, rw)

    val chunk =
      Await.result(chunk1F, 2.seconds).asInstanceOf[NettyHttp.HttpContent]

    assert(chunk.content.toString(UTF_8) == "msg1")


    val chunk2F = write.poll()

    rw.write(Buf.Utf8("msg2"))

    val chunk2 = Await.result(chunk2F, 2.seconds).asInstanceOf[NettyHttp.HttpContent]
    assert(chunk2.content.toString(UTF_8) == "msg2")

    Await.ready(rw.close(), 2.seconds)


    val lastChunk = Await.result(write.poll(), 2.seconds).asInstanceOf[NettyHttp.HttpContent]

    assert(lastChunk.isInstanceOf[NettyHttp.LastHttpContent])
  }


  val failingT = new Transport[Any, Any] {
    def write(req: Any): Future[Unit] = Future.exception(new Exception("nop"))

    def remoteAddress: SocketAddress = ???

    def peerCertificate: Option[Certificate] = ???

    def localAddress: SocketAddress = ???

    def status: Status = ???

    def read(): Future[Any] = ???

    val onClose: Future[Throwable] = Future.exception(new Exception)

    def close(deadline: Time): Future[Unit] = ???
  }

  test("streamChunks: discard reader on transport write failure") {
    val rw = Reader.writable()
    rw.write(Buf.Utf8("msg"))

    streamChunks(failingT, rw)

    intercept[ReaderDiscarded] { Await.result(rw.read(1), 2.seconds) }
  }
}
