package com.twitter.finagle.netty4.http

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.http.{Chunk, HeaderMap}
import com.twitter.finagle.transport.{QueueTransport, TransportProxy}
import com.twitter.io.ReaderDiscardedException
import com.twitter.io.{Buf, Pipe}
import com.twitter.util._
import io.netty.buffer.Unpooled
import io.netty.handler.codec.http.{
  DefaultHttpContent,
  DefaultLastHttpContent,
  HttpContent,
  LastHttpContent
}
import java.nio.charset.StandardCharsets.UTF_8
import org.scalatest.OneInstancePerTest
import scala.language.reflectiveCalls
import org.scalatest.funsuite.AnyFunSuite

class Netty4StreamTransportTest extends AnyFunSuite with OneInstancePerTest {
  import Netty4StreamTransport._

  private def await[A](f: Future[A]): A = Await.result(f, 2.seconds)

  val write = new AsyncQueue[Any]
  val read = new AsyncQueue[Any]
  val transport = new QueueTransport(write, read)

  test("streamOut: writes through (no trailers)") {
    val rw = new Pipe[Chunk]
    val out = streamOut(transport, rw, None)

    rw.write(Chunk.fromString("foo"))
    assert(await(write.poll()).asInstanceOf[HttpContent].content.toString(UTF_8) == "foo")

    await(rw.close())

    val lastChunk = await(write.poll()).asInstanceOf[LastHttpContent]
    assert(lastChunk.content == Unpooled.EMPTY_BUFFER)
    assert(out.isDefined)
  }

  test("streamOut: writes through (trailers; empty last content)") {
    val rw = new Pipe[Chunk]
    val out = streamOut(transport, rw, None)

    rw.write(Chunk.fromString("foo"))
    assert(await(write.poll()).asInstanceOf[HttpContent].content.toString(UTF_8) == "foo")

    rw.write(Chunk.last(HeaderMap("foo" -> "bar")))
    val trailers = write.poll()
    assert(!trailers.isDefined)

    await(rw.close())

    val lastChunk = await(trailers).asInstanceOf[LastHttpContent]
    assert(lastChunk.trailingHeaders().get("foo") == "bar")
    assert(lastChunk.content == Unpooled.EMPTY_BUFFER)
    assert(out.isDefined)
  }

  test("streamOut: writes through (trailers; non-empty last content)") {
    val rw = new Pipe[Chunk]
    val out = streamOut(transport, rw, None)

    rw.write(Chunk.fromString("foo"))
    assert(await(write.poll()).asInstanceOf[HttpContent].content.toString(UTF_8) == "foo")

    rw.write(Chunk.last(Buf.Utf8("bar"), HeaderMap("foo" -> "bar")))
    await(rw.close())

    val lastChunk = await(write.poll()).asInstanceOf[LastHttpContent]

    assert(lastChunk.trailingHeaders().get("foo") == "bar")
    assert(lastChunk.content.toString(UTF_8) == "bar")
    assert(out.isDefined)
  }

  test("streamOut: fails when not terminated with trailers") {
    val rw = new Pipe[Chunk]
    val out = streamOut(transport, rw, None)

    rw.write(Chunk.last(HeaderMap("foo" -> "bar")))
    val trailers = write.poll()
    assert(!trailers.isDefined)

    rw.write(Chunk.last(HeaderMap("bar" -> "baz")))
    intercept[IllegalStateException](await(out))
  }

  test("streamOut: fails if more data is written than specified by the content length header") {
    val rw = new Pipe[Chunk]
    val out = streamOut(transport, rw, Some(1))

    await(rw.write(Chunk.fromString("foo")))
    intercept[IllegalStateException](await(out))
    await(rw.onClose)
  }

  test(
    "streamOut: fails if more data is written than specified by the content length header (trailers)") {
    val rw = new Pipe[Chunk]
    val out = streamOut(transport, rw, Some(1))

    await(rw.write(Chunk.last(Buf.Utf8("foo"), HeaderMap.newHeaderMap)))
    intercept[IllegalStateException](await(out))
    await(rw.onClose)
  }

  test("streamOut: fails if less data is written than specified by the content length header") {
    val rw = new Pipe[Chunk]
    val out = streamOut(transport, rw, Some(100))

    await(rw.write(Chunk.fromString("foo")))
    await(rw.close())
    intercept[IllegalStateException](await(out))
    await(rw.onClose)
  }

  test(
    "streamOut: fails if less data is written than specified by the content length header (trailers)") {
    val rw = new Pipe[Chunk]
    val out = streamOut(transport, rw, Some(100))

    await(rw.write(Chunk.last(Buf.Utf8("foo"), HeaderMap.newHeaderMap)))
    intercept[IllegalStateException](await(out))
    await(rw.onClose)
  }

  test(
    "streamOut: fails if less data is written than specified by the content length header + trailers") {
    val rw = new Pipe[Chunk]
    val out = streamOut(transport, rw, Some(100))

    await(rw.write(Chunk.fromString("foo")))
    await(rw.write(Chunk.last(HeaderMap.newHeaderMap)))
    intercept[IllegalStateException](await(out))
    await(rw.onClose)
  }

  test("streamIn: reads through (no trailers; empty last content)") {
    val in = streamIn(transport)

    read.offer(new DefaultHttpContent(Unpooled.wrappedBuffer("foo".getBytes)))
    read.offer(LastHttpContent.EMPTY_LAST_CONTENT)

    val content = await(in.read())
    assert(Buf.decodeString(content.get.content, UTF_8) == "foo")

    val last = await(in.read()).get
    assert(last.isLast)
    assert(last.content.isEmpty)
    assert(last.trailers.isEmpty)
    assert(in.isDefined)
  }

  test("streamIn: reads through (trailers; empty last content)") {
    val in = streamIn(transport)

    read.offer(new DefaultHttpContent(Unpooled.wrappedBuffer("foo".getBytes)))

    val trailers = new DefaultLastHttpContent()
    trailers.trailingHeaders().add("foo", "bar")
    read.offer(trailers)

    val content = await(in.read())
    assert(Buf.decodeString(content.get.content, UTF_8) == "foo")

    val last = await(in.read()).get
    assert(last.isLast)
    assert(last.content.isEmpty)
    assert(last.trailers.get("foo").contains("bar"))
    assert(in.isDefined)
  }

  test("streamIn: reads through (trailers; non empty last content)") {
    val in = streamIn(transport)

    read.offer(new DefaultHttpContent(Unpooled.wrappedBuffer("foo".getBytes)))

    val trailers = new DefaultLastHttpContent(Unpooled.wrappedBuffer("bar".getBytes))
    trailers.trailingHeaders().add("foo", "bar")
    read.offer(trailers)

    val content = await(in.read())
    assert(Buf.decodeString(content.get.content, UTF_8) == "foo")

    val last = await(in.read()).get
    assert(last.isLast)
    assert(Buf.decodeString(last.content, UTF_8) == "bar")
    assert(last.trailers.get("foo").contains("bar"))
    assert(in.isDefined)
  }

  test("streamIn: discard while reading") {
    val fakeTransport = new TransportProxy[Any, Any](transport) {
      val p = new Promise[Any]
      var interruptedWith: Throwable = null

      p.setInterruptHandler {
        case e => interruptedWith = e
      }

      def read(): Future[Any] = p
      def write(a: Any): Future[Unit] = self.write(a)
    }

    val in = streamIn(fakeTransport)
    val r1 = in.read()
    assert(!r1.isDefined)

    assert(fakeTransport.interruptedWith == null)
    in.discard()
    intercept[ReaderDiscardedException](await(r1))

    assert(!in.isDefined)
    assert(fakeTransport.interruptedWith.isInstanceOf[ReaderDiscardedException])

    // This is what a typical transport will do.
    fakeTransport.p.setException(fakeTransport.interruptedWith)
    intercept[ReaderDiscardedException](await(in))
  }

  test("streamIn: discard while idle") {
    val in = streamIn(transport)

    read.offer(new DefaultHttpContent(Unpooled.wrappedBuffer("foo".getBytes)))
    in.discard()
    intercept[ReaderDiscardedException](await(in))
    intercept[ReaderDiscardedException](await(in.read()))
  }
}
