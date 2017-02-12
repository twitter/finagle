package com.twitter.finagle.http.netty

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.time._
import com.twitter.finagle.http.exp.Multi
import com.twitter.finagle.http._
import com.twitter.finagle.transport.QueueTransport
import com.twitter.io.Buf
import com.twitter.util.Await
import java.nio.charset.StandardCharsets.UTF_8
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class Netty3StreamTransportTest extends FunSuite {
  class Ctx {
    val in = new AsyncQueue[Any]
    val out = new AsyncQueue[Any]
    val qTransport = new QueueTransport(in, out)
    val transport = new Netty3ClientStreamTransport(qTransport)
  }

  test("writing unchunked content gets turned into a single whole request") {
    val ctx = new Ctx
    import ctx._

    val f = transport.write(Request("twitter.com"))
    val req = Await.result(in.poll(), 5.seconds).asInstanceOf[HttpRequest]
    Await.result(f, 5.seconds)
    assert(req.getUri() == "twitter.com")
    assert(!req.isChunked)
    assert(!in.poll().isDefined)
  }

  test("reading unchunked content means we get a satisfied future") {
    val ctx = new Ctx
    import ctx._

    out.offer(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK))
    val Multi(rep, future) = Await.result(transport.read(), 5.seconds)
    assert(rep.version == Version.Http11)
    assert(rep.status == Status.Ok)
    assert(!rep.isChunked)
    assert(future.isDefined)
  }

  // we need to write in sections because we try to chunk in size Int.MaxValue if possible
  test("writing a streamed request is chunky") {
    val ctx = new Ctx
    import ctx._

    val streamedReq = Request("twitter.com")
    streamedReq.setChunked(true)

    // enqueue writing some 0s
    val write1 = streamedReq.writer.write(Buf.Utf8("0" * 1000))
    assert(!write1.isDefined)

    // start actually writing
    // we've now written the headers, and the first chunk
    val f = transport.write(streamedReq)
    assert(write1.isDefined)
    assert(!f.isDefined)

    // the first request has the headers
    val req1 = Await.result(in.poll(), 5.seconds).asInstanceOf[HttpRequest]
    assert(req1.getUri() == "twitter.com")
    assert(req1.isChunked)
    assert(!f.isDefined)
    assert(req1.getContent.toString(UTF_8) == "")

    // the first chunk has the 0s
    val freq2 = in.poll()
    assert(freq2.isDefined)
    val req2 = Await.result(freq2, 5.seconds).asInstanceOf[HttpChunk]
    assert(!f.isDefined)
    assert(req2.getContent.toString(UTF_8) == ("0" * 1000))

    // start writing some 1s, and then read them
    val freq3 = in.poll()
    assert(!freq3.isDefined)
    assert(streamedReq.writer.write(Buf.Utf8("1" * 1000)).isDefined)
    val req3 = Await.result(freq3, 5.seconds).asInstanceOf[HttpChunk]
    assert(!f.isDefined)
    assert(req3.getContent.toString(UTF_8) == ("1" * 1000))

    // close the stream
    assert(streamedReq.writer.close().isDefined)
    val req4 = Await.result(in.poll(), 5.seconds).asInstanceOf[HttpChunk]
    assert(req4 == HttpChunk.LAST_CHUNK)
    assert(f.isDefined)
  }

  test("reading a chunky response is streamed") {
    val ctx = new Ctx
    import ctx._

    val nettyRep = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
    nettyRep.setChunked(true)

    // enqueue the headers, which lets them know to read, and gives them a handle
    out.offer(nettyRep)
    val Multi(rep, future) = Await.result(transport.read(), 5.seconds)
    assert(rep.version == Version.Http11)
    assert(rep.status == Status.Ok)
    assert(rep.isChunked)
    assert(!future.isDefined)

    // write some 0s
    out.offer(new DefaultHttpChunk(ChannelBuffers.copiedBuffer("0" * 1000, UTF_8)))
    val Some(Buf.Utf8(zeros)) = Await.result(rep.reader.read(1000), 5.seconds)
    assert(zeros == "0" * 1000)

    val f = rep.reader.read(1000)
    assert(!f.isDefined)
    // write some 1s
    out.offer(new DefaultHttpChunk(ChannelBuffers.copiedBuffer("1" * 1000, UTF_8)))
    val Some(Buf.Utf8(ones)) = Await.result(f, 5.seconds)
    assert(ones == "1" * 1000)

    // finish writing the stream
    out.offer(HttpChunk.LAST_CHUNK)
    assert(future.isDefined)
    assert(Await.result(rep.reader.read(1000), 5.seconds) == None)
  }
}
