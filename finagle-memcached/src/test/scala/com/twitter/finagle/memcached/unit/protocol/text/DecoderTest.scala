package com.twitter.finagle.memcached.protocol.text

import com.twitter.finagle.memcached.protocol.text.client.ClientDecoder
import com.twitter.io.Buf
import com.twitter.util.Promise
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DecoderTest extends FunSuite {

  test("Decode tokens when no more data needed returns Decoding") {
    val decoder = new ClientDecoder
    val buffer = Buf.Utf8("I'M ALL DONE")
    val needsData: Seq[Buf] => Int = _ => -1
    val awaitData: (Seq[Buf], Int) => Unit = (_, _) => ()
    val continue: Seq[Buf] => Decoding = { tokens => Tokens(tokens) }
    assert(decoder.decodeLine(buffer, needsData, awaitData)(continue) ==
      Tokens(Seq(Buf.Utf8("I'M"), Buf.Utf8("ALL"), Buf.Utf8("DONE"))))
  }

  test("Decode tokens when no more data needed doesn't call 'continue' function") {
    val decoder = new ClientDecoder
    val awaitDataCalled = new Promise[Unit]
    val buffer = Buf.Utf8("I'M ALL DONE")
    val needsData: Seq[Buf] => Int = _ => -1
    val awaitData: (Seq[Buf], Int) => Unit = (_, _) => {
      awaitDataCalled.setDone()
    }
    val continue: Seq[Buf] => Decoding = { tokens => Tokens(tokens) }
    decoder.decodeLine(buffer, needsData, awaitData)(continue)
    assert(!awaitDataCalled.isDefined)
  }

  test("Decode tokens when data needed returns null") {
    val decoder = new ClientDecoder
    val buffer = Buf.Utf8("WAITING")
    val needsData: Seq[Buf] => Int = _ => 3
    val awaitData: (Seq[Buf], Int) => Unit = (_, _) => ()
    val continue: Seq[Buf] => Decoding = { tokens => Tokens(tokens) }
    assert(decoder.decodeLine(buffer, needsData, awaitData)(continue) == null)
  }

  test("Decode tokens when data needed calls 'continue' function") {
    val decoder = new ClientDecoder
    val awaitDataCalled = new Promise[Unit]
    val buffer = Buf.Utf8("WAITING")
    val needsData: Seq[Buf] => Int = _ => 3
    val awaitData: (Seq[Buf], Int) => Unit = (_, _) => {
      awaitDataCalled.setDone()
    }
    val continue: Seq[Buf] => Decoding = { tokens => Tokens(tokens) }
    decoder.decodeLine(buffer, needsData, awaitData)(continue)
    assert(awaitDataCalled.isDefined)
  }
}