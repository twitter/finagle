package com.twitter.finagle.kestrel.integration

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import com.twitter.finagle.kestrel.protocol._
import com.twitter.finagle.memcached.protocol.text.{Decoding, Tokens, TokensWithData}
import com.twitter.io.Buf
import com.twitter.util.{Duration, Time}

@RunWith(classOf[JUnitRunner])
class CommandEncodingTest extends FunSuite {

  private def testGetCommandEncodeDecode(
    name: String,
    timeout: Option[Duration] = None,
    mkCommand: (Buf, Option[Duration]) => GetCommand,
    extractCommand: GetCommand => Option[(Buf,Option[Duration])]
  ) {
    val decoder = new DecodingToCommand
    val encoder = new CommandToEncoding

    val command = mkCommand(Buf.Utf8(name), timeout)
    val enc = encoder.encode(null, null, command).asInstanceOf[Tokens]

    val decoded = decoder.parseNonStorageCommand(enc.tokens).asInstanceOf[GetCommand]

    val Some((Buf.Utf8(queueName), expiry)) = extractCommand(decoded)

    assert(queueName == name)
    assert(timeout.map{_.inSeconds} == expiry.map{_.inSeconds})
  }

  private def testCommandEncodeDecode(
    name: String,
    mkCommand: Buf => Command,
    extractCommand: Command => Option[Buf]
  ) {
    val decoder = new DecodingToCommand
    val encoder = new CommandToEncoding

    val command = mkCommand(Buf.Utf8(name))
    val enc = encoder.encode(null, null, command).asInstanceOf[Tokens]

    val decoded = decoder.parseNonStorageCommand(enc.tokens)

    val Some(Buf.Utf8(queueName)) = extractCommand(decoded)

    assert(queueName == name)
  }

  test("SET can be decoded") {
    val decoder = new DecodingToCommand
    val encoder = new CommandToEncoding

    val qName = "MyQueue"
    val data = "hi"
    val time = Time.now
    val msg = encoder.encode(
      null,
      null,
      Set(Buf.Utf8(qName),
        time,
        Buf.Utf8(data))
    ).asInstanceOf[TokensWithData]

    val Set(Buf.Utf8(queueName), expiry, Buf.Utf8(dataOut)) =
      decoder.parseStorageCommand(msg.tokens, msg.data)

    assert(queueName == qName)
    assert(dataOut == data)
    assert(expiry.inSeconds == time.inSeconds)
  }

  test("DELETE can be decoded") {
    testCommandEncodeDecode(
      name = "MyQueue",
      mkCommand = Delete.apply,
      extractCommand = { case d: Delete => Delete.unapply(d) }
    )
  }

  test("FLUSH can be decoded") {
    testCommandEncodeDecode(
      name = "MyQueue",
      mkCommand = Flush.apply,
      extractCommand = { case f: Flush => Flush.unapply(f) }
    )
  }

  test("GET without timeout can be decoded") {
    testGetCommandEncodeDecode(
      name = "MyQueue",
      mkCommand = Get.apply,
      extractCommand = { case c: Get => Get.unapply(c) }
    )
  }

  test("GET with timeout can be decoded") {
    testGetCommandEncodeDecode(
      name = "MyQueue",
      timeout = Some(Duration.fromSeconds(2)),
      mkCommand = Get.apply,
      extractCommand = { case c: Get => Get.unapply(c) }
    )
  }


  test("PEEK without timeout can be decoded") {
    testGetCommandEncodeDecode(
      name = "MyQueue",
      mkCommand = Peek.apply,
      extractCommand = { case c: Peek => Peek.unapply(c) }
    )
  }

  test("PEEK with timeout can be decoded") {
    testGetCommandEncodeDecode(
      name = "MyQueue",
      timeout = Some(Duration.fromSeconds(2)),
      mkCommand = Peek.apply,
      extractCommand = { case c: Peek => Peek.unapply(c) }
    )
  }

  test("ABORT without timeout can be decoded") {
    testGetCommandEncodeDecode(
      name = "MyQueue",
      mkCommand = Get.apply,
      extractCommand = { case c: Get => Get.unapply(c) }
    )
  }

  test("ABORT with timeout can be decoded") {
    testGetCommandEncodeDecode(
      name = "MyQueue",
      timeout = Some(Duration.fromSeconds(2)),
      mkCommand = Abort.apply,
      extractCommand = { case c: Abort => Abort.unapply(c) }
    )
  }

  test("CLOSE without timeout can be decoded") {
    testGetCommandEncodeDecode(
      name = "MyQueue",
      mkCommand = Close.apply,
      extractCommand = { case c: Close => Close.unapply(c) }
    )
  }

  test("CLOSE with timeout can be decoded") {
    testGetCommandEncodeDecode(
      name = "MyQueue",
      timeout = Some(Duration.fromSeconds(2)),
      mkCommand = Close.apply,
      extractCommand = { case c: Close => Close.unapply(c) }
    )
  }

  test("OPEN with timeout can be decoded") {
    testGetCommandEncodeDecode(
      name = "MyQueue",
      timeout = Some(Duration.fromSeconds(2)),
      mkCommand = Open.apply,
      extractCommand = { case c: Open => Open.unapply(c) }
    )
  }

  test("OPEN without timeout can be decoded") {
    testGetCommandEncodeDecode(
      name = "MyQueue",
      mkCommand = Open.apply,
      extractCommand = { case c: Open => Open.unapply(c) }
    )
  }


  test("CLOSE AND OPEN without timeout can be decoded") {
    testGetCommandEncodeDecode(
      name = "MyQueue",
      mkCommand = CloseAndOpen.apply,
      extractCommand = { case c: CloseAndOpen => CloseAndOpen.unapply(c) }
    )
  }

  test("CLOSE AND OPEN with timeout can be decoded") {
    testGetCommandEncodeDecode(
      name = "MyQueue",
      timeout = Some(Duration.fromSeconds(2)),
      mkCommand = CloseAndOpen.apply,
      extractCommand = { case c: CloseAndOpen => CloseAndOpen.unapply(c) }
    )
  }
}
