package com.twitter.finagle.postgresql.transport

import java.nio.charset.StandardCharsets

import com.twitter.io.Buf
import com.twitter.io.BufByteWriter
import com.twitter.io.ByteReader

object PgBuf {

  class Writer(w: BufByteWriter) {

    def opt[T](o: Option[T])(f: (T, Writer) => Writer): Writer = o match {
      case Some(v) => f(v, this)
      case None => this
    }

    def foreach[T](xs: TraversableOnce[T])(f: (T, Writer) => Writer): Writer = {
      xs.map { x => f(x, this) }
      this
    }

    def byte(v: Byte): Writer = {
      w.writeByte(v)
      this
    }

    def short(v: Short): Writer = {
      w.writeShortBE(v)
      this
    }

    def int(v: Int): Writer = {
      w.writeIntBE(v)
      this
    }

    def string(v: String): Writer = {
      w.writeString(v, StandardCharsets.UTF_8)
      byte(0)
    }

    def buf(b: Buf): Writer = {
      w.writeBytes(b)
      this
    }

    def framed(f: Writer => Buf): Writer = {
      val b = f(writer)
      int(b.length + 4) // length including self
      buf(b)
    }

    def build: Buf =
      w.owned
  }

  def writer: Writer = new Writer(BufByteWriter.dynamic())

  class Reader(b: Buf) {
    val reader = ByteReader(b)

    def byte(): Byte = reader.readByte()
    def short(): Short = reader.readShortBE()
    def int(): Int = reader.readIntBE()
    def string(): String = {
      val length = reader.remainingUntil(0)

      val str = if(length < 0) sys.error("invalid string value")
      else if(length == 0) ""
      else reader.readString(length, StandardCharsets.UTF_8)

      reader.skip(1) // skip the null-terminating byte
      str
    }
    def buf(length: Int): Buf = reader.readBytes(length)
    def remainingBuf(): Buf = reader.readAll()

    def remaining: Int = reader.remaining
  }

  def reader(b: Buf): Reader = new Reader(b)
}

