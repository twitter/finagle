package com.twitter.finagle.postgresql.transport

import java.nio.charset.StandardCharsets

import com.twitter.finagle.postgresql.Types.Format
import com.twitter.finagle.postgresql.Types.Name
import com.twitter.finagle.postgresql.Types.WireValue
import com.twitter.io.Buf
import com.twitter.io.BufByteWriter
import com.twitter.io.ByteReader

object PgBuf {

  class Writer(w: BufByteWriter) {

    def opt[T](o: Option[T])(f: (T, Writer) => Writer): Writer = o match {
      case Some(v) => f(v, this)
      case None => this
    }

    def foreachUnframed[T](xs: TraversableOnce[T])(f: (T, Writer) => Writer): Writer = {
      xs.map { x => f(x, this) }
      this
    }

    def foreach[T](xs: Seq[T])(f: (T, Writer) => Writer): Writer = {
      short(xs.length.toShort)
      foreachUnframed(xs)(f)
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

    def framedBuf(b: Buf): Writer = {
      int(b.length).buf(b)
    }

    def framed(f: Writer => Buf): Writer = {
      val b = f(writer)
      int(b.length + 4) // length including self
      buf(b)
    }

    def value(v: WireValue): Writer = v match {
      case WireValue.Null => int(-1)
      case WireValue.Value(value) => framedBuf(value)
    }

    def name(n: Name): Writer = n match {
      case Name.Unnamed => string("")
      case Name.Named(value) => string(value)
    }

    def format(f: Format): Writer = f match {
      case Format.Text => short(0)
      case Format.Binary => short(1)
    }

    def build: Buf =
      w.owned
  }

  def writer: Writer = new Writer(BufByteWriter.dynamic())

  class Reader(b: Buf) {
    private[this] val reader = ByteReader(b)

    def byte(): Byte = reader.readByte()
    def short(): Short = reader.readShortBE()
    def int(): Int = reader.readIntBE()
    def string(): String = {
      val length = reader.remainingUntil(0)

      val str = if(length < 0) sys.error(s"invalid string length $length")
      else if(length == 0) ""
      else reader.readString(length, StandardCharsets.UTF_8)

      reader.skip(1) // skip the null-terminating byte
      str
    }
    def format(): Format = short() match {
      case 0 => Format.Text
      case 1 => Format.Binary
      case v => sys.error(s"unexpected format value $v")
    }
    def collect[T](f: Reader => T): IndexedSeq[T] = {
      val size = short()
      val builder = IndexedSeq.newBuilder[T]
      builder.sizeHint(size)
      for(_ <- 0 until size) { builder += f(this) }
      builder.result()
    }
    def value(): WireValue = {
      int() match {
        case -1 => WireValue.Null
        case length => WireValue.Value(buf(length))
      }
    }
    def buf(length: Int): Buf = reader.readBytes(length)
    def remainingBuf(): Buf = reader.readAll()

    def remaining: Int = reader.remaining
  }

  def reader(b: Buf): Reader = new Reader(b)
}

