package com.twitter.finagle.postgresql.transport

import java.nio.charset.StandardCharsets

import com.twitter.finagle.postgresql.Types.Format
import com.twitter.finagle.postgresql.Types.Name
import com.twitter.finagle.postgresql.Types.Oid
import com.twitter.finagle.postgresql.Types.PgArray
import com.twitter.finagle.postgresql.Types.PgArrayDim
import com.twitter.finagle.postgresql.Types.WireValue
import com.twitter.io.Buf
import com.twitter.io.BufByteWriter
import com.twitter.io.ByteReader

object PgBuf {

  class Writer(w: BufByteWriter) {

    def opt[T](o: Option[T])(f: (Writer, T) => Writer): Writer = o match {
      case Some(v) => f(this, v)
      case None => this
    }

    def foreachUnframed[T](xs: TraversableOnce[T])(f: (Writer, T) => Writer): Writer =
      xs.foldLeft(this)(f)

    def foreach[T](xs: Seq[T])(f: (Writer, T) => Writer): Writer = {
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

    def unsignedInt(v: Long): Writer = {
      w.writeIntBE(v)
      this
    }

    // writes a null-terminated string (C-style string)
    def cstring(v: String): Writer = {
      // TODO: not clear what to do about strings that contain the null byte?
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
      case Name.Unnamed => cstring("")
      case Name.Named(value) => cstring(value)
    }

    def format(f: Format): Writer = f match {
      case Format.Text => short(0)
      case Format.Binary => short(1)
    }

    def array(a: PgArray): Writer = {
      int(a.dimensions)
      int(a.dataOffset)
      unsignedInt(a.elemType.value)
      foreachUnframed(a.arrayDims) { (w,d) =>
        w.int(d.size).int(d.lowerBound)
      }
      foreachUnframed(a.data) { (w, v) =>
        w.value(v)
      }
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
    def long(): Long = reader.readLongBE()
    def unsignedInt(): Long = reader.readUnsignedIntBE()

    // reads a null-terminated string (C-style string)
    def cstring(): String = {
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
    def framedBuf(): Buf = reader.readBytes(int())
    def remainingBuf(): Buf = reader.readAll()

    def remaining: Int = reader.remaining

    // https://github.com/postgres/postgres/blob/master/src/include/utils/array.h
    def array(): PgArray = {
      val dims = int()
      val offset = int()
      val tpe = Oid(unsignedInt())
      val arrayDims = for(_ <- 0 until dims) yield PgArrayDim(int(), int())
      val elements = arrayDims.map(_.size).sum
      val values = for(_ <- 0 until elements) yield value()
      if(remaining > 0) sys.error(s"error decoding array, remaining bytes is non zero: $remaining")
      PgArray(
        dimensions = dims,
        dataOffset = offset,
        elemType = tpe,
        arrayDims = arrayDims,
        data = values
      )
    }
  }

  def reader(b: Buf): Reader = new Reader(b)
}

