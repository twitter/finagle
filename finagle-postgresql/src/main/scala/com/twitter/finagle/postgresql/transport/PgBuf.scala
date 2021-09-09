package com.twitter.finagle.postgresql.transport

import java.nio.charset.StandardCharsets
import com.twitter.finagle.postgresql.PgSqlClientError
import com.twitter.finagle.postgresql.Types.Format
import com.twitter.finagle.postgresql.Types.Inet
import com.twitter.finagle.postgresql.Types.Name
import com.twitter.finagle.postgresql.Types.Numeric
import com.twitter.finagle.postgresql.Types.NumericSign
import com.twitter.finagle.postgresql.Types.Oid
import com.twitter.finagle.postgresql.Types.PgArray
import com.twitter.finagle.postgresql.Types.PgArrayDim
import com.twitter.finagle.postgresql.Types.Timestamp
import com.twitter.finagle.postgresql.Types.WireValue
import com.twitter.io.Buf
import com.twitter.io.BufByteWriter
import com.twitter.io.ByteReader
import scala.reflect.ClassTag

object PgBuf {

  /**
   * Wire value for the -Infinity timestamp
   */
  private[postgresql] val NegInfinity: Long = 0x8000000000000000L

  /**
   * Wire value for the Infinity timestamp
   */
  private[postgresql] val Infinity: Long = 0x7fffffffffffffffL

  class Writer(w: BufByteWriter) {

    def opt[T](o: Option[T])(f: (Writer, T) => Writer): Writer = o match {
      case Some(v) => f(this, v)
      case None => this
    }

    def foreachUnframed[T](xs: Iterable[T])(f: (Writer, T) => Writer): Writer =
      xs.foldLeft(this)(f)

    def foreach[T](xs: Seq[T])(f: (Writer, T) => Writer): Writer = {
      short(xs.length.toShort)
      foreachUnframed(xs)(f)
    }

    def byte(v: Byte): Writer = {
      w.writeByte(v.toInt)
      this
    }

    def short(v: Short): Writer = {
      w.writeShortBE(v.toInt)
      this
    }

    def unsignedShort(v: Int): Writer = {
      w.writeShortBE(v)
      this
    }

    def int(v: Int): Writer = {
      w.writeIntBE(v.toLong)
      this
    }

    def unsignedInt(v: Long): Writer = {
      w.writeIntBE(v)
      this
    }

    def long(v: Long): Writer = {
      w.writeLongBE(v)
      this
    }

    def double(v: Double): Writer = {
      w.writeDoubleBE(v)
      this
    }

    def float(v: Float): Writer = {
      w.writeFloatBE(v)
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

    def framedBuf(b: Buf): Writer =
      int(b.length).buf(b)

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
      foreachUnframed(a.arrayDims) { (w, d) =>
        w.int(d.size).int(d.lowerBound)
      }
      foreachUnframed(a.data) { (w, v) =>
        w.value(v)
      }
    }

    def timestamp(ts: Timestamp): Writer =
      ts match {
        case Timestamp.NegInfinity => long(NegInfinity)
        case Timestamp.Infinity => long(Infinity)
        case Timestamp.Micros(v) => long(v)
      }

    def numericSign(sign: NumericSign): Writer =
      sign match {
        case NumericSign.Positive => short(0)
        case NumericSign.Negative => short(0x4000)
        case NumericSign.NaN => unsignedShort(0xc000)
        case NumericSign.Infinity => unsignedShort(0xd000)
        case NumericSign.NegInfinity => unsignedShort(0xf000)
      }

    def numeric(n: Numeric): Writer = {
      unsignedShort(n.digits.length)
      short(n.weight)
      numericSign(n.sign)
      unsignedShort(n.displayScale)
      foreachUnframed(n.digits)((w, d) => w.unsignedShort(d.toInt))
    }

    def inet(i: Inet): Writer = {
      val family = i.ipAddress match {
        case _: java.net.Inet4Address => 2
        case _: java.net.Inet6Address => 3
      }
      byte(family.toByte)
      byte(i.netmask.toByte)
      byte(0) // is CIDR

      val addr = i.ipAddress.getAddress
      byte(addr.length.toByte)
      buf(Buf.ByteArray.Owned(addr))
    }

    def build: Buf =
      w.owned()
  }

  def writer: Writer = new Writer(BufByteWriter.dynamic())

  object Reader {
    def apply(b: Buf): Reader = {
      val reader = ByteReader(b)
      new Reader(reader)
    }
  }

  final class Reader(val reader: ByteReader) extends AnyVal {
    def byte(): Byte = reader.readByte()
    def unsignedByte(): Short = reader.readUnsignedByte()
    def short(): Short = reader.readShortBE()
    def unsignedShort(): Int = reader.readUnsignedShortBE()
    def int(): Int = reader.readIntBE()
    def long(): Long = reader.readLongBE()
    def unsignedInt(): Long = reader.readUnsignedIntBE()
    def float(): Float = reader.readFloatBE()
    def double(): Double = reader.readDoubleBE()

    // reads a null-terminated string (C-style string)
    def cstring(): String = {
      val length = reader.remainingUntil(0)

      val str =
        if (length < 0) sys.error(s"invalid string length $length")
        else if (length == 0) ""
        else reader.readString(length, StandardCharsets.UTF_8)

      reader.skip(1) // skip the null-terminating byte
      str
    }

    def format(): Format = short() match {
      case 0 => Format.Text
      case 1 => Format.Binary
      case v => sys.error(s"unexpected format value $v")
    }

    def collect[T: ClassTag](f: Reader => T): IndexedSeq[T] = {
      val size = short().toInt
      val builder = new Array[T](size)
      var idx = 0
      while (idx < size) {
        builder(idx) = f(this)
        idx += 1
      }
      scala.collection.compat.immutable.ArraySeq.unsafeWrapArray(builder)
    }

    def value(): WireValue = int() match {
      case -1 => WireValue.Null
      case length => WireValue.Value(buf(length))
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
      val arrayDims = for (_ <- 0 until dims) yield PgArrayDim(int(), int())
      val elements = arrayDims.map(_.size).sum
      val values = for (_ <- 0 until elements) yield value()
      if (remaining > 0)
        throw new PgSqlClientError(s"error decoding array, remaining bytes is non zero: $remaining")
      PgArray(
        dimensions = dims,
        dataOffset = offset,
        elemType = tpe,
        arrayDims = arrayDims,
        data = values
      )
    }

    def timestamp(): Timestamp =
      long() match {
        case NegInfinity => Timestamp.NegInfinity
        case Infinity => Timestamp.Infinity
        case v => Timestamp.Micros(v)
      }

    // TODO: this is actually a bit mask, but it's not clear if any other values are possible anyway
    def numericSign(): NumericSign =
      unsignedShort() match {
        case 0 => NumericSign.Positive
        case 0x4000 => NumericSign.Negative
        case 0xc000 => NumericSign.NaN
        case 0xd000 => NumericSign.Infinity
        case 0xf000 => NumericSign.NegInfinity
        case v => throw new PgSqlClientError(f"unexpected numeric sign value: $v%04X")
      }

    def numeric(): Numeric = {
      val len = unsignedShort()
      Numeric(
        weight = short(),
        sign = numericSign(),
        displayScale = unsignedShort(),
        digits = Seq.fill(len)(short())
      )
    }

    def inet(): Inet = {
      val family = byte()
      val netmask = unsignedByte()
      val _ = byte() // is CIDR
      val len = byte()
      (family, len) match {
        case (2, 4) => // IPv4
          if (netmask > 32) throw new PgSqlClientError(s"invalid netmask for IPv4 $netmask")
          val bytes = buf(4)
          val addr = java.net.InetAddress.getByAddress(Buf.ByteArray.Owned.extract(bytes))
          Inet(addr, netmask)
        case (3, 16) => // IPv6
          if (netmask > 128) throw new PgSqlClientError(s"invalid netmask for IPv6 $netmask")
          val bytes = buf(16)
          val addr = java.net.InetAddress.getByAddress(Buf.ByteArray.Owned.extract(bytes))
          Inet(addr, netmask)
        case _ =>
          throw new PgSqlClientError(s"invalid length ($len) for inet family ($family)")
      }
    }
  }

  def reader(b: Buf): Reader = Reader(b)
}
