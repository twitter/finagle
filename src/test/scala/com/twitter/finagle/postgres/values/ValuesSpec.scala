package com.twitter.finagle.postgres.values

import java.time._
import java.time.temporal.{ChronoField, JulianFields}

import com.twitter.finagle.postgres.{Client, Generators, ResultSet, Spec}, Generators._
import com.twitter.util.Await
import org.jboss.netty.buffer.ChannelBuffers
import org.scalacheck.Arbitrary
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class ValuesSpec extends Spec with GeneratorDrivenPropertyChecks {


  def test[T : Arbitrary](
    decoder: ValueDecoder[T])(
    send: String,
    typ: String,
    toStr: T => String = (t: T) => t.toString,
    tester: (T, T) => Boolean = (a: T, b: T) => a == b)(
    implicit client: Client
  ) = forAll {
    (t: T) =>
      //TODO: change this once prepared statements are available
      val escaped = toStr(t).replaceAllLiterally("'", "\\'")
      val ResultSet(List(binaryRow)) = Await.result(client.query(s"SELECT $send('$escaped'::$typ) AS out"))
      val ResultSet(List(textRow)) = Await.result(client.query(s"SELECT CAST('$escaped'::$typ AS text) AS out"))
      val bytes = binaryRow.get[Array[Byte]]("out")
      val textString = textRow.get[String]("out")
      val binaryOut = decoder.decodeBinary(ChannelBuffers.wrappedBuffer(bytes), client.charset).get.value
      val textOut = decoder.decodeText(textString).get.value

      if(!tester(t, binaryOut))
        fail(s"binary: $t does not match $binaryOut")

      if(!tester(t, textOut))
        fail(s"text: $t does not match $textOut")
  }


  for {
    hostPort <- sys.env.get("PG_HOST_PORT")
    user <- sys.env.get("PG_USER")
    password = sys.env.get("PG_PASSWORD")
    dbname <- sys.env.get("PG_DBNAME")
    useSsl = sys.env.getOrElse("USE_PG_SSL", "0") == "1"
  } yield {
    implicit val client = Client(hostPort, user, password, dbname, useSsl)
    "ValueDecoders" should {
      "parse varchars" in test(ValueDecoder.String)("varcharsend", "varchar")
      "parse text" in test(ValueDecoder.String)("textsend", "text")
      "parse booleans" in test(ValueDecoder.Boolean)("boolsend", "boolean", b => if(b) "t" else "f")
      "parse shorts" in test(ValueDecoder.Int2)("int2send", "int2")
      "parse ints" in test(ValueDecoder.Int4)("int4send", "int4")
      "parse longs" in test(ValueDecoder.Int8)("int8send", "int8")
      //precision seems to be an issue when postgres parses text floats
      "parse floats" in test(ValueDecoder.Float4)("float4send", "numeric")
      "parse doubles" in test(ValueDecoder.Float8)("float8send", "numeric")
      "parse numerics" in test(ValueDecoder.Numeric)("numeric_send", "numeric")
      "parse timestamps" in test(ValueDecoder.Timestamp)(
        "timestamp_send",
        "timestamp",
        ts => java.sql.Timestamp.from(ts.atZone(ZoneId.systemDefault()).toInstant).toString,
        (a, b) => a.getLong(ChronoField.MICRO_OF_DAY) == b.getLong(ChronoField.MICRO_OF_DAY)
      )
      "parse timestamps with time zone" in test(ValueDecoder.TimestampTZ)(
        "timestamptz_send",
        "timestamptz",
        ts => java.sql.Timestamp.from(ts.toInstant).toString,
        (a, b) => a.getLong(ChronoField.MICRO_OF_DAY) == b.getLong(ChronoField.MICRO_OF_DAY)
      )
      "parse uuids" in test(ValueDecoder.Uuid)("uuid_send", "uuid")
      "parse dates" in test(ValueDecoder.Date)("date_send", "date")
    }
  }
}
