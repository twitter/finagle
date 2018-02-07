package com.twitter.finagle.postgres.values

import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.time._
import java.time.temporal.ChronoField

import com.twitter.finagle.Postgres
import com.twitter.finagle.postgres.Generators._
import com.twitter.finagle.postgres.PostgresClient
import com.twitter.finagle.postgres.PostgresClient.TypeSpecifier
import com.twitter.finagle.postgres.ResultSet
import com.twitter.finagle.postgres.Spec
import com.twitter.finagle.postgres.messages.DataRow
import com.twitter.finagle.postgres.messages.Field
import com.twitter.util.Await
import org.jboss.netty.buffer.ChannelBuffers
import org.scalacheck.Arbitrary
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class ValuesSpec extends Spec with GeneratorDrivenPropertyChecks {

  def test[T : Arbitrary](
    decoder: ValueDecoder[T],
    encoder: ValueEncoder[T])(
    send: String,
    typ: String,
    toStr: T => String = (t: T) => t.toString,
    tester: (T, T) => Boolean = (a: T, b: T) => a == b,
    nonDefault: Boolean = false)(
    implicit client: PostgresClient
  ) = {
    val recv = send.replace("send", "recv")
    implicit val dec = decoder
    forAll {
      (t: T) =>
        //TODO: change this once prepared statements are available
        val escaped = toStr(t).replaceAllLiterally("'", "\\'")
        val ResultSet(List(binaryRow)) = Await.result(client.query(s"SELECT $send('$escaped'::$typ) AS out"))
        val ResultSet(List(textRow)) = Await.result(client.query(s"SELECT CAST('$escaped'::$typ AS text) AS out"))
        val bytes = binaryRow.get[Array[Byte]]("out")
        val textString = textRow.get[String]("out")
        val binaryOut = decoder.decodeBinary(recv, ChannelBuffers.wrappedBuffer(bytes), client.charset).get
        val textOut = decoder.decodeText(recv, textString).get

        if(!tester(t, binaryOut))
          fail(s"binary: $t does not match $binaryOut")

        if(!tester(t, textOut))
          fail(s"text: $t does not match $textOut")

        val encodedBinary = encoder.encodeBinary(t, client.charset).getOrElse(fail("Binary encoding produced null"))
        val encodedText = encoder.encodeText(t).getOrElse(fail("Text encoding produced null"))

        val binaryInOut = decoder.decodeBinary(recv, encodedBinary.duplicate(), client.charset).get
        val textInOut = decoder.decodeText(recv, encodedText).get

        if(!tester(t, binaryInOut))
          fail(s"binary: $t was encoded/decoded to $binaryInOut")

        if(!tester(t, textInOut))
          fail(s"text: $t was encoded/decoded to $textInOut")

        if(!nonDefault) {
          val List(rowBinary) = ResultSet(
            Array(Field("column", 1, 0)), StandardCharsets.UTF_8,
            List(DataRow(Array(Some(encodedBinary)))),
            Map(0 -> TypeSpecifier(recv, typ, 0)), ValueDecoder.decoders
          ).rows

          assert(tester(rowBinary.getAnyOption("column").get.asInstanceOf[T], t))
          assert(tester(rowBinary.getAnyOption(0).get.asInstanceOf[T], t))
          assert(tester(rowBinary.getOption[T]("column").get, t))
          assert(tester(rowBinary.getOption[T](0).get, t))
          assert(tester(rowBinary.get[T]("column"), t))
          assert(tester(rowBinary.get[T](0), t))
          assert(tester(rowBinary.getTry[T]("column").get, t))
          assert(tester(rowBinary.getTry[T](0).get, t))

          val List(rowText) = ResultSet(
            Array(Field("column", 0, 0)), StandardCharsets.UTF_8,
            List(DataRow(Array(Some(ChannelBuffers.copiedBuffer(encodedText, StandardCharsets.UTF_8))))),
            Map(0 -> TypeSpecifier(recv, typ, 0)), ValueDecoder.decoders
          ).rows

          assert(tester(rowText.getAnyOption("column").get.asInstanceOf[T], t))
          assert(tester(rowText.getAnyOption(0).get.asInstanceOf[T], t))
          assert(tester(rowText.getOption[T]("column").get, t))
          assert(tester(rowText.getOption[T](0).get, t))
          assert(tester(rowText.get[T]("column"), t))
          assert(tester(rowText.get[T](0), t))
          assert(tester(rowText.getTry[T]("column").get, t))
          assert(tester(rowText.getTry[T](0).get, t))
        }
    }
  }


  for {
    hostPort <- sys.env.get("PG_HOST_PORT")
    user <- sys.env.get("PG_USER")
    password = sys.env.get("PG_PASSWORD")
    dbname <- sys.env.get("PG_DBNAME")
    useSsl = sys.env.getOrElse("USE_PG_SSL", "0") == "1"
  } yield {
    implicit val client = Postgres.Client()
      .database(dbname)
      .withCredentials(user, password)
      .conditionally(useSsl, _.withTransport.tlsWithoutValidation)
      .withSessionPool.maxSize(1)
      .newRichClient(hostPort)

    "ValueDecoders" should {
      "parse varchars" in test(ValueDecoder.string, ValueEncoder.string)("varcharsend", "varchar")
      "parse text" in test(ValueDecoder.string, ValueEncoder.string)("textsend", "text")
      "parse booleans" in test(ValueDecoder.boolean, ValueEncoder.boolean)("boolsend", "boolean", b => if(b) "t" else "f")
      "parse shorts" in test(ValueDecoder.int2, ValueEncoder.int2)("int2send", "int2")
      "parse ints" in test(ValueDecoder.int4, ValueEncoder.int4)("int4send", "int4")
      "parse longs" in test(ValueDecoder.int8, ValueEncoder.int8)("int8send", "int8")
      //precision seems to be an issue when postgres parses text floats
      "parse floats" in test(ValueDecoder.float4, ValueEncoder.float4)("float4send", "numeric")
      "parse doubles" in test(ValueDecoder.float8, ValueEncoder.float8)("float8send", "numeric")
      "parse numerics" in test(ValueDecoder.bigDecimal, ValueEncoder.numeric)("numeric_send", "numeric")
      "parse timestamps" in test(ValueDecoder.localDateTime, ValueEncoder.timestamp)(
        "timestamp_send",
        "timestamp",
        ts => java.sql.Timestamp.from(ts.atZone(ZoneId.systemDefault()).toInstant).toString,
        (a, b) => a.getLong(ChronoField.MICRO_OF_DAY) == b.getLong(ChronoField.MICRO_OF_DAY)
      )
      "parse timestamps with time zone" in test(ValueDecoder.zonedDateTime, ValueEncoder.timestampTz)(
        "timestamptz_send",
        "timestamptz",
        ts => ts.toOffsetDateTime.toString,
        (a, b) => a.getLong(ChronoField.MICRO_OF_DAY) == b.getLong(ChronoField.MICRO_OF_DAY),
        nonDefault = true
      )
      "parse timestamps as instants" in test(ValueDecoder.instant, ValueEncoder.instant)(
        "timestamptz_send",
        "timestamptz",
        ts => ts.toString
      )
      "parse uuids" in test(ValueDecoder.uuid, ValueEncoder.uuid)("uuid_send", "uuid")
      "parse dates" in test(ValueDecoder.localDate, ValueEncoder.date)("date_send", "date")

      "parse jsonb" in {
        val json = "{\"a\":\"b\"}"
        val createBuffer = () => {
          ValueEncoder[JSONB].encodeBinary(JSONB(json.getBytes), Charset.defaultCharset()).get
        }
        val buffer = createBuffer()
        val version = buffer.readByte()
        val encoded = Array.fill(buffer.readableBytes())(buffer.readByte())
        version must equal(1)
        encoded must equal(json.getBytes)

        val decoded = ValueDecoder[JSONB].decodeBinary("", createBuffer(), Charset.defaultCharset()).get()
        JSONB.stringify(decoded) must equal(json)
      }
    }
  }
}
