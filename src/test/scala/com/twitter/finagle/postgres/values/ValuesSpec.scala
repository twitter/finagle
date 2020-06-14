package com.twitter.finagle.postgres.values

import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.time._
import java.time.temporal.ChronoField
import java.util.TimeZone

import com.twitter.finagle.Postgres
import com.twitter.finagle.postgres.Generators._
import com.twitter.finagle.postgres.PostgresClient
import com.twitter.finagle.postgres.PostgresClient.TypeSpecifier
import com.twitter.finagle.postgres.ResultSet
import com.twitter.finagle.postgres.Spec
import com.twitter.finagle.postgres.messages.DataRow
import com.twitter.finagle.postgres.messages.Field
import com.twitter.util.Await
import io.netty.buffer.Unpooled
import org.scalacheck.{Arbitrary, Gen}
import Arbitrary.arbitrary
import com.twitter.concurrent.AsyncStream
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import io.circe.testing.instances.arbitraryJson

class ValuesSpec extends Spec with ScalaCheckDrivenPropertyChecks {

  for {
    hostPort <- sys.env.get("PG_HOST_PORT")
    user <- sys.env.get("PG_USER")
    password = sys.env.get("PG_PASSWORD")
    dbname <- sys.env.get("PG_DBNAME")
    useSsl = sys.env.getOrElse("USE_PG_SSL", "0") == "1"
  } yield {
    val client = Postgres.Client()
      .database(dbname)
      .withCredentials(user, password)
      .conditionally(useSsl, _.withTransport.tlsWithoutValidation)
      .withSessionPool.maxSize(1)
      .newRichClient(hostPort)

    def tzSensitive(tst: TimeZone => Unit) = {
      tst(TimeZone.getTimeZone("UTC"))
      tst(TimeZone.getTimeZone("America/Montreal"))
    }

    def test[T: Arbitrary](
      decoder: ValueDecoder[T],
      encoder: ValueEncoder[T])(
      send: String,
      typ: String,
      toStr: T => String = (t: T) => t.toString,
      tester: (T, T) => Boolean = (a: T, b: T) => a == b,
      nonDefault: Boolean = false,
      localTz: TimeZone = TimeZone.getDefault
    ) = {
      val recv = send.replace("send", "recv")
      implicit val dec = decoder
      val oldDefault = TimeZone.getDefault
      TimeZone.setDefault(localTz)
      try {
        forAll {
          (t: T) =>
            val str = toStr(t)
            val bytes = Await.result(
              client.prepareAndQuery(s"SELECT $send($$1::$typ) AS out", str)(_.get[Array[Byte]]("out"))
            ).head
            val textString = Await.result(
              client.prepareAndQuery(s"SELECT CAST($$1::$typ AS text) AS out", str)(_.get[String]("out"))
            ).head
            val binaryOut = decoder.decodeBinary(recv, Unpooled.wrappedBuffer(bytes), client.charset).get
            val textOut = decoder.decodeText(recv, textString).get

            if (!tester(t, binaryOut))
              fail(s"binary: $t does not match $binaryOut")

            if (!tester(t, textOut))
              fail(s"text: $t does not match $textOut")

            val encodedBinary = encoder.encodeBinary(t, client.charset).getOrElse(fail("Binary encoding produced null"))
            val encodedText = encoder.encodeText(t).getOrElse(fail("Text encoding produced null"))

            val binaryInOut = decoder.decodeBinary(recv, encodedBinary.duplicate(), client.charset).get
            val textInOut = decoder.decodeText(recv, encodedText).get

            if (!tester(t, binaryInOut))
              fail(s"binary: $t was encoded/decoded to $binaryInOut")

            if (!tester(t, textInOut))
              fail(s"text: $t was encoded/decoded to $textInOut")

            if (!nonDefault) {
              val List(rowBinary) = Await.result(
                ResultSet(
                  Array(Field("column", 1, 0)), StandardCharsets.UTF_8,
                  AsyncStream.fromSeq(List(DataRow(Array(Some(encodedBinary))))),
                  Map(0 -> TypeSpecifier(recv, typ, 0)), ValueDecoder.decoders
                ).rows.toSeq.map(_.toList)
              )

              assert(tester(rowBinary.getAnyOption("column").get.asInstanceOf[T], t))
              assert(tester(rowBinary.getAnyOption(0).get.asInstanceOf[T], t))
              assert(tester(rowBinary.getOption[T]("column").get, t))
              assert(tester(rowBinary.getOption[T](0).get, t))
              assert(tester(rowBinary.get[T]("column"), t))
              assert(tester(rowBinary.get[T](0), t))
              assert(tester(rowBinary.getTry[T]("column").get, t))
              assert(tester(rowBinary.getTry[T](0).get, t))

              val List(rowText) = Await.result(
                ResultSet(
                  Array(Field("column", 0, 0)), StandardCharsets.UTF_8,
                  AsyncStream.fromSeq(List(DataRow(Array(Some(Unpooled.copiedBuffer(encodedText, StandardCharsets.UTF_8)))))),
                  Map(0 -> TypeSpecifier(recv, typ, 0)), ValueDecoder.decoders
                ).rows.toSeq.map(_.toList)
              )

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
      } finally {
        TimeZone.setDefault(oldDefault)
      }

    }

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

      tzSensitive { tz =>
        s"parse timestamps when local timezone is ${tz.getID}" in test(ValueDecoder.localDateTime, ValueEncoder.timestamp)(
          "timestamp_send",
          "timestamp",
          ts => java.sql.Timestamp.from(ts.atZone(ZoneId.systemDefault()).toInstant).toString,
          (a, b) => a.getLong(ChronoField.MICRO_OF_DAY) == b.getLong(ChronoField.MICRO_OF_DAY)
        )

        s"parse timestamps with time zone when local timezone is ${tz.getID}" in
          test(ValueDecoder.zonedDateTime, ValueEncoder.timestampTz)(
            "timestamptz_send",
            "timestamptz",
            ts => ts.toOffsetDateTime.toString,
            (a, b) => {
              // when reading the value, the timezone may have changed:
              //   the binary protocol does not include timezone information (everything is in UTC)
              //   the text protocol returns in the server's timezone which may be different than the supplied tz.
              // so we convert the input value to the read value's timezone and then compare them
              a.withZoneSameInstant(b.getOffset) == b
            },
            nonDefault = true,
            localTz = tz
          )

        s"parse timestamps as instants when local timezone is ${tz.getID}" in
          test(ValueDecoder.instant, ValueEncoder.instant)(
            "timestamptz_send",
            "timestamptz",
            ts => ts.toString,
            localTz = tz
          )
      }
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

      "parse json" in test(ValueDecoder.string, ValueEncoder.string)("json_send", "json")(
        Arbitrary(
          Gen.oneOf(
            arbitraryJson.arbitrary.map(_.noSpaces),
            arbitraryJson.arbitrary.map(_.spaces4),
            arbitraryJson.arbitrary.map(_.spaces2)
          ).map(_.replace("\u0000", "\\u0000"))
        )
      )
    }
  }
}
