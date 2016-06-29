package com.twitter.finagle.postgres.integration

import java.time.ZoneId
import java.time.temporal.ChronoField

import com.twitter.finagle.postgres.values.ValueDecoder
import com.twitter.finagle.postgres.{Client, ResultSet, Spec, Generators}, Generators._
import com.twitter.util.Await
import org.jboss.netty.buffer.ChannelBuffers
import org.scalacheck.Arbitrary
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class CustomTypesSpec extends Spec with GeneratorDrivenPropertyChecks {

  def test[T : Arbitrary](
    decoder: ValueDecoder[T])(
    typ: String,
    toStr: T => String = (t: T) => t.toString,
    tester: (T, T) => Boolean = (a: T, b: T) => a == b)(
    implicit client: Client
  ) = forAll {
    (t: T) =>
      //TODO: change this once prepared statements are available
      val result = Await.result(client.prepareAndQuery(s"SELECT $$1::$typ AS out", t) {
        row => row.get(0).value.asInstanceOf[T]
      }).head
      if(!tester(t, result))
        fail(s"$t does not match $result")
  }

  for {
    binary <- Seq(false, true)
    hostPort <- sys.env.get("PG_HOST_PORT")
    user <- sys.env.get("PG_USER")
    password = sys.env.get("PG_PASSWORD")
    dbname <- sys.env.get("PG_DBNAME")
    useSsl = sys.env.getOrElse("USE_PG_SSL", "0") == "1"
  } yield {

    implicit val client = Client(hostPort, user, password, dbname, useSsl, customTypes = true, binaryResults = binary)

    val mode = if(binary) "binary mode" else "text mode"

    s"A $mode postgres client" should {
      "retrieve the available types from the remote DB" in {
        val types = Await.result(client.typeMap)
        assert(types.nonEmpty)
        assert(types != Client.defaultTypes)
      }
    }

    s"Retrieved type map decoders for $mode client" must {
      "parse varchars" in test(ValueDecoder.String)("varchar")
      "parse text" in test(ValueDecoder.String)("text")
      "parse booleans" in test(ValueDecoder.Boolean)("boolean", b => if(b) "t" else "f")
      "parse shorts" in test(ValueDecoder.Int2)("int2")
      "parse ints" in test(ValueDecoder.Int4)("int4")
      "parse longs" in test(ValueDecoder.Int8)("int8")
      //precision seems to be an issue when postgres parses text floats
      "parse floats" in test(ValueDecoder.Float4)("numeric::float4")
      "parse doubles" in test(ValueDecoder.Float8)("numeric::float8")
      "parse numerics" in test(ValueDecoder.Numeric)("numeric")
      "parse timestamps" in test(ValueDecoder.Timestamp)(
        "timestamp",
        ts => java.sql.Timestamp.from(ts.atZone(ZoneId.systemDefault()).toInstant).toString,
        (a, b) => a.getLong(ChronoField.MICRO_OF_DAY) == b.getLong(ChronoField.MICRO_OF_DAY)
      )
      "parse timestamps with time zone" in test(ValueDecoder.TimestampTZ)(
        "timestamptz",
        ts => java.sql.Timestamp.from(ts.toInstant).toString,
        (a, b) => a.getLong(ChronoField.MICRO_OF_DAY) == b.getLong(ChronoField.MICRO_OF_DAY)
      )
      "parse uuids" in test(ValueDecoder.Uuid)("uuid")
    }

  }
}
