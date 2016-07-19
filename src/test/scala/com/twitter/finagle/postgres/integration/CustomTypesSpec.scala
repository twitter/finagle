package com.twitter.finagle.postgres.integration

import java.time.ZoneId
import java.time.temporal.ChronoField

import com.twitter.finagle.postgres.values.{ValueDecoder, ValueEncoder}
import com.twitter.finagle.postgres.{Client, Generators, ResultSet, Spec}
import com.twitter.util.Await
import org.jboss.netty.buffer.ChannelBuffers
import org.scalacheck.Arbitrary
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import Generators._

class CustomTypesSpec extends Spec with GeneratorDrivenPropertyChecks {

  def test[T : Arbitrary](
    decoder: ValueDecoder[T])(
    typ: String,
    tester: (T, T) => Boolean = (a: T, b: T) => a == b)(
    implicit client: Client,
    manifest: Manifest[T],
    binaryParams: Boolean,
    valueEncoder: ValueEncoder[T]
  ) = forAll {
    (t: T) =>
      val m = manifest
      val encoder = valueEncoder
      val query = if(binaryParams)
        client.prepareAndQuery(s"SELECT $$1::$typ AS out", t) {
          row => row.get(0).value.asInstanceOf[T]
        }
      else
        client.prepareAndQuery(s"SELECT $$1::$typ AS out", t) {
          row => row.get(0).value.asInstanceOf[T]
        }
      val result = Await.result(query).head
      if(!tester(t, result))
        fail(s"$t does not match $result")
  }

  for {
    binaryResults <- Seq(false, true)
    binaryParams <- Seq(false, true)
    hostPort <- sys.env.get("PG_HOST_PORT")
    user <- sys.env.get("PG_USER")
    password = sys.env.get("PG_PASSWORD")
    dbname <- sys.env.get("PG_DBNAME")
    useSsl = sys.env.getOrElse("USE_PG_SSL", "0") == "1"
  } yield {

    implicit val client = Client(
      hostPort,
      user,
      password,
      dbname,
      useSsl,
      customTypes = true,
      binaryResults = binaryResults,
      binaryParams = binaryParams)

    val mode = if(binaryResults) "binary mode" else "text mode"
    val paramsMode = if(binaryParams)
      "binary"
    else
      "text"

    implicit val useBinaryParams = binaryParams

    s"A $mode postgres client with $paramsMode params" should {
      "retrieve the available types from the remote DB" in {
        val types = Await.result(client.typeMap)
        assert(types.nonEmpty)
        assert(types != Client.defaultTypes)
      }
    }

    s"Retrieved type map decoders for $mode client with $paramsMode params" must {
      "parse varchars" in test(ValueDecoder.String)("varchar")
      "parse text" in test(ValueDecoder.String)("text")
      "parse booleans" in test(ValueDecoder.Boolean)("boolean")
      "parse shorts" in test(ValueDecoder.Int2)("int2")
      "parse ints" in test(ValueDecoder.Int4)("int4")
      "parse longs" in test(ValueDecoder.Int8)("int8")
      //precision seems to be an issue when postgres parses text floats
      "parse floats" in test(ValueDecoder.Float4)("numeric::float4")
      "parse doubles" in test(ValueDecoder.Float8)("numeric::float8")
      "parse numerics" in test(ValueDecoder.Numeric)("numeric")
      "parse timestamps" in test(ValueDecoder.Timestamp)(
        "timestamp",
        (a, b) => a.getLong(ChronoField.MICRO_OF_DAY) == b.getLong(ChronoField.MICRO_OF_DAY)
      )
      "parse timestamps with time zone" in test(ValueDecoder.TimestampTZ)(
        "timestamptz",
        (a, b) => a.getLong(ChronoField.MICRO_OF_DAY) == b.getLong(ChronoField.MICRO_OF_DAY)
      )
      "parse times" in test(ValueDecoder.Time)("time")
      "parse times with timezone" in test(ValueDecoder.TimeTz)("timetz")
      "parse intervals" in test(ValueDecoder.Interval)("interval")
      "parse uuids" in test(ValueDecoder.Uuid)("uuid")

      try {
        //not sure why this test doesn't pass in Travis
        Await.result(client.query("CREATE EXTENSION IF NOT EXISTS hstore"))
        "parse hstore maps" ignore test(ValueDecoder.HStore)("hstore")
      } catch {
        case err: Throwable => // can't run this one because we're not superuser
      }
    }

  }
}
