package com.twitter.finagle.postgres.integration

import java.time.ZoneId
import java.time.temporal.ChronoField

import com.twitter.finagle.Postgres
import com.twitter.finagle.postgres.values.{ValueDecoder, ValueEncoder}
import com.twitter.finagle.postgres.{Generators, PostgresClient, ResultSet, Spec}
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
    implicit client: PostgresClient,
    manifest: Manifest[T],
    binaryParams: Boolean,
    valueEncoder: ValueEncoder[T]
  ) = {
    implicit val dec = decoder
    forAll {
      (t: T) =>
        val m = manifest
        val encoder = valueEncoder
        val query = if(binaryParams)
          client.prepareAndQuery(s"SELECT $$1::$typ AS out", t) {
            row => row.get[T](0)
          }
        else
          client.prepareAndQuery(s"SELECT $$1::$typ AS out", t) {
            row => row.get[T](0)
          }
        val result = Await.result(query).head
        if(!tester(t, result))
          fail(s"$t does not match $result")
    }
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

    implicit val client = Postgres.Client()
      .database(dbname)
      .withCredentials(user, password)
      .withBinaryParams(binaryParams)
      .withBinaryResults(binaryResults)
      .conditionally(useSsl, _.withTransport.tlsWithoutValidation)
      .withSessionPool.maxSize(1)
      .newRichClient(hostPort)


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
        assert(types != PostgresClient.defaultTypes)
      }
    }

    s"Retrieved type map decoders for $mode client with $paramsMode params" must {
      "parse varchars" in test(ValueDecoder.string)("varchar")
      "parse text" in test(ValueDecoder.string)("text")
      "parse booleans" in test(ValueDecoder.boolean)("boolean")
      "parse shorts" in test(ValueDecoder.int2)("int2")
      "parse ints" in test(ValueDecoder.int4)("int4")
      "parse longs" in test(ValueDecoder.int8)("int8")
      //precision seems to be an issue when postgres parses text floats
      "parse floats" in test(ValueDecoder.float4)("numeric::float4")
      "parse doubles" in test(ValueDecoder.float8)("numeric::float8")
      "parse numerics" in test(ValueDecoder.bigDecimal)("numeric")
      "parse timestamps" in test(ValueDecoder.localDateTime)(
        "timestamp",
        (a, b) => a.getLong(ChronoField.MICRO_OF_DAY) == b.getLong(ChronoField.MICRO_OF_DAY)
      )
      "parse timestamps with time zone" in test(ValueDecoder.zonedDateTime)(
        "timestamptz",
        (a, b) => a.getLong(ChronoField.MICRO_OF_DAY) == b.getLong(ChronoField.MICRO_OF_DAY)
      )
      "parse times" in test(ValueDecoder.localTime)("time")
      "parse times with timezone" in test(ValueDecoder.offsetTime)("timetz")
      "parse intervals" in test(ValueDecoder.interval)("interval")
      "parse uuids" in test(ValueDecoder.uuid)("uuid")
      "parse hstore maps" in test(ValueDecoder.hstoreMap)("hstore")
    }

  }
}
