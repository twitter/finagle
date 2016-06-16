package com.twitter.finagle.postgres.values

import java.util.{Date, TimeZone, UUID}

import com.twitter.finagle.postgres.Spec

class ValuesSpec extends Spec {
  "A StringValueParser" should {
    "parse varchars" in {
      StringValueParser.parseVarChar(StringValueEncoder.encode("someTestString ")) must equal(Value("someTestString "))
    }

    "parse booleans" in {
      StringValueParser.parseBoolean(StringValueEncoder.encode("t")) must equal(Value(true))
      StringValueParser.parseBoolean(StringValueEncoder.encode("f")) must equal(Value(false))
    }

    "parse numbers" in {
      StringValueParser.parseInt2(StringValueEncoder.encode(555)) must equal(Value(555))
      StringValueParser.parseInt8(StringValueEncoder.encode(1234567891011L)) must equal(Value(1234567891011L))
      StringValueParser.parseFloat4(StringValueEncoder.encode(-10.17)) must equal(Value(-10.17f))
      StringValueParser.parseFloat8(StringValueEncoder.encode(1312310.17881919)) must equal(Value(1312310.17881919))
    }

    "parse a timestamp without a timezone" in {
      val timestamp = StringValueParser.parseTimestamp(StringValueEncoder.encode("2015-01-08 14:55:12.123")).value

      // Java interprets timestamp as being in local timezone; need to adjust to get epoch time
      timestamp.getTime must equal(1420728912123L - TimeZone.getDefault.getOffset(1420728912123L))
    }

    "parse timestamps with timezones" in {
      val timestamp = StringValueParser.parseTimestampTZ(StringValueEncoder.encode("2015-01-08 14:55:12.123-05")).value
      // Note: Milliseconds in timestamp are ignored
      timestamp.getTime must equal(1420746912000L)

      val timestamp2 = StringValueParser.parseTimestampTZ(StringValueEncoder.encode("2015-01-08 14:55:12+0800")).value
      timestamp2.getTime must equal(1420700112000L)
    }

    "parse UUIDs" in {
      val uuid = StringValueParser.parseUUID(StringValueEncoder.encode("deadbeef-dead-dead-beef-deaddeadbeef")).value
      uuid must equal(UUID.fromString("deadbeef-dead-dead-beef-deaddeadbeef"))
    }
  }
}
