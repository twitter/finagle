package com.twitter.finagle.exp.mysql.integration

import com.twitter.finagle.exp.mysql._
import com.twitter.util.{Await, Time, TwitterDateFormat}
import java.sql.Timestamp
import java.util.{Calendar, TimeZone}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NumericTypeTest extends FunSuite with IntegrationClient {
  for (c <- client) {
    Await.ready(c.query(
      """CREATE TEMPORARY TABLE IF NOT EXISTS `numeric` (
        `smallint` smallint(6) NOT NULL,
        `tinyint` tinyint(4) NOT NULL,
        `mediumint` mediumint(9) NOT NULL,
        `int` int(11) NOT NULL,
        `bigint` bigint(20) NOT NULL,
        `float` float(4,2) NOT NULL,
        `double` double(4,3) NOT NULL,
        `decimal` decimal(30,11) NOT NULL,
        `bit` bit(1) NOT NULL,
        PRIMARY KEY (`smallint`)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""))

    Await.ready(c.query(
      """INSERT INTO `numeric` (`smallint`,
        `tinyint`, `mediumint`, `int`,
        `bigint`, `float`, `double`, `decimal`, `bit`)
        VALUES (1, 2, 3, 4, 5, 1.61, 1.618, 1.61803398875, 1);"""))

    val textEncoded = Await.result(c.query("SELECT * FROM `numeric`") map {
      case rs: ResultSet if rs.rows.size > 0 => rs.rows(0)
      case v => fail("expected a ResultSet with 1 row but received: %s".format(v))
    })

    val ps = c.prepare("SELECT * FROM `numeric`")
    val binaryrows = Await.result(ps.select()(identity))
    assert(binaryrows.size == 1)
    val binaryEncoded = binaryrows(0)

    testRow(textEncoded)
    testRow(binaryEncoded)
  }

  def testRow(row: Row) {
    val rowType = row.getClass.getName
    test("extract %s from %s".format("tinyint", rowType)) {
      row("tinyint") match {
        case Some(ByteValue(b)) => assert(b == 2)
        case v => fail("expected ByteValue but got %s".format(v))
      }
    }

    test("extract %s from %s".format("smallint", rowType)) {
      row("smallint") match {
        case Some(ShortValue(s)) => assert(s == 1)
        case v => fail("expected ShortValue but got %s".format(v))
      }
    }

    test("extract %s from %s".format("mediumint", rowType)) {
      row("mediumint") match {
        case Some(IntValue(i)) => assert(i == 3)
        case v => fail("expected IntValue but got %s".format(v))
      }
    }

    test("extract %s from %s".format("int", rowType)) {
      row("int") match {
        case Some(IntValue(i)) => assert(i == 4)
        case v => fail("expected IntValue but got %s".format(v))
      }
    }

    test("extract %s from %s".format("bigint", rowType)) {
      row("bigint") match {
        case Some(LongValue(l)) => assert(l == 5)
        case v => fail("expected LongValue but got %s".format(v))
      }
    }

    test("extract %s from %s".format("float", rowType)) {
      row("float") match {
        case Some(FloatValue(f)) =>
          assert(math.abs((f - 1.61F)) <= .000001)
        case v => fail("expected FloatValue but got %s".format(v))
      }
    }

    test("extract %s from %s".format("double", rowType)) {
      row("double") match {
        case Some(DoubleValue(d)) =>
          assert(math.abs((d - 1.618)) <= .000001)
        case v => fail("expected DoubleValue but got %s".format(v))
      }
    }

    test("extract %s from %s".format("decimal", rowType)) {
      row("decimal") match {
        case Some(BigDecimalValue(bd)) => assert(bd == BigDecimal(1.61803398875))
        case v => fail("expected BigDecimalValue but got %s".format(v))
      }
    }

    test("extract %s from %s".format("bit", rowType)) {
      row("bit") match {
        case Some(v: RawValue) => // pass
        case v => fail("expected a RawValue but got %s".format(v))
      }
    }
  }
}

@RunWith(classOf[JUnitRunner])
class BlobTypeTest extends FunSuite with IntegrationClient {
  for (c <- client) {
    Await.ready(c.query(
      """CREATE TEMPORARY TABLE `blobs` (
        `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
        `char` char(5) DEFAULT NULL,
        `varchar` varchar(10) DEFAULT NULL,
        `tinytext` tinytext,
        `text` text,
        `mediumtext` mediumtext,
        `tinyblob` tinyblob,
        `mediumblob` mediumblob,
        `blob` blob,
        `binary` binary(2) DEFAULT NULL,
        `varbinary` varbinary(10) DEFAULT NULL,
        `enum` enum('small','medium','large') DEFAULT NULL,
        `set` set('1','2','3','4') DEFAULT NULL,
        PRIMARY KEY (`id`)
      ) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;"""))

    Await.ready(c.query(
      """INSERT INTO `blobs` (`id`, `char`,
        `varchar`, `tinytext`,
        `text`, `mediumtext`, `tinyblob`,
        `mediumblob`, `blob`, `binary`,
        `varbinary`, `enum`, `set`)
        VALUES (1, 'a', 'b', 'c', 'd', 'e', X'66',
        X'67', X'68', X'6970', X'6A', 'small', '1');"""))

    val textEncoded = Await.result(c.query("SELECT * FROM `blobs`") map {
      case rs: ResultSet if rs.rows.size > 0 => rs.rows(0)
      case v => fail("expected a ResultSet with 1 row but received: %s".format(v))
    })

    val ps = c.prepare("SELECT * FROM `blobs`")
    val binaryrows = Await.result(ps.select()(identity))
    assert(binaryrows.size == 1)
    val binaryEncoded = binaryrows(0)

    testRow(textEncoded)
    testRow(binaryEncoded)
  }

  def testRow(row: Row) {
    val rowType = row.getClass.getName
    test("extract %s from %s".format("char", rowType)) {
      row("char") match {
        case Some(StringValue(s)) => assert(s == "a")
        case a => fail("Expected StringValue but got %s".format(a))
      }
    }

    test("extract %s from %s".format("varchar", rowType)) {
      row("varchar") match {
        case Some(StringValue(s)) => assert(s == "b")
        case a => fail("Expected StringValue but got %s".format(a))
      }
    }

    test("extract %s from %s".format("tinytext", rowType)) {
      row("tinytext") match {
        case Some(StringValue(s)) => assert(s == "c")
        case a => fail("Expected StringValue but got %s".format(a))
      }
    }

    test("extract %s from %s".format("text", rowType)) {
      row("text") match {
        case Some(StringValue(s)) => assert(s == "d")
        case a => fail("Expected StringValue but got %s".format(a))
      }
    }

    test("extract %s from %s".format("mediumtext", rowType)) {
      row("mediumtext") match {
        case Some(StringValue(s)) => assert(s == "e")
        case a => fail("Expected StringValue but got %s".format(a))
      }
    }

    test("extract %s from %s".format("tinyblob", rowType)) {
      row("tinyblob") match {
        case Some(RawValue(_, _, _, bytes)) => assert(bytes.toList == List(0x66))
        case a => fail("Expected RawValue but got %s".format(a))
      }
    }

    test("extract %s from %s".format("mediumblob", rowType)) {
      row("mediumblob") match {
        case Some(RawValue(_, _, _, bytes)) => assert(bytes.toList == List(0x67))
        case a => fail("Expected RawValue but got %s".format(a))
      }
    }

    test("extract %s from %s".format("blob", rowType)) {
      row("blob") match {
        case Some(RawValue(_, _, _, bytes)) => assert(bytes.toList == List(0x68))
        case a => fail("Expected RawValue but got %s".format(a))
      }
    }

    test("extract %s from %s".format("binary", rowType)) {
      row("binary") match {
        case Some(RawValue(_, _, _, bytes)) => assert(bytes.toList == List(0x69, 0x70))
        case a => fail("Expected RawValue but got %s".format(a))
      }
    }

    test("extract %s from %s".format("varbinary", rowType)) {
      row("varbinary") match {
        case Some(RawValue(_, _, _, bytes)) => assert(bytes.toList == List(0x6A))
        case a => fail("Expected RawValue but got %s".format(a))
      }
    }

    test("extract %s from %s".format("enum", rowType)) {
      row("enum") match {
        case Some(StringValue(s)) => assert(s == "small")
        case a => fail("Expected StringValue but got %s".format(a))
      }
    }

    test("extract %s from %s".format("set", rowType)) {
      row("set") match {
        case Some(StringValue(s)) => assert(s == "1")
        case a => fail("Expected StringValue but got %s".format(a))
      }
    }
  }
}

@RunWith(classOf[JUnitRunner])
class DateTimeTypeTest extends FunSuite with IntegrationClient {
  for (c <- client) {
    Await.ready(c.query(
      """CREATE TEMPORARY TABLE `datetime` (
        `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
        `date` date NOT NULL,
        `datetime` datetime NOT NULL,
        `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        `time` time NOT NULL,
        `year` year(4) NOT NULL,
        PRIMARY KEY (`id`)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""))

    Await.ready(c.query(
      """INSERT INTO `datetime`
        (`id`, `date`, `datetime`, `timestamp`, `time`, `year`)
        VALUES (1, '2013-11-02', '2013-11-02 19:56:24',
        '2013-11-02 19:56:36', '19:56:32', '2013');"""))

    val textEncoded = Await.result(c.query("SELECT * FROM `datetime`") map {
      case rs: ResultSet if rs.rows.size > 0 => rs.rows(0)
      case v => fail("expected a ResultSet with 1 row but received: %s".format(v))
    })

    val ps = c.prepare("SELECT * FROM `datetime`")
    val binaryrows = Await.result(ps.select()(identity))
    assert(binaryrows.size == 1)
    val binaryEncoded = binaryrows(0)

    testRow(textEncoded)
    testRow(binaryEncoded)
  }

  def testRow(row: Row) {
    val rowType = row.getClass.getName
    test("extract %s from %s".format("date", rowType)) {
      row("date") match {
        case Some(DateValue(d)) => assert(d.toString() == "2013-11-02")
        case a => fail("Expected DateValue but got %s".format(a))
      }
    }

    val timestampValueLocal = new TimestampValue(TimeZone.getDefault(), TimeZone.getDefault())
    val timestampValueUTC = new TimestampValue(TimeZone.getDefault(), TimeZone.getTimeZone("UTC"))
    val timestampValueEST = new TimestampValue(TimeZone.getDefault(), TimeZone.getTimeZone("EST"))

    for ((repr, secs) <- Seq(("datetime", 24), ("timestamp", 36))) {
      test("extract %s from %s in local time".format(repr, rowType)) {
        row(repr) match {
          case Some(timestampValueLocal(t)) =>
            val timestamp = java.sql.Timestamp.valueOf("2013-11-02 19:56:" + secs)
            assert(t == timestamp)
          case a => fail("Expected TimestampValue but got %s".format(a))
        }
      }

      test("extract %s from %s in UTC".format(repr, rowType)) {
        row(repr) match {
          case Some(timestampValueUTC(t)) =>
            val format = TwitterDateFormat("yyyy-MM-dd HH:mm:ss")
            format.setTimeZone(TimeZone.getTimeZone("UTC"))
            val timestamp = new Timestamp(format.parse("2013-11-02 19:56:" + secs).getTime)
            assert(t == timestamp)
          case a => fail("Expected TimestampValue but got %s".format(a))
        }
      }

      test("extract %s from %s in EST".format(repr, rowType)) {
        row(repr) match {
          case Some(timestampValueEST(t)) =>
            val format = TwitterDateFormat("yyyy-MM-dd HH:mm:ss")
            format.setTimeZone(TimeZone.getTimeZone("EST"))
            val timestamp = new Timestamp(format.parse("2013-11-02 19:56:" + secs).getTime)
            assert(t == timestamp)
          case a => fail("Expected TimestampValue but got %s".format(a))
        }
      }
    }

    test("extract %s from %s".format("time", rowType)) {
      row("time") match {
        case Some(r: RawValue) => // pass
        case a => fail("Expected RawValue but got %s".format(a))
      }
    }

    test("extract %s from %s".format("year", rowType)) {
      row("year") match {
        case Some(ShortValue(s)) => assert(s == 2013)
        case a => fail("Expected ShortValue but got %s".format(a))
      }
    }
  }
}
