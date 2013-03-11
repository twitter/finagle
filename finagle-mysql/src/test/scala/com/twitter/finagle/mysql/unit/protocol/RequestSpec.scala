package com.twitter.finagle.exp.mysql.protocol

import java.sql.{Timestamp, Date => SQLDate}
import java.util.Date
import java.util.Calendar
import org.specs.SpecificationWithJUnit

class RequestSpec extends SpecificationWithJUnit {
  "Request" should {
    "encode simple request" in {
      val seq = 3
      val bytes = Array[Byte](0x01, 0x02, 0x03, 0x04)
      val cb = Buffer.toChannelBuffer(bytes)

      val req = new Request(seq.toShort) {
        val data = cb
      }

      val br = BufferReader(req.toChannelBuffer)

      br.readInt24() mustEqual bytes.size
      br.readByte() mustEqual seq
      br.take(bytes.size) must containAll(bytes)
    }

    "encode simple command request" in {
      val bytes = "table".getBytes
      val cmd = 0x00
      val req = new SimpleCommandRequest(cmd.toByte, bytes)

      val br = BufferReader(req.toChannelBuffer)
      br.readInt24() mustEqual bytes.size + 1 // cmd byte
      br.readByte() mustEqual 0x00
      br.readByte() mustEqual cmd
      br.take(bytes.size) must containAll(bytes)
    }
  }


  "Execute Request" should {
    // supported types
    val strVal = "test"
    val boolVal = true
    val byteVal = 1.toByte
    val shortVal = 2.toShort
    val intVal = 3
    val longVal = 4L
    val floatVal = 1.5F
    val doubleVal = 2.345
    val cal = Calendar.getInstance()
    val millis = cal.getTimeInMillis
    val timestamp = new Timestamp(millis)
    val sqlDate = new SQLDate(millis)
    val datetime = new Date(millis)
    val params = Array(
      strVal,
      boolVal,
      byteVal,
      shortVal,
      null,
      intVal,
      longVal,
      floatVal,
      doubleVal,
      null,
      timestamp,
      sqlDate,
      datetime,
      null
    )
    val stmtId = 1
    val ps = PreparedStatement(stmtId, params.size)
    ps.parameters = params

    "encode" in {
      val flags, iteration = 0
      val req = ExecuteRequest(ps, flags.toByte, iteration)

      val br = BufferReader(req.toChannelBuffer)
      br.skip(4) // header

      val cmd = br.readByte()
      val id = br.readInt()
      val flg = br.readByte()
      val iter = br.readInt()

      "statement Id, flags, and iteration count" in {
       cmd mustEqual Command.COM_STMT_EXECUTE
       id mustEqual stmtId
       flg mustEqual flags
       iter mustEqual iteration
      }


      // null bit map
      val len = ((params.size + 7) / 8).toInt
      val bytesAsBigEndian = br.take(len).reverse
      val bits = BigInt(bytesAsBigEndian)

      "null bits" in {
        for (i <- 0 until params.size) {
          if (params(i) == null)
            bits.testBit(i) mustEqual true
          else
            bits.testBit(i) mustEqual false
        }
      }


      val hasNewParams = br.readByte() == 1
      hasNewParams mustEqual ps.hasNewParameters

      if (hasNewParams) {

        "type codes" in {
          for (p <- params) {
            br.readShort() mustEqual Type.getCode(p)
          }
        }

        "values" in {
          br.skip(params.size * 2)

          val str = br.readLengthCodedString()

          "String" in {
            str mustEqual strVal
          }

          val bool = br.readByte()

          "Boolean" in {
            bool mustEqual (if (boolVal) 1 else 0)
          }

          val byte = br.readByte()

          "Byte" in {
            byte mustEqual byteVal
          }

          val short = br.readShort()

          "Short" in {
            short mustEqual shortVal
          }

          val int = br.readInt()

          "Int" in {
            int mustEqual intVal
          }

          val long = br.readLong()

          "Long" in {
            long mustEqual longVal
          }

          val float = br.readFloat()

          "Float" in {
            float mustEqual floatVal
          }

          val dbl = br.readDouble()

          "Double" in {
            dbl mustEqual doubleVal
          }

          val TimestampValue(ts) = TimestampValue(br.readLengthCodedBytes())

          "java.sql.Timestamp" in {
            ts  mustEqual timestamp
          }

          val DateValue(d) = DateValue(br.readLengthCodedBytes())

          "java.sql.Date" in {
            d.toString mustEqual sqlDate.toString
          }

          val TimestampValue(dt) = TimestampValue(br.readLengthCodedBytes())

          "java.util.Date" in {
            dt mustEqual datetime
          }
        }
      }
    }
  }
}
