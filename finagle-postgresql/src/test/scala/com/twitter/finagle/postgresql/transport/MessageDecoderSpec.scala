package com.twitter.finagle.postgresql.transport

import java.nio.ByteBuffer
import java.nio.ByteOrder

import com.twitter.finagle.postgresql.BackendMessage.AuthenticationCleartextPassword
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationGSS
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationGSSContinue
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationKerberosV5
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationMD5Password
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationMessage
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationOk
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationSASL
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationSASLContinue
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationSASLFinal
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationSCMCredential
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationSSPI
import com.twitter.finagle.postgresql.BackendMessage.BindComplete
import com.twitter.finagle.postgresql.BackendMessage.CommandComplete
import com.twitter.finagle.postgresql.BackendMessage.EmptyQueryResponse
import com.twitter.finagle.postgresql.BackendMessage.FailedTx
import com.twitter.finagle.postgresql.BackendMessage.Field
import com.twitter.finagle.postgresql.BackendMessage.InTx
import com.twitter.finagle.postgresql.BackendMessage.NoData
import com.twitter.finagle.postgresql.BackendMessage.NoTx
import com.twitter.finagle.postgresql.BackendMessage.Parameter
import com.twitter.finagle.postgresql.BackendMessage.ParameterDescription
import com.twitter.finagle.postgresql.BackendMessage.ParseComplete
import com.twitter.finagle.postgresql.BackendMessage.PortalSuspended
import com.twitter.finagle.postgresql.BackendMessage.ReadyForQuery
import com.twitter.finagle.postgresql.BackendMessage.TxState
import com.twitter.finagle.postgresql.Types.AttributeId
import com.twitter.finagle.postgresql.Types.Format
import com.twitter.finagle.postgresql.Types.Oid
import com.twitter.finagle.postgresql.Types.WireValue
import com.twitter.finagle.postgresql.BackendMessage
import com.twitter.finagle.postgresql.BackendMessage.CloseComplete
import com.twitter.finagle.postgresql.BackendMessage.CommandTag
import com.twitter.finagle.postgresql.BackendMessage.CopyDone
import com.twitter.finagle.postgresql.BackendMessage.CopyInResponse
import com.twitter.finagle.postgresql.BackendMessage.CopyOutResponse
import com.twitter.finagle.postgresql.PgSqlClientError
import com.twitter.finagle.postgresql.PropertiesSpec
import com.twitter.io.Buf
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.specs2.mutable.Specification

class MessageDecoderSpec extends Specification with PropertiesSpec {

  def mkBuf(capacity: Int = 32768)(f: ByteBuffer => ByteBuffer): Buf = {
    val bb = ByteBuffer.allocate(capacity).order(ByteOrder.BIG_ENDIAN)
    f(bb)
    bb.flip()
    Buf.ByteBuffer.Owned(bb)
  }
  def cstring(s: String) = s.getBytes("UTF8") :+ 0x00.toByte

  def unsignedInt(v: Long) = (v & 0xffffffffL).toInt

  def fieldByte(field: Field): Byte =
    field match {
      case Field.LocalizedSeverity => 'S'
      case Field.Severity => 'V'
      case Field.Code => 'C'
      case Field.Message => 'M'
      case Field.Detail => 'D'
      case Field.Hint => 'H'
      case Field.Position => 'P'
      case Field.InternalPosition => 'p'
      case Field.InternalQuery => 'q'
      case Field.Where => 'W'
      case Field.Schema => 's'
      case Field.Table => 't'
      case Field.Column => 'c'
      case Field.DataType => 'd'
      case Field.Constraint => 'n'
      case Field.File => 'F'
      case Field.Line => 'L'
      case Field.Routine => 'R'
      case Field.Unknown(c) => c.toByte
    }

  implicit lazy val arbCommandComplete: Arbitrary[CommandComplete] =
    Arbitrary(genCommandTag.map(CommandComplete))

  lazy val genAuthenticationMessage: Gen[AuthenticationMessage] =
    Gen.oneOf(
      Gen.const(AuthenticationOk),
      Gen.const(AuthenticationKerberosV5),
      Gen.const(AuthenticationCleartextPassword),
      Gen.const(AuthenticationSCMCredential),
      Gen.containerOfN[Array, Byte](4, Arbitrary.arbitrary[Byte]).map(Buf.ByteArray.Owned(_)).map(
        AuthenticationMD5Password
      ),
      Gen.const(AuthenticationGSS),
      Gen.const(AuthenticationSSPI),
      genBuf.map(AuthenticationGSSContinue),
      genAsciiString.map(str => AuthenticationSASL(str.value)),
      genBuf.map(AuthenticationSASLContinue),
      genBuf.map(AuthenticationSASLFinal),
    )
  implicit lazy val arbAuthenticationMessage: Arbitrary[AuthenticationMessage] = Arbitrary(genAuthenticationMessage)

  lazy val genTxState: Gen[TxState] = Gen.oneOf(NoTx, InTx, FailedTx)
  implicit lazy val arbReadyForQuery: Arbitrary[ReadyForQuery] = Arbitrary(genTxState.map(ReadyForQuery))

  implicit lazy val arbParameterDescription: Arbitrary[ParameterDescription] =
    Arbitrary(Arbitrary.arbitrary[IndexedSeq[Oid]].map(ParameterDescription))

  val genCopyTuple: Gen[(Format, IndexedSeq[Format])] = for {
    nbCols <- Gen.chooseNum(1, 256)
    o <- arbitrary[Format]
    formats <- Gen.listOfN(nbCols, arbitrary[Format])
  } yield (o, formats.toIndexedSeq)
  implicit lazy val arbCopyInResponse: Arbitrary[CopyInResponse] = Arbitrary(genCopyTuple.map(CopyInResponse.tupled))
  implicit lazy val arbCopyOutResponse: Arbitrary[CopyOutResponse] = Arbitrary(genCopyTuple.map(CopyOutResponse.tupled))

  def decodeFragment[M <: BackendMessage: Arbitrary](dec: MessageDecoder[M])(toPacket: M => Packet) = {
    "decode packet body correctly" in prop { msg: M =>
      dec.decode(PgBuf.reader(toPacket(msg).body)).asScala must beSuccessfulTry(msg)
    }
    "decode packet correctly" in prop { msg: M =>
      MessageDecoder.fromPacket(toPacket(msg)).asScala must beSuccessfulTry(msg)
    }
  }

  def singleton[M <: BackendMessage](key: Byte, msg: M) =
    "decode packet correctly" in {
      MessageDecoder.fromPacket(Packet(Some(key), Buf.Empty)).asScala must beSuccessfulTry(msg)
    }

  "MessageDecoder" should {
    "fail when packet is not identified" in {
      val decoded = MessageDecoder.fromPacket(Packet(None, Buf.Empty))
      decoded.get() must throwA[IllegalStateException]("invalid backend packet, missing message type.")
    }

    "fail when decoder is not implemented" in {
      val decoded = MessageDecoder.fromPacket(Packet(Some(' '), Buf.Empty))
      decoded.get() must throwA[PgSqlClientError](s"unimplemented message ' '")
    }

    "ErrorResponse" should decodeFragment(MessageDecoder.errorResponseDecoder) { msg =>
      Packet(
        cmd = Some('E'),
        body = mkBuf() { bb =>
          msg.values.foreach { case (field, value) =>
            bb.put(fieldByte(field)).put(cstring(value))
          }
          bb.put(0.toByte)
        }
      )
    }

    "NoticeResponse" should decodeFragment(MessageDecoder.noticeResponseDecoder) { msg =>
      Packet(
        cmd = Some('N'),
        body = mkBuf() { bb =>
          msg.values.foreach { case (field, value) =>
            bb.put(fieldByte(field)).put(cstring(value))
          }
          bb.put(0.toByte)
        }
      )
    }

    "BackendKeyData" should decodeFragment(MessageDecoder.backendKeyDataDecoder) { msg =>
      Packet(
        cmd = Some('K'),
        body = mkBuf() { bb =>
          bb.putInt(msg.pid)
          bb.putInt(msg.secret)
        }
      )
    }

    "command tag" should {
      def assert(value: String, command: CommandTag.Command, rows: Int) =
        MessageDecoder.commandTag(value) must_== CommandTag.AffectedRows(command, rows)

      "parse insert" in {
        assert("INSERT 0 42", CommandTag.Insert, 42)
        assert("INSERT 42 0", CommandTag.Insert, 0)
      }
      "parse delete" in {
        assert("DELETE 42", CommandTag.Delete, 42)
        assert("DELETE 0", CommandTag.Delete, 0)
      }
      "parse update" in {
        assert("UPDATE 42", CommandTag.Update, 42)
        assert("UPDATE 0", CommandTag.Update, 0)
      }
      "parse select" in {
        assert("SELECT 42", CommandTag.Select, 42)
        assert("SELECT 0", CommandTag.Select, 0)
      }
      "parse move" in {
        assert("MOVE 42", CommandTag.Move, 42)
        assert("MOVE 0", CommandTag.Move, 0)
      }
      "parse fetch" in {
        assert("FETCH 42", CommandTag.Fetch, 42)
        assert("FETCH 0", CommandTag.Fetch, 0)
      }
      "parse other" in prop { str: String =>
        MessageDecoder.commandTag(str) must_== CommandTag.Other(str)
      }
    }

    "CommandComplete" should decodeFragment(MessageDecoder.commandCompleteDecoder) { msg =>
      Packet(
        cmd = Some('C'),
        body = mkBuf() { bb =>
          val str = msg.commandTag match {
            case CommandTag.AffectedRows(CommandTag.Insert, rows) => s"INSERT 0 $rows"
            case CommandTag.AffectedRows(CommandTag.Delete, rows) => s"DELETE $rows"
            case CommandTag.AffectedRows(CommandTag.Update, rows) => s"UPDATE $rows"
            case CommandTag.AffectedRows(CommandTag.Select, rows) => s"SELECT $rows"
            case CommandTag.AffectedRows(CommandTag.Move, rows) => s"MOVE $rows"
            case CommandTag.AffectedRows(CommandTag.Fetch, rows) => s"FETCH $rows"
            case CommandTag.Other(value) => value
          }
          bb.put(cstring(str))
        }
      )
    }

    "AuthenticationMessage" should decodeFragment(MessageDecoder.authenticationMessageDecoder) { msg =>
      Packet(
        cmd = Some('R'),
        body = mkBuf() { bb =>
          msg match {
            case AuthenticationOk => bb.putInt(0)
            case AuthenticationKerberosV5 => bb.putInt(2)
            case AuthenticationCleartextPassword => bb.putInt(3)
            case AuthenticationMD5Password(buf) => bb.putInt(5).put(Buf.ByteBuffer.Shared.extract(buf))
            case AuthenticationSCMCredential => bb.putInt(6)
            case AuthenticationGSS => bb.putInt(7)
            case AuthenticationGSSContinue(buf) => bb.putInt(8).put(Buf.ByteBuffer.Shared.extract(buf))
            case AuthenticationSSPI => bb.putInt(9)
            case AuthenticationSASL(m) => bb.putInt(10).put(cstring(m))
            case AuthenticationSASLContinue(buf) => bb.putInt(11).put(Buf.ByteBuffer.Shared.extract(buf))
            case AuthenticationSASLFinal(buf) => bb.putInt(12).put(Buf.ByteBuffer.Shared.extract(buf))
          }
        }
      )
    }

    "ParameterStatus" should decodeFragment(MessageDecoder.parameterStatusDecoder) { msg =>
      Packet(
        cmd = Some('S'),
        body = mkBuf() { bb =>
          val name = msg.key match {
            case Parameter.ServerVersion => "server_version"
            case Parameter.ServerEncoding => "server_encoding"
            case Parameter.ClientEncoding => "client_encoding"
            case Parameter.ApplicationName => "application_name"
            case Parameter.IsSuperUser => "is_superuser"
            case Parameter.SessionAuthorization => "session_authorization"
            case Parameter.DateStyle => "DateStyle"
            case Parameter.IntervalStyle => "IntervalStyle"
            case Parameter.TimeZone => "TimeZone"
            case Parameter.IntegerDateTimes => "integer_datetimes"
            case Parameter.StandardConformingStrings => "standard_conforming_strings"
            case Parameter.Other(n) => n
          }
          bb.put(cstring(name)).put(cstring(msg.value))
        }
      )
    }

    "ReaderForQuery" should decodeFragment(MessageDecoder.readyForQueryDecoder) { msg =>
      Packet(
        cmd = Some('Z'),
        body = mkBuf() { bb =>
          val tx = msg.state match {
            case NoTx => 'I'
            case InTx => 'T'
            case FailedTx => 'F'
          }
          bb.put(tx.toByte)
        }
      )
    }

    "RowDescription" should decodeFragment(MessageDecoder.rowDescriptionDecoder) { msg =>
      Packet(
        cmd = Some('T'),
        body = mkBuf() { bb =>
          bb.putShort(msg.rowFields.size.toShort)
          msg.rowFields.foreach { f =>
            bb.put(cstring(f.name))
            f.tableOid match {
              case None => bb.putInt(0)
              case Some(oid) => bb.putInt(unsignedInt(oid.value))
            }
            f.tableAttributeId match {
              case None => bb.putShort(0)
              case Some(AttributeId(value)) => bb.putShort(value.toShort)
            }
            bb.putInt(unsignedInt(f.dataType.value))
            bb.putShort(f.dataTypeSize)
            bb.putInt(f.typeModifier)
            f.format match {
              case Format.Text => bb.putShort(0)
              case Format.Binary => bb.putShort(1)
            }
          }
          bb
        }
      )
    }

    "DataRow" should decodeFragment(MessageDecoder.dataRowDecoder) { msg =>
      Packet(
        cmd = Some('D'),
        body = mkBuf() { bb =>
          bb.putShort(msg.values.size.toShort)
          msg.values.foreach {
            case WireValue.Null => bb.putInt(-1)
            case WireValue.Value(v) => bb.putInt(v.length).put(Buf.ByteBuffer.Shared.extract(v))
          }
          bb
        }
      )
    }

    "ParameterDescription" should decodeFragment(MessageDecoder.parameterDescriptionDecoder) { msg =>
      Packet(
        cmd = Some('t'),
        body = mkBuf() { bb =>
          bb.putShort(msg.parameters.size.toShort)
          msg.parameters.foreach(oid => bb.putInt(unsignedInt(oid.value)))
          bb
        }
      )
    }

    "ParseComplete" should singleton('1', ParseComplete)
    "BindComplete" should singleton('2', BindComplete)
    "CloseComplete" should singleton('3', CloseComplete)
    "EmptyQueryResponse" should singleton('I', EmptyQueryResponse)
    "NoData" should singleton('n', NoData)
    "PortalSuspended" should singleton('s', PortalSuspended)

    def copyInOutBody(overallFormat: Format, columnsFormat: IndexedSeq[Format]) =
      mkBuf() { bb =>
        val b = overallFormat match {
          case Format.Text => 0
          case Format.Binary => 1
        }
        bb.put(b.toByte)
        bb.putShort(columnsFormat.size.toShort)
        columnsFormat.foreach {
          case Format.Text => bb.putShort(0)
          case Format.Binary => bb.putShort(1)
        }
        bb
      }

    "CopyInResponse" should decodeFragment(MessageDecoder.copyInResponseDecoder) { msg =>
      Packet(
        cmd = Some('G'),
        body = copyInOutBody(msg.overallFormat, msg.columnsFormat)
      )
    }
    "CopyOutResponse" should decodeFragment(MessageDecoder.copyOutResponseDecoder) { msg =>
      Packet(
        cmd = Some('H'),
        body = copyInOutBody(msg.overallFormat, msg.columnsFormat)
      )
    }
    "CopyData" should decodeFragment(MessageDecoder.copyDataDecoder) { msg =>
      Packet(
        cmd = Some('d'),
        body = mkBuf() { bb =>
          bb.put(Buf.ByteBuffer.Owned.extract(msg.bytes))
          bb
        }
      )
    }
    "CopyDone" should singleton('c', CopyDone)
  }

}
