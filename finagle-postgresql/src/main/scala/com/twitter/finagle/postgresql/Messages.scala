package com.twitter.finagle.postgresql

import com.twitter.finagle.postgresql.transport.Packet
import com.twitter.finagle.postgresql.transport.PgBuf
import com.twitter.io.Buf

object Messages {

  sealed abstract class FrontendMessage(b: Option[Byte]) {
    def this(value: Char) = this(Some(value.toByte))

    def write: Packet = Packet(b, body(PgBuf.writer).build)

    protected def body(writer: PgBuf.Writer): PgBuf.Writer
  }

  case class Version(major: Short, minor: Short)
  object Version {
    val `3.0` = Version(3,0)
  }

  sealed trait Replication // TODO

  case class StartupMessage(
    version: Version = Version.`3.0`,
    user: String,
    database: Option[String] = None,
    replication: Option[Replication] = None,
    params: Map[String, String] = Map.empty
  ) extends FrontendMessage(None) {
    protected def body(writer: PgBuf.Writer): PgBuf.Writer =
      writer
        .short(version.major)
        .short(version.minor)
        .string("user").string(user)
        .opt(database) { (db, w) =>
          w.string("database").string(db)
        }
        .foreach(params) { case((key, value), w) => w.string(key).string(value) }
        .byte(0)
  }

  case class PasswordMessage(password: String) extends FrontendMessage('p') {
    protected def body(writer: PgBuf.Writer): PgBuf.Writer =
      writer.string(password)
  }

  case object Sync extends FrontendMessage('S') {
    override protected def body(writer: PgBuf.Writer): PgBuf.Writer = writer
  }

  sealed trait BackendMessage

  case object AuthenticationOk extends BackendMessage
  case object AuthenticationKerberosV5 extends BackendMessage
  case object AuthenticationCleartextPassword extends BackendMessage
  case object AuthenticationSCMCredential extends BackendMessage
  case class AuthenticationMD5Password(salt: Buf) extends BackendMessage
  case object AuthenticationGSS extends BackendMessage
  case object AuthenticationSSPI extends BackendMessage
  case class AuthenticationGSSContinue(authData: Buf) extends BackendMessage
  case class AuthenticationSASL(mechanism: String) extends BackendMessage
  case class AuthenticationSASLContinue(challenge: Buf) extends BackendMessage
  case class AuthenticationSASLFinal(challenge: Buf) extends BackendMessage

  object R {
    def read(reader: PgBuf.Reader): BackendMessage = {
      reader.int() match {
        case 0 => AuthenticationOk
        case 2 => AuthenticationKerberosV5
        case 3 => AuthenticationCleartextPassword
        case 5 => AuthenticationMD5Password(reader.buf(4))
        case 6 => AuthenticationSCMCredential
        case 7 => AuthenticationGSS
        case 8 => AuthenticationGSSContinue(reader.remainingBuf())
        case 9 => AuthenticationSSPI
        case 10 => AuthenticationSASL(reader.string())
        case 11 => AuthenticationSASLContinue(reader.remainingBuf())
        case 12 => AuthenticationSASLFinal(reader.remainingBuf())
      }
    }
  }

  case class ParameterStatus(name: String, value: String) extends BackendMessage
  object S {
    def read(reader: PgBuf.Reader): BackendMessage =
      ParameterStatus(reader.string(), reader.string())
  }

  case class BackendKeyData(pid: Int, secret: Int) extends BackendMessage
  object K {
    def read(reader: PgBuf.Reader): BackendMessage =
      BackendKeyData(reader.int(), reader.int())
  }

  sealed trait TxState
  case object NoTx extends TxState
  case object InTx extends TxState
  case object FailedTx extends TxState
  case class ReadyForQuery(state: TxState) extends BackendMessage

  object Z {
    def read(reader: PgBuf.Reader): BackendMessage = {
      val state = reader.byte().toChar match {
        case 'I' => NoTx
        case 'T' => InTx
        case 'F' => FailedTx
      }
      ReadyForQuery(state)
    }
  }

  sealed trait Field
  case class ErrorResponse(values: Map[Field, String]) extends BackendMessage
  object E {
    def read(reader: PgBuf.Reader): BackendMessage = {
      def nextField: Option[Field] =
        reader.byte() match {
          case 0 => None
          case field => Some(new Field{}) // TODO
        }

      def loop(fields: Map[Field, String]): Map[Field, String] =
        nextField match {
          case None => fields
          case Some(field) =>
            val value = reader.string()
            loop(fields + (field -> value))
        }

      ErrorResponse(loop(Map.empty))
    }
  }

  object BackendMessage {
    def read(b: Packet): BackendMessage = {
      val reader = PgBuf.reader(b.body)
      b.cmd.getOrElse(sys.error("invalid backend packet")).toChar match {
        case 'E' => E.read(reader)
        case 'K' => K.read(reader)
        case 'R' => R.read(reader)
        case 'S' => S.read(reader)
        case 'Z' => Z.read(reader)
        case byte => sys.error(s"unimplemented message $byte")
      }
    }
  }

}
