package com.twitter.finagle.postgresql.transport

import com.twitter.finagle.postgresql.Messages
import com.twitter.finagle.postgresql.Messages.AuthenticationCleartextPassword
import com.twitter.finagle.postgresql.Messages.AuthenticationGSS
import com.twitter.finagle.postgresql.Messages.AuthenticationGSSContinue
import com.twitter.finagle.postgresql.Messages.AuthenticationKerberosV5
import com.twitter.finagle.postgresql.Messages.AuthenticationMD5Password
import com.twitter.finagle.postgresql.Messages.AuthenticationMessage
import com.twitter.finagle.postgresql.Messages.AuthenticationOk
import com.twitter.finagle.postgresql.Messages.AuthenticationSASL
import com.twitter.finagle.postgresql.Messages.AuthenticationSASLContinue
import com.twitter.finagle.postgresql.Messages.AuthenticationSASLFinal
import com.twitter.finagle.postgresql.Messages.AuthenticationSCMCredential
import com.twitter.finagle.postgresql.Messages.AuthenticationSSPI
import com.twitter.finagle.postgresql.Messages.BackendKeyData
import com.twitter.finagle.postgresql.Messages.BackendMessage
import com.twitter.finagle.postgresql.Messages.ErrorResponse
import com.twitter.finagle.postgresql.Messages.FailedTx
import com.twitter.finagle.postgresql.Messages.Field
import com.twitter.finagle.postgresql.Messages.InTx
import com.twitter.finagle.postgresql.Messages.NoTx
import com.twitter.finagle.postgresql.Messages.ParameterStatus
import com.twitter.finagle.postgresql.Messages.ReadyForQuery
import com.twitter.util.Throw
import com.twitter.util.Try

trait MessageDecoder[M <: Messages.BackendMessage] {
  def decode(b: PgBuf.Reader): Try[M]
}

object MessageDecoder {

  def decode[M <: Messages.BackendMessage](reader: PgBuf.Reader)(implicit decoder: MessageDecoder[M]) =
    decoder.decode(reader)

  def fromPacket(p: Packet): Try[BackendMessage] = {
    lazy val reader = PgBuf.reader(p.body)
    p.cmd.getOrElse(Throw(new IllegalStateException("invalid backend packet, missing type."))) match {
      case 'E' => decode[ErrorResponse](reader)
      case 'K' => decode[BackendKeyData](reader)
      case 'R' => decode[AuthenticationMessage](reader)
      case 'S' => decode[ParameterStatus](reader)
      case 'Z' => decode[ReadyForQuery](reader)
      case byte => sys.error(s"unimplemented message $byte")
    }
  }

  def apply[M <: Messages.BackendMessage](f: PgBuf.Reader => M): MessageDecoder[M] = reader => Try(f(reader))

  implicit lazy val errorResponseDecoder: MessageDecoder[ErrorResponse] = MessageDecoder { reader =>
    def nextField: Option[Field] =
      reader.byte() match {
        case 0 => None
        case field => Some(Field.TODO) // TODO
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

  implicit lazy val backendKeyDataDecoder: MessageDecoder[BackendKeyData] = MessageDecoder { reader =>
    BackendKeyData(reader.int(), reader.int())
  }

  implicit lazy val authenticationMessageDecoder: MessageDecoder[AuthenticationMessage] = MessageDecoder { reader =>
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

  implicit lazy val parameterStatusDecoder: MessageDecoder[ParameterStatus] = MessageDecoder { reader =>
    ParameterStatus(reader.string(), reader.string())
  }

  implicit lazy val readyForQueryDecoder: MessageDecoder[ReadyForQuery] = MessageDecoder { reader =>
    val state = reader.byte().toChar match {
      case 'I' => NoTx
      case 'T' => InTx
      case 'F' => FailedTx
    }
    ReadyForQuery(state)
  }
}
