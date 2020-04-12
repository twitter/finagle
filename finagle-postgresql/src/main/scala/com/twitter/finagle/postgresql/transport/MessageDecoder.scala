package com.twitter.finagle.postgresql.transport

import com.twitter.finagle.postgresql.BackendMessage
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
import com.twitter.finagle.postgresql.BackendMessage.BackendKeyData
import com.twitter.finagle.postgresql.BackendMessage.ErrorResponse
import com.twitter.finagle.postgresql.BackendMessage.FailedTx
import com.twitter.finagle.postgresql.BackendMessage.Field
import com.twitter.finagle.postgresql.BackendMessage.InTx
import com.twitter.finagle.postgresql.BackendMessage.NoTx
import com.twitter.finagle.postgresql.BackendMessage.ParameterStatus
import com.twitter.finagle.postgresql.BackendMessage.ReadyForQuery
import com.twitter.util.Throw
import com.twitter.util.Try

trait MessageDecoder[M <: BackendMessage] {
  def decode(b: PgBuf.Reader): Try[M]
}

object MessageDecoder {

  def decode[M <: BackendMessage](reader: PgBuf.Reader)(implicit decoder: MessageDecoder[M]) =
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

  def apply[M <: BackendMessage](f: PgBuf.Reader => M): MessageDecoder[M] = reader => Try(f(reader))

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
