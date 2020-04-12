package com.twitter.finagle.postgresql

import com.twitter.io.Buf

sealed trait BackendMessage
object BackendMessage {

  sealed trait AuthenticationMessage extends BackendMessage
  case object AuthenticationOk extends AuthenticationMessage
  case object AuthenticationKerberosV5 extends AuthenticationMessage
  case object AuthenticationCleartextPassword extends AuthenticationMessage
  case object AuthenticationSCMCredential extends AuthenticationMessage
  case class AuthenticationMD5Password(salt: Buf) extends AuthenticationMessage
  case object AuthenticationGSS extends AuthenticationMessage
  case object AuthenticationSSPI extends AuthenticationMessage
  case class AuthenticationGSSContinue(authData: Buf) extends AuthenticationMessage
  case class AuthenticationSASL(mechanism: String) extends AuthenticationMessage
  case class AuthenticationSASLContinue(challenge: Buf) extends AuthenticationMessage
  case class AuthenticationSASLFinal(challenge: Buf) extends AuthenticationMessage

  case class ParameterStatus(name: String, value: String) extends BackendMessage

  case class BackendKeyData(pid: Int, secret: Int) extends BackendMessage

  sealed trait TxState
  case object NoTx extends TxState
  case object InTx extends TxState
  case object FailedTx extends TxState
  case class ReadyForQuery(state: TxState) extends BackendMessage

  sealed trait Field
  object Field {
    case object TODO extends Field // TODO
  }
  case class ErrorResponse(values: Map[Field, String]) extends BackendMessage

}
