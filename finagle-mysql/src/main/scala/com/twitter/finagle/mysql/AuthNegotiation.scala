package com.twitter.finagle.mysql

import com.twitter.finagle.FailureFlags
import com.twitter.finagle.mysql.transport.Packet
import com.twitter.finagle.transport.Transport
import com.twitter.util.Future

import java.lang.IllegalStateException
import java.nio.charset.StandardCharsets

private object AuthNegotiation {

  /**
   * Make the `AuthSwitchResponse` with the user's password or a phony
   * password to cause a cache miss. We only use a phony password during testing.
   */
  private def makeAuthSwitchResponse(
    seqNum: Short,
    salt: Array[Byte],
    authInfo: AuthInfo
  ): AuthSwitchResponse = {
    AuthSwitchResponse(
      seqNum,
      getPasswordForSwitchResponse(authInfo),
      salt,
      authInfo.settings.charset,
      withSha256 = true
    )
  }

  /**
   * Get the password to send in the `AuthSwitchResponse`. This will return either a phony
   * password to cause a cache miss during the `caching_sha2_password` authentication
   * method, the password from the settings, or `None` if the user doesn't have a password.
   * We only use a phony password during testing.
   */
  private def getPasswordForSwitchResponse(authInfo: AuthInfo): Option[String] =
    authInfo.settings.password match {
      // Sending a password when the user doesn't have one set doesn't invalidate the cache,
      // instead, the server will throw an error. Here we make sure we only send the wrong password
      // to invalidate the cache when the user has a non-null password.
      case Some(_) if authInfo.settings.causeAuthCacheMiss =>
        Some("wrong-password") // invalidate cache to perform full auth
      case None => None // if the user doesn't have the password, don't send a password
      case pw => pw
    }

  /**
   * Make the `AuthMoreData` packet that is the response to receiving
   * the server's RSA key. Encrypt the password with the RSA key and
   * send the password to the server.
   */
  private def makeAuthMoreDataWithServersSentRsaKey(
    authMoreData: AuthMoreDataFromServer,
    authInfo: AuthInfo
  ): PasswordAuthMoreDataToServer = authMoreData.authData match {
    case Some(rsaKey) =>
      makeAuthMoreDataWithRsaKeyEncryptedPassword(
        authMoreData,
        authInfo,
        new String(rsaKey, StandardCharsets.UTF_8))
    case None =>
      throw new NegotiationFailure(
        "RSA public key is missing from the AuthMoreData packet sent from the server.")
  }

  /**
   * Make the `AuthMoreData` packet that is sent when TLS is enabled.
   * In this case we send the plaintext password to the server over
   * an encrypted connection.
   */
  private def makeAuthMoreDataWithPlaintextPassword(
    authMoreData: AuthMoreDataFromServer,
    authInfo: AuthInfo
  ): PasswordAuthMoreDataToServer = authInfo.settings.password match {
    case Some(password) =>
      val passwordBytes = PasswordUtils.addNullByteToPassword(
        password.getBytes(MysqlCharset(authInfo.settings.charset).displayName))
      PasswordAuthMoreDataToServer(
        (authMoreData.seqNum + 1).toShort,
        PerformFullAuth,
        passwordBytes)
    case None =>
      throw new IllegalStateException(
        "Null passwords should complete authentication after sending the AuthSwitchResponse")
  }

  /**
   * Make the `AuthMoreData` packet with the password encrypted
   * with the server's RSA public key.
   */
  private def makeAuthMoreDataWithRsaKeyEncryptedPassword(
    authMoreData: AuthMoreDataFromServer,
    authInfo: AuthInfo,
    rsaKey: String
  ): PasswordAuthMoreDataToServer = authInfo.settings.password match {
    case Some(password) =>
      PasswordAuthMoreDataToServer(
        (authMoreData.seqNum + 1).toShort,
        NeedPublicKey,
        PasswordUtils.encryptPasswordWithRsaPublicKey(
          password,
          rsaKey,
          authInfo.salt,
          authInfo.settings.charset,
          authInfo.serverVersion)
      )
    case None =>
      throw new IllegalStateException(
        "Null passwords should complete authentication after sending the AuthSwitchResponse")
  }

  /**
   * Request the server's RSA public key.
   */
  private def makePublicKeyRequestToServer(
    authMoreData: AuthMoreDataFromServer
  ): PlainAuthMoreDataToServer =
    PlainAuthMoreDataToServer((authMoreData.seqNum + 1).toShort, NeedPublicKey)

  /**
   * The exception that is thrown if something goes awry during the authentication process.
   * This exception has the [[FailureFlags.NonRetryable]] flag because this error is
   * thrown only in cases when the server is sent bad authentication information, or the
   * server sends the client bad authentication information.
   */
  private class NegotiationFailure(
    message: String,
    caughtException: Throwable,
    val flags: Long)
      extends Exception(
        s"Failed to authenticate the client with the MySQL server. $message",
        caughtException)
      with FailureFlags[NegotiationFailure] {

    def this(caughtException: Throwable) = this("", caughtException, FailureFlags.NonRetryable)

    def this(message: String) = this(message, null, FailureFlags.NonRetryable)

    protected def copyWithFlags(flags: Long): NegotiationFailure =
      new NegotiationFailure(message, caughtException, flags)
  }

  private sealed trait State
  private object State {
    case class Init(msg: HandshakeResponse, info: AuthInfo) extends State
    case class Switch(msg: AuthSwitchRequest, info: AuthInfo) extends State
    case class MoreData(msg: AuthMoreDataFromServer, info: AuthInfo) extends State
  }
}

/**
 * The class that handles negotiating authentication. Both the `mysql_native_password`
 * and the `caching_sha2_password` auth method terminate here, though the
 * `native_mysql_password` terminates in [[com.twitter.finagle.mysql.AuthNegotiation.State.Init]]
 * whereas `caching_sha2_password` in either the [[com.twitter.finagle.mysql.AuthNegotiation.State.Switch]]
 * or [[com.twitter.finagle.mysql.AuthNegotiation.State.MoreData]] state.
 *
 * Authentication happens during the connection phase, which means [[doAuth()]] should
 * be called during the handshake after receiving the InitialHandshake packet from the Server.
 *
 * @param transport the [[Transport]] used to send messages
 * @param resultDecoder the decoder to use to decode the messages
 */
private class AuthNegotiation(
  transport: Transport[Packet, Packet],
  resultDecoder: Packet => Future[Result]) {
  import AuthNegotiation._

  /**
   * Start the authentication process.
   *
   * @param initMessage the message to send to the server
   * @param initAuthInfo extra information needed at every step
   */
  def doAuth(initMessage: HandshakeResponse, initAuthInfo: AuthInfo): Future[Result] = step(
    State.Init(initMessage, initAuthInfo))

  /**
   * Dispatch a message, then read and return the result.
   */
  private def dispatch(msg: ProtocolMessage): Future[Result] =
    transport
      .write(msg.toPacket)
      .before(transport.read())
      .flatMap(resultDecoder)

  /**
   * The state machine that determines which sends the correct message
   * depending on the state (Init, Switch, or MoreData) it is passed.
   */
  private def step(state: State): Future[Result] = state match {
    // dispatch(Init Message) -> AuthSwitchRequest | <terminate>
    case State.Init(msg, info) =>
      dispatch(msg).flatMap {
        // Change state to Switch.
        case res: AuthSwitchRequest => step(State.Switch(res, info))
        // Or terminate the state machine with OK, Error, or an Exception.
        case ok: OK => Future.value(ok)
        case error: Error => Future.value(error)
        case m =>
          Future.exception(
            new NegotiationFailure(s"Unrecognized or unexpected message from server: $m"))
      }

    // dispatch(AuthSwitchResponse) -> AuthMoreData | <terminate>
    case State.Switch(msg, info) =>
      val req = makeAuthSwitchResponse((msg.seqNum + 1).toShort, msg.pluginData, info)
      dispatch(req).flatMap {
        // Change state to MoreData.
        case res: AuthMoreDataFromServer =>
          val nextInfo = info.copy(salt = Some(msg.pluginData))
          step(State.MoreData(res, nextInfo))
        // Or terminate the state machine with OK, Error, or an Exception.
        case ok: OK => Future.value(ok)
        case error: Error => Future.value(error)
        case m =>
          Future.exception(
            new NegotiationFailure(s"Unrecognized or unexpected message from server: $m"))
      }

    // AuthMoreData -> AuthMoreData | <terminate>
    case State.MoreData(msg, info) =>
      val nextState: Result => Future[Result] = {
        // Stay in the MoreData state.
        case more: AuthMoreDataFromServer => step(State.MoreData(more, info))
        // Or terminate the state machine with OK, Error, or an Exception.
        case ok: OK => Future.value(ok)
        case error: Error => Future.value(error)
        case m =>
          Future.exception(
            new NegotiationFailure(s"Unrecognized or unexpected message from server: $m"))
      }

      // The server sends three AuthMoreDataTypes, and the PerformFullAuth
      // type is handled differently depending on if TLS is enabled or not.
      // If TLS is not enabled, then we perform full auth with the server's
      // RSA public key.
      msg.moreDataType match {
        // The user is already cached in the server so we get a fast auth success.
        case FastAuthSuccess =>
          info.fastAuthSuccessCounter.incr()
          transport.read().flatMap(resultDecoder) // Server sends separate OK packet

        // We previously sent the server the request for the RSA public key
        // This AuthMoreData packet contains the server's public key.
        case NeedPublicKey =>
          dispatch(makeAuthMoreDataWithServersSentRsaKey(msg, info)).flatMap(nextState)

        // When TLS is enabled, we send the password as plaintext.
        case PerformFullAuth if info.tlsEnabled =>
          dispatch(makeAuthMoreDataWithPlaintextPassword(msg, info)).flatMap(nextState)

        // When TLS is not enabled we either request the RSA public key from the
        // server or send the AuthMoreData packet with the password encrypted with
        // the locally stored RSA public key of the server. We determine if we need
        // to send the request for the RSA public key to the server by checking if a
        // path to the locally stored key is provided through the
        // PathToServerRsaPublicKey param.
        case PerformFullAuth if !info.tlsEnabled =>
          if (info.settings.pathToServerRsaPublicKey.nonEmpty) {
            val rsaKey = PasswordUtils.readFromPath(info.settings.pathToServerRsaPublicKey)
            val req = makeAuthMoreDataWithRsaKeyEncryptedPassword(msg, info, rsaKey)
            dispatch(req).flatMap(nextState)
          } else {
            // Public key unknown to client, request the public key from the server.
            dispatch(makePublicKeyRequestToServer(msg)).flatMap(nextState)
          }
      }
  }
}
